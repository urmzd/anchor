package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	anchor "github.com/urmzd/anchor"
)

// Order is the workflow input.
type Order struct {
	ID     string `json:"id"`
	Email  string `json:"email"`
	Amount int    `json:"amount_cents"`
}

// Receipt is the workflow output.
type Receipt struct {
	OrderID  string    `json:"order_id"`
	ChargeID string    `json:"charge_id"`
	Amount   int       `json:"amount_cents"`
	IssuedAt time.Time `json:"issued_at"`
}

// Invocation counters. The demo prints them at the end: after a worker crash
// the workflow re-executes from the top, but recorded activities replay from
// the ledger instead of running again — the counters are the proof.
var (
	chargeCalls  atomic.Int64
	reserveCalls atomic.Int64
	confirmCalls atomic.Int64
)

// Crash-demo hook: when armed, the next sendConfirmation call hangs until
// its worker dies — simulating a process crash mid-send.
var (
	confirmShouldHang atomic.Bool
	confirmEntered    = make(chan struct{}, 1)
)

// The activities are plain functions wrapped once at package level. Outside
// a run they are ordinary Go calls; under a worker they are memoized.
var (
	chargePayment    = anchor.Activity("charge-payment", charge)
	reserveInventory = anchor.Activity("reserve-inventory", reserve)
	sendConfirmation = anchor.Activity("send-confirmation", confirm)
	processOrder     = anchor.Activity("process-order", process)
)

func charge(ctx context.Context, o Order) (string, error) {
	chargeCalls.Add(1)
	// Hand the stable per-call key to the payment provider: even if this
	// activity re-executes in the at-least-once window, a deduplicating
	// provider charges once. anchor's at-least-once becomes effective-once.
	fmt.Printf("  charge   $%d.%02d  idempotency key %s\n",
		o.Amount/100, o.Amount%100, anchor.IdempotencyKey(ctx))
	return "ch_" + o.ID, nil
}

func reserve(ctx context.Context, o Order) (anchor.Void, error) {
	reserveCalls.Add(1)
	fmt.Printf("  reserve  1 unit for order %s\n", o.ID)
	return anchor.Void{}, nil
}

func confirm(ctx context.Context, o Order) (anchor.Void, error) {
	confirmCalls.Add(1)
	if confirmShouldHang.CompareAndSwap(true, false) {
		select {
		case confirmEntered <- struct{}{}:
		default:
		}
		<-ctx.Done() // the worker dies here; no record is written
		return anchor.Void{}, ctx.Err()
	}
	fmt.Printf("  confirm  email to %s\n", o.Email)
	return anchor.Void{}, nil
}

// process is the orchestrator — itself just an activity that calls others.
// Crash anywhere and it re-executes from the top: recorded calls return
// instantly, the first unrecorded call runs for real.
func process(ctx context.Context, o Order) (Receipt, error) {
	chargeID, err := chargePayment.Call(ctx, o)
	if err != nil {
		return Receipt{}, err
	}
	_ = anchor.Emit(ctx, progress{Stage: "charged"})

	if _, err := reserveInventory.Call(ctx, o); err != nil {
		return Receipt{}, err
	}
	_ = anchor.Emit(ctx, progress{Stage: "reserved"})

	if _, err := sendConfirmation.Call(ctx, o); err != nil {
		return Receipt{}, err
	}
	_ = anchor.Emit(ctx, progress{Stage: "confirmed"})

	// Step records inline non-determinism: the first value is the value
	// forever, so replays never shift the receipt time.
	issuedAt, err := anchor.Step(ctx, "issued-at",
		func(context.Context) (time.Time, error) { return time.Now(), nil })
	if err != nil {
		return Receipt{}, err
	}
	return Receipt{OrderID: o.ID, ChargeID: chargeID, Amount: o.Amount, IssuedAt: issuedAt}, nil
}

// progress is the delta payload streamed to watchers — advisory flow, never
// truth.
type progress struct {
	Stage string `json:"stage"`
}
