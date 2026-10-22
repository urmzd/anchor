package main

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/urmzd/anchor"
	"github.com/urmzd/anchor/pkg/ledger"
	"github.com/urmzd/anchor/pkg/queue"
	"github.com/urmzd/anchor/pkg/run"
	"github.com/urmzd/anchor/pkg/worker"
)

const jsonCT = "application/json"

var t0 = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// history builds records recorded one second apart, in argument order.
// Each spec is "kind key name"; the kind letter is a (activity), s (step)
// or e (event).
func history(specs ...string) []run.Record {
	var recs []run.Record
	for i, spec := range specs {
		f := strings.Fields(spec)
		rec := run.Record{
			RunID: "r", Key: f[1], Name: f[2], Status: run.RecordOK, Codec: jsonCT,
			Output: []byte(`"v"`), Attempt: 1, RecordedAt: t0.Add(time.Duration(i) * time.Second),
		}
		switch f[0] {
		case "a":
			rec.Kind = run.KindActivity
			rec.InputHash = run.HashInput([]byte(`{"in":1}`))
		case "s":
			rec.Kind = run.KindStep
		case "e":
			rec.Kind, rec.Codec, rec.Attempt = run.KindEvent, "", 0
		}
		recs = append(recs, rec)
	}
	return recs
}

func pendingRun() run.Run {
	return run.Run{ID: "r", Name: "wf", Status: run.StatusPending, CreatedAt: t0}
}

func keys(cs []Replayed) []string {
	var out []string
	for _, c := range cs {
		out = append(out, c.Key)
	}
	return out
}

func eq(a, b []string) bool { return slices.Equal(a, b) }

func TestReplayNoRecordsStartsFromTheTop(t *testing.T) {
	rep := replayRun(context.Background(), pendingRun(), nil, jsonCT)
	if !rep.OK() || rep.Complete() {
		t.Fatalf("report = %+v, want ok and not complete", rep)
	}
	if f := rep.Frontier; f == nil || f.At != "" || f.After != "" || len(f.Reexecutes) != 0 {
		t.Fatalf("frontier = %+v, want the first call", rep.Frontier)
	}
}

func TestReplayResumesAfterTheLastRecordedCall(t *testing.T) {
	recs := history("a charge-payment#0 charge-payment", "a reserve-inventory#0 reserve-inventory", "s issued-at#0 issued-at")
	rep := replayRun(context.Background(), pendingRun(), recs, jsonCT)
	if !rep.OK() {
		t.Fatalf("report not ok: %+v", rep)
	}
	if got := keys(rep.Replayed); !eq(got, []string{"charge-payment#0", "reserve-inventory#0", "issued-at#0"}) {
		t.Fatalf("replayed = %v", got)
	}
	if f := rep.Frontier; f == nil || f.After != "issued-at#0" || f.At != "" {
		t.Fatalf("frontier = %+v, want after issued-at#0", rep.Frontier)
	}
}

func TestReplayCompleteRunCoversNestedRecords(t *testing.T) {
	// Nested calls record before their parent.
	recs := history(
		"a fulfil#0/pick#0 pick",
		"a fulfil#0/pack#0 pack",
		"a fulfil#0 fulfil",
		"s issued-at#0 issued-at",
	)
	r := pendingRun()
	r.Status, r.Attempt = run.StatusSucceeded, 2
	rep := replayRun(context.Background(), r, recs, jsonCT)
	if !rep.OK() || !rep.Complete() || rep.Frontier != nil {
		t.Fatalf("report = %+v, want ok, complete, no frontier", rep)
	}
	if got := keys(rep.Replayed); !eq(got, []string{"fulfil#0", "issued-at#0"}) {
		t.Fatalf("replayed = %v, want the top-level calls only", got)
	}
	if !eq(rep.Covered, []string{"fulfil#0/pick#0", "fulfil#0/pack#0"}) {
		t.Fatalf("covered = %v, want the nested records", rep.Covered)
	}
	if len(rep.Unreached) != 0 {
		t.Fatalf("unreached = %v, want none", rep.Unreached)
	}
}

func TestReplaySkippedOccurrenceIsTheFrontier(t *testing.T) {
	// The first charge failed retryably and the workflow went on: no
	// record for charge-payment#0, so a worker executes it again.
	recs := history("a charge-payment#1 charge-payment", "a reserve-inventory#0 reserve-inventory")
	rep := replayRun(context.Background(), pendingRun(), recs, jsonCT)
	if !rep.OK() {
		t.Fatalf("report not ok: %+v", rep)
	}
	if f := rep.Frontier; f == nil || f.At != "charge-payment#0" || f.After != "" {
		t.Fatalf("frontier = %+v, want at charge-payment#0", rep.Frontier)
	}
	// The frontier does not end the attempt: the later records replay.
	if got := keys(rep.Replayed); !eq(got, []string{"charge-payment#1", "reserve-inventory#0"}) {
		t.Fatalf("replayed = %v", got)
	}
}

func TestReplayReexecutesAParentWithoutARecord(t *testing.T) {
	// The worker died inside fulfil: its nested calls are recorded, fulfil
	// itself is not.
	recs := history("a charge-payment#0 charge-payment", "a fulfil#0/pick#0 pick", "a fulfil#0/pack#0 pack")
	rep := replayRun(context.Background(), pendingRun(), recs, jsonCT)
	if !rep.OK() {
		t.Fatalf("report not ok: %+v", rep)
	}
	f := rep.Frontier
	if f == nil || !eq(f.Reexecutes, []string{"fulfil#0"}) || f.At != "" || f.After != "fulfil#0/pack#0" {
		t.Fatalf("frontier = %+v, want fulfil#0 re-executing after fulfil#0/pack#0", f)
	}
	if got := keys(rep.Replayed); !eq(got, []string{"charge-payment#0", "fulfil#0/pick#0", "fulfil#0/pack#0"}) {
		t.Fatalf("replayed = %v", got)
	}
}

func TestReplayNestedParentsWithoutRecords(t *testing.T) {
	recs := history("a a#0/b#0/c#1 c")
	rep := replayRun(context.Background(), pendingRun(), recs, jsonCT)
	f := rep.Frontier
	if !rep.OK() || f == nil || !eq(f.Reexecutes, []string{"a#0", "a#0/b#0"}) || f.At != "a#0/b#0/c#0" {
		t.Fatalf("report = %+v frontier %+v, want a#0 and a#0/b#0 re-executing, at a#0/b#0/c#0", rep, f)
	}
	if got := keys(rep.Replayed); !eq(got, []string{"a#0/b#0/c#1"}) {
		t.Fatalf("replayed = %v", got)
	}
}

func TestReplayParksAtAMissingEvent(t *testing.T) {
	recs := history("a charge-payment#0 charge-payment", "e event:approval#1 approval", "a ship#0 ship")
	rep := replayRun(context.Background(), pendingRun(), recs, jsonCT)
	if !rep.OK() {
		t.Fatalf("report not ok: %+v", rep)
	}
	f := rep.Frontier
	if f == nil || !f.Parks || f.At != "event:approval#0" || f.After != "charge-payment#0" {
		t.Fatalf("frontier = %+v, want a park at event:approval#0", f)
	}
	// A park ends the attempt: nothing after it is reached.
	if !eq(rep.Unreached, []string{"event:approval#1", "ship#0"}) {
		t.Fatalf("unreached = %v", rep.Unreached)
	}
}

func TestReplayRecordedFailureReplaysAsAnError(t *testing.T) {
	recs := history("a charge-payment#0 charge-payment")
	recs[0].Status, recs[0].Output, recs[0].Error = run.RecordFailed, nil, "card declined"
	rep := replayRun(context.Background(), pendingRun(), recs, jsonCT)
	if !rep.OK() || len(rep.Replayed) != 1 || !rep.Replayed[0].Failed {
		t.Fatalf("report = %+v, want ok with one replayed failure", rep)
	}
}

func TestReplayCodecMismatchDiverges(t *testing.T) {
	recs := history("a charge-payment#0 charge-payment", "a reserve-inventory#0 reserve-inventory")
	recs[1].Codec = "application/x-protobuf"
	rep := replayRun(context.Background(), pendingRun(), recs, jsonCT)
	if rep.OK() || rep.Divergence == nil || rep.Divergence.Key != "reserve-inventory#0" ||
		!strings.Contains(rep.Divergence.Message, "codec") {
		t.Fatalf("report = %+v, want a codec divergence at reserve-inventory#0", rep)
	}
	if rep.Frontier != nil {
		t.Fatalf("frontier = %+v, want none: the run fails before any call executes", rep.Frontier)
	}
	// The workers' own codec replays cleanly.
	if rep := replayRun(context.Background(), pendingRun(), recs[1:], "application/x-protobuf"); !rep.OK() {
		t.Fatalf("replay with the recording codec: %+v", rep.Divergence)
	}
}

func TestReplayActivityWithoutInputHashDiverges(t *testing.T) {
	recs := history("a charge-payment#0 charge-payment")
	recs[0].InputHash = ""
	rep := replayRun(context.Background(), pendingRun(), recs, jsonCT)
	if rep.Divergence == nil || rep.Divergence.Key != "charge-payment#0" || !strings.Contains(rep.Divergence.Message, "input hash") {
		t.Fatalf("divergence = %+v, want a missing input hash at charge-payment#0", rep.Divergence)
	}
}

func TestReplayMalformedRecordsAreProblems(t *testing.T) {
	recs := history(
		"a charge-payment#0 refund",  // key and name disagree
		"a weird/pick#0 pick",        // nested under a key no call produces
		"a ok#0 ok",                  // fine
		"e event:approval#0/x#0 x",   // nested under an event
		"a charge-payment#01 charge", // non-canonical occurrence
		"a charge-payment#0/y#0 y",   // nested under a malformed record
	)
	recs = append(recs, run.Record{RunID: "r", Key: "z#0", Kind: "bogus", Name: "z", Status: run.RecordOK, RecordedAt: t0})
	rep := replayRun(context.Background(), pendingRun(), recs, jsonCT)
	var bad []string
	for _, p := range rep.Problems {
		bad = append(bad, p.Key)
	}
	slices.Sort(bad)
	want := []string{"charge-payment#0", "charge-payment#0/y#0", "charge-payment#01", "event:approval#0/x#0", "weird/pick#0", "z#0"}
	if !eq(bad, want) {
		t.Fatalf("problems = %+v, want keys %v", rep.Problems, want)
	}
	for _, p := range rep.Problems {
		if p.Key == "charge-payment#0/y#0" && !strings.Contains(p.Message, "a record no call could have produced") {
			t.Fatalf("problem for the child of a malformed record = %q", p.Message)
		}
	}
	if rep.OK() {
		t.Fatal("report ok with malformed records, want not ok")
	}
	if got := keys(rep.Replayed); !eq(got, []string{"ok#0"}) {
		t.Fatalf("replayed = %v, want ok#0", got)
	}
}

func TestReplayMalformedRecordInAnOccurrenceSlotDiverges(t *testing.T) {
	// charge-payment#0 holds a record of another name; the second charge
	// call derives charge-payment#0 first and the frame refuses it, as it
	// would for a worker.
	recs := history("a charge-payment#0 refund", "a charge-payment#1 charge-payment")
	rep := replayRun(context.Background(), pendingRun(), recs, jsonCT)
	if rep.Divergence == nil || rep.Divergence.Key != "charge-payment#0" {
		t.Fatalf("divergence = %+v, want one at charge-payment#0", rep.Divergence)
	}
}

func TestReplayKeyedCalls(t *testing.T) {
	recs := history("a k:order-1 charge", "a k:order-1/k:line-2 pick", "a k:order-1/pack#0 pack", "a k:solo charge")
	rep := replayRun(context.Background(), pendingRun(), recs, jsonCT)
	if !rep.OK() {
		t.Fatalf("report not ok: %+v %+v", rep.Problems, rep.Divergence)
	}
	if got := keys(rep.Replayed); !eq(got, []string{"k:order-1", "k:solo"}) {
		t.Fatalf("replayed = %v", got)
	}
	if !eq(rep.Covered, []string{"k:order-1/k:line-2", "k:order-1/pack#0"}) {
		t.Fatalf("covered = %v", rep.Covered)
	}
}

func TestReplayOccurrenceOrderSurvivesClockSkew(t *testing.T) {
	// A step and an activity share the name's counter; a skewed clock
	// recorded the later occurrence first.
	recs := history("s x#1 x", "a x#0 x")
	rep := replayRun(context.Background(), pendingRun(), recs, jsonCT)
	if !rep.OK() {
		t.Fatalf("report not ok: %+v", rep.Divergence)
	}
	if got := keys(rep.Replayed); !eq(got, []string{"x#0", "x#1"}) {
		t.Fatalf("replayed = %v, want occurrence order", got)
	}
}

func TestReplayTerminalRunWithGapIsComplete(t *testing.T) {
	recs := history("a charge-payment#1 charge-payment")
	r := pendingRun()
	r.Status, r.Attempt = run.StatusFailed, 3
	rep := replayRun(context.Background(), r, recs, jsonCT)
	if !rep.OK() || !rep.Complete() || rep.Frontier != nil {
		t.Fatalf("report = %+v, want ok and complete with no frontier", rep)
	}
}

// sdkHistory runs wf to its outcome on the in-memory ledger and queue with
// a real worker, and returns the run and its records.
func sdkHistory[I, O any](t *testing.T, reg *run.Registry, wf *anchor.ActivityFn[I, O], in I, runID string, until func(run.Run, []run.Record) bool) (run.Run, []run.Record) {
	t.Helper()
	lgr := ledger.NewMemory()
	q := queue.NewMemory()
	ctx, cancel := context.WithCancel(context.Background())
	w := worker.New(lgr, q, worker.WithRegistry(reg), worker.WithJanitorEvery(0),
		worker.WithBackoff(func(int) time.Duration { return time.Millisecond }))
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	if _, err := anchor.Start(ctx, anchor.New(lgr, q), wf, in, anchor.WithRunID(runID)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		r, recs, err := lgr.Load(context.Background(), runID)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if until(r, recs) {
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("worker: %v", err)
			}
			// Read again: the worker settles on its way out.
			r, recs, err = lgr.Load(context.Background(), runID)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			return r, recs
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("run %s did not reach the wanted state: %+v %+v", runID, r, recs)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type order struct{ ID string }

func TestReplaySDKHistories(t *testing.T) {
	reg := run.NewRegistry()
	charge := anchor.ActivityIn(reg, "charge-payment", func(_ context.Context, o order) (string, error) { return "ch_" + o.ID, nil })
	pick := anchor.ActivityIn(reg, "pick", func(context.Context, order) (anchor.Void, error) { return anchor.Void{}, nil })
	fulfil := anchor.ActivityIn(reg, "fulfil", func(ctx context.Context, o order) (int, error) {
		for range 2 {
			if _, err := pick.Call(ctx, o); err != nil {
				return 0, err
			}
		}
		return 2, nil
	})
	done := anchor.ActivityIn(reg, "done", func(ctx context.Context, o order) (string, error) {
		if _, err := charge.Call(ctx, o); err != nil {
			return "", err
		}
		if _, err := fulfil.CallKeyed(ctx, "fulfil-"+o.ID, o); err != nil {
			return "", err
		}
		at, err := anchor.Step(ctx, "issued-at", func(context.Context) (time.Time, error) { return time.Now(), nil })
		return at.String(), err
	})
	parked := anchor.ActivityIn(reg, "parked", func(ctx context.Context, o order) (string, error) {
		if _, err := charge.Call(ctx, o); err != nil {
			return "", err
		}
		return anchor.Event[string](ctx, "approval")
	})
	declined := anchor.ActivityIn(reg, "declined", func(ctx context.Context, o order) (string, error) {
		return anchor.Step(ctx, "decline", func(context.Context) (string, error) {
			return "", anchor.NonRetryable(errors.New("card declined"))
		})
	})

	t.Run("succeeded", func(t *testing.T) {
		r, recs := sdkHistory(t, reg, done, order{ID: "1"}, "run-done", func(r run.Run, _ []run.Record) bool { return r.Status.Terminal() })
		rep := replayRun(context.Background(), r, recs, jsonCT)
		if !rep.OK() || !rep.Complete() || r.Status != run.StatusSucceeded {
			t.Fatalf("report = %+v (run %s), want ok and complete", rep, r.Status)
		}
		if got := keys(rep.Replayed); !eq(got, []string{"charge-payment#0", "k:fulfil-1", "issued-at#0"}) {
			t.Fatalf("replayed = %v", got)
		}
		if !eq(rep.Covered, []string{"k:fulfil-1/pick#0", "k:fulfil-1/pick#1"}) {
			t.Fatalf("covered = %v", rep.Covered)
		}
	})

	t.Run("parked", func(t *testing.T) {
		r, recs := sdkHistory(t, reg, parked, order{ID: "2"}, "run-parked", func(_ run.Run, recs []run.Record) bool { return len(recs) == 1 })
		rep := replayRun(context.Background(), r, recs, jsonCT)
		if !rep.OK() || rep.Complete() || rep.Frontier == nil || rep.Frontier.After != "charge-payment#0" {
			t.Fatalf("report = %+v frontier %+v, want a pending run resuming after charge-payment#0", rep, rep.Frontier)
		}
	})

	t.Run("failed", func(t *testing.T) {
		r, recs := sdkHistory(t, reg, declined, order{ID: "3"}, "run-declined", func(r run.Run, _ []run.Record) bool { return r.Status.Terminal() })
		rep := replayRun(context.Background(), r, recs, jsonCT)
		if !rep.OK() || !rep.Complete() || r.Status != run.StatusFailed || len(rep.Replayed) != 1 || !rep.Replayed[0].Failed {
			t.Fatalf("report = %+v (run %s), want a complete failed run replaying its recorded failure", rep, r.Status)
		}
	})
}
