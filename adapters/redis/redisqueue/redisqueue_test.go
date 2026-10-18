package redisqueue

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/urmzd/anchor/pkg/queue"
	"github.com/urmzd/anchor/pkg/run"
)

func TestKeyConstruction(t *testing.T) {
	q := New(nil, "prod")

	want := map[string]string{
		"ready":    "du:{prod}:ready",
		"leased":   "du:{prod}:leased",
		"attempt":  "du:{prod}:attempt",
		"failures": "du:{prod}:failures",
		"deltas":   "du:{prod}:deltas:r1",
	}
	got := map[string]string{
		"ready":    q.keyReady,
		"leased":   q.keyLeased,
		"attempt":  q.keyAttempt,
		"failures": q.keyFailures,
		"deltas":   q.deltaKey("r1"),
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s key = %q, want %q", name, got[name], w)
		}
	}
}

// TestNewRejectsBadNamespace pins the constructor's validation: the
// namespace forms the du:{namespace}: cluster hash tag, so an empty value or
// one carrying braces would scatter the key families across cluster slots
// and break every multi-key script with CROSSSLOT. Both violations must
// panic, and the message must name the constraint.
func TestNewRejectsBadNamespace(t *testing.T) {
	mustPanic := func(t *testing.T, ns string) {
		t.Helper()
		defer func() {
			r := recover()
			if r == nil {
				t.Fatalf("New(%q) did not panic", ns)
			}
			msg, ok := r.(string)
			if !ok {
				t.Fatalf("New(%q) panicked with %T, want string", ns, r)
			}
			if !strings.Contains(msg, "non-empty") || !strings.Contains(msg, "'{' or '}'") {
				t.Fatalf("New(%q) panic message %q does not name the constraint", ns, msg)
			}
		}()
		New(nil, ns)
	}

	t.Run("EmptyNamespace", func(t *testing.T) {
		mustPanic(t, "")
	})
	t.Run("BracesInNamespace", func(t *testing.T) {
		for _, ns := range []string{"a{b", "a}b", "{prod}", "{}"} {
			mustPanic(t, ns)
		}
	})
	t.Run("ValidNamespaceAccepted", func(t *testing.T) {
		if q := New(nil, "prod"); q == nil {
			t.Fatal("New returned nil for a valid namespace")
		}
	})
}

func TestOptionsAndDefaults(t *testing.T) {
	if q := New(nil, "ns"); q.pollInterval != defaultPollInterval {
		t.Errorf("default poll interval = %v, want %v", q.pollInterval, defaultPollInterval)
	}
	if q := New(nil, "ns", WithPollInterval(5*time.Millisecond)); q.pollInterval != 5*time.Millisecond {
		t.Errorf("poll interval = %v, want 5ms", q.pollInterval)
	}
	if q := New(nil, "ns", WithPollInterval(-time.Second)); q.pollInterval != defaultPollInterval {
		t.Errorf("non-positive interval accepted: %v", q.pollInterval)
	}
}

func TestPollDelayJitterBounds(t *testing.T) {
	q := New(nil, "ns", WithPollInterval(4*time.Millisecond))
	for range 100 {
		d := q.pollDelay()
		if d < 4*time.Millisecond || d >= 5*time.Millisecond {
			t.Fatalf("pollDelay = %v, want [4ms, 5ms)", d)
		}
	}
	// Sub-4ns intervals have no jitter budget; the interval itself is used.
	tiny := New(nil, "ns", WithPollInterval(2*time.Nanosecond))
	if d := tiny.pollDelay(); d != 2*time.Nanosecond {
		t.Fatalf("pollDelay = %v, want 2ns", d)
	}
}

func TestCeilMillis(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want int64
	}{
		{0, 0},
		{-time.Second, 0},
		{time.Millisecond, 1},
		{time.Microsecond, 1}, // rounds up, never collapses to 0
		{1500 * time.Microsecond, 2},
		{30 * time.Millisecond, 30},
		{time.Minute, 60_000},
	}
	for _, c := range cases {
		if got := ceilMillis(c.d); got != c.want {
			t.Errorf("ceilMillis(%v) = %d, want %d", c.d, got, c.want)
		}
	}
}

func TestRangeStart(t *testing.T) {
	if got := rangeStart(""); got != "-" {
		t.Errorf("rangeStart(\"\") = %q, want \"-\"", got)
	}
	if got := rangeStart(queue.Cursor("1720000000000-3")); got != "(1720000000000-3" {
		t.Errorf("rangeStart = %q, want exclusive form", got)
	}
}

func TestParseClaimReply(t *testing.T) {
	it, err := parseClaimReply([]any{"r1", int64(3), int64(2), int64(1720000000000)})
	if err != nil {
		t.Fatalf("parseClaimReply: %v", err)
	}
	if it.RunID != "r1" || it.Attempt != 3 || it.Failures != 2 {
		t.Fatalf("parseClaimReply = %+v, want {r1 3 2}", it)
	}

	for _, bad := range []any{
		nil,
		"r1",
		[]any{"r1", int64(1)},
		[]any{"r1", int64(1), int64(0)}, // pre-failures 3-element shape
		[]any{int64(1), int64(2), int64(3), int64(4)},
		[]any{"r1", "not-an-int", int64(0), int64(3)},
		[]any{"r1", int64(1), "not-an-int", int64(3)},
	} {
		if _, err := parseClaimReply(bad); err == nil {
			t.Errorf("parseClaimReply(%v) = nil error, want failure", bad)
		}
	}
}

func TestDeltaFromValues(t *testing.T) {
	values := map[string]any{
		fieldKey:     "charge-payment#2",
		fieldAttempt: "4",
		fieldKind:    string(run.DeltaUser),
		fieldPayload: "hello",
	}
	d, err := deltaFromValues("r9", values)
	if err != nil {
		t.Fatalf("deltaFromValues: %v", err)
	}
	want := run.Delta{
		RunID:     "r9",
		RecordKey: "charge-payment#2",
		Attempt:   4,
		Kind:      run.DeltaUser,
		Payload:   []byte("hello"),
	}
	if d.RunID != want.RunID || d.RecordKey != want.RecordKey ||
		d.Attempt != want.Attempt || d.Kind != want.Kind || string(d.Payload) != "hello" {
		t.Fatalf("deltaFromValues = %+v, want %+v", d, want)
	}

	for name, mutate := range map[string]func(map[string]any){
		"missing key":     func(m map[string]any) { delete(m, fieldKey) },
		"missing attempt": func(m map[string]any) { delete(m, fieldAttempt) },
		"bad attempt":     func(m map[string]any) { m[fieldAttempt] = "NaN" },
		"bad type":        func(m map[string]any) { m[fieldPayload] = 42 },
	} {
		broken := map[string]any{}
		for k, v := range values {
			broken[k] = v
		}
		mutate(broken)
		if _, err := deltaFromValues("r9", broken); err == nil {
			t.Errorf("%s: deltaFromValues = nil error, want failure", name)
		}
	}
}

func TestSupersededErrorsWrap(t *testing.T) {
	// The mapping from supersededReply to run.ErrSuperseded must survive
	// wrapping so callers can errors.Is on it.
	err := errorFromSuperseded()
	if !errors.Is(err, run.ErrSuperseded) {
		t.Fatalf("wrapped error does not match run.ErrSuperseded: %v", err)
	}
}

// errorFromSuperseded mirrors the wrap pattern every fenced method uses.
func errorFromSuperseded() error {
	return wrapSuperseded("heartbeat", "r1", 2)
}
