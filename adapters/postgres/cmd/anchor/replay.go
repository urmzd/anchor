package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/urmzd/anchor/pkg/replay"
	"github.com/urmzd/anchor/pkg/run"
)

// A replay without the workflow code. A worker replays a run by running the
// workflow again under a replay.Frame: every wrapped call asks the frame,
// which answers recorded calls from the ledger and executes the first
// unrecorded one for real. This tool has no workflow code, so it plays the
// recorded calls themselves into the same kind of frame, in the order they
// were recorded, and reads off the frame's answers:
//
//   - a recorded call replays: the frame derives the call's key from its
//     kind, name and occurrence and finds the record, as it would for the
//     real code;
//   - a call whose key the frame derives differently (an occurrence with no
//     record) or whose own record is missing (a parent whose nested calls
//     are recorded) is the frontier: the frame would execute it, and this
//     replay hands back errFrontier instead, so nothing executes;
//   - anything else the frame refuses is a divergence a worker would fail
//     the run on.
//
// The frame gets a ledger that refuses every write and no delta log, so the
// replay cannot change anything even by mistake. Without the code it cannot
// know live inputs, so it checks that each activity record carries an input
// hash and then feeds the frame a placeholder input with the hash re-keyed
// to match; kinds, names, occurrences, codecs and recorded failures are
// checked as a worker checks them.

// errFrontier stands in for executing a call: the frame reached a call with
// no record, which a worker would execute for real.
var errFrontier = errors.New("replay reached a call with no record")

// placeholderInput is fed to the frame as every recorded activity's input.
var placeholderInput = []byte("anchor replay: the live input is unknown without the workflow code")

// Report is what a replay found.
type Report struct {
	RunID   string
	Name    string
	Status  run.RunStatus
	Attempt int
	Records int // records the ledger holds for the run

	// Replayed are the calls the frame answered from the ledger, in
	// replay order.
	Replayed []Replayed
	// Covered are nested records no replay reaches: their recorded parent
	// answers for its whole subtree.
	Covered []string
	// Frontier is where a worker would resume a pending run. Nil for a
	// terminal run, or when the replay diverged.
	Frontier *Frontier
	// Unreached are recorded keys the replay did not reach: beyond the
	// frontier or the divergence. A worker may still reach them later
	// (a signaled event, a keyed call) or never (a call the code no
	// longer makes).
	Unreached []string
	// Divergence is where the replay stopped because the frame refused
	// the history: a worker would fail the run there.
	Divergence *Finding
	// Problems are records no call could have produced: the key does not
	// derive from the record's kind and name.
	Problems []Finding
}

// Replayed is one call the frame answered from the ledger.
type Replayed struct {
	Key  string
	Kind run.Kind
	Name string
	// Failed is a recorded non-retryable failure: the workflow receives
	// it as *run.RecordedError, on every attempt.
	Failed bool
}

// Frontier is where a worker resumes: the first call it would execute for
// real.
type Frontier struct {
	// Reexecutes are calls, outermost first, whose own records are
	// missing while calls nested in them are recorded: a worker runs
	// their bodies again, and their recorded calls replay.
	Reexecutes []string
	// At is the call that executes for real, when the ledger names it: a
	// skipped occurrence. Empty when the next call is one the ledger has
	// never seen.
	At string
	// Parks says At is an event with no record: the run parks there until
	// something writes it.
	Parks bool
	// After is the last call replayed before the frontier, in the
	// innermost scope; empty when the frontier is that scope's first call.
	After string
}

// Finding is one problem with one record.
type Finding struct {
	Key     string
	Message string
}

// OK reports whether the history replays cleanly.
func (r *Report) OK() bool { return r.Divergence == nil && len(r.Problems) == 0 }

// Complete reports whether the run is terminal: a worker settles it without
// replaying anything.
func (r *Report) Complete() bool { return r.Status.Terminal() }

// node is one call in the reconstructed call tree: a record, or a parent
// whose record is missing but whose nested calls are recorded.
type node struct {
	key     string
	rec     *run.Record // nil for a parent with no record
	kind    run.Kind
	name    string
	userKey string // set for a CallKeyed call
	occ     int    // occurrence; -1 for a keyed call
	parent  string // parent key; "" at the top level
	first   time.Time
	kids    []*node
}

// base is the occurrence counter a call shares: activities and steps count
// per name, events per "event:" name, and keyed calls not at all.
func (n *node) base() string {
	switch {
	case n.userKey != "":
		return ""
	case n.kind == run.KindEvent:
		return "event:" + n.name
	default:
		return n.name
	}
}

// replayRun replays r's records without side effects. contentType is the
// codec content type the workers record with.
func replayRun(ctx context.Context, r run.Run, records []run.Record, contentType string) *Report {
	rep := &Report{RunID: r.ID, Name: r.Name, Status: r.Status, Attempt: r.Attempt, Records: len(records)}

	nodes, roots, problems := buildTree(records)
	rep.Problems = problems

	// The frame's memo is the run's records, with each well-formed activity
	// record's input hash re-keyed to the placeholder input.
	memo := make([]run.Record, 0, len(records))
	for _, rec := range records {
		if rec.Kind == run.KindActivity && validHash(rec.InputHash) {
			rec.InputHash = run.HashInput(placeholderInput)
		}
		memo = append(memo, rec)
	}
	d := &driver{
		frame: replay.NewFrame(r.ID, r.Attempt, refusingLedger{}, codecOf(contentType), memo, nil),
		rep:   rep,
		count: make(map[string]int),
	}
	if !d.drive(ctx, "", roots) {
		// Every recorded call replayed: the workflow's next call is one
		// the ledger has never seen.
		d.stop(&Frontier{After: d.lastTop})
	}
	if rep.Complete() {
		rep.Frontier = nil // a worker settles a terminal run without replaying it
	}

	// What the frame never consumed is either covered by a replayed
	// parent, a malformed record, or beyond where the replay stopped.
	replayed := make(map[string]bool, len(rep.Replayed))
	for _, c := range rep.Replayed {
		replayed[c.Key] = true
	}
	bad := make(map[string]bool, len(problems))
	for _, p := range problems {
		bad[p.Key] = true
	}
	unconsumed := make(map[string]bool)
	for _, k := range d.frame.Unconsumed() {
		unconsumed[k] = true
	}
	for _, rec := range sortedRecords(records) {
		if !unconsumed[rec.Key] || bad[rec.Key] {
			continue
		}
		if coveredBy(nodes, rec.Key, replayed) {
			rep.Covered = append(rep.Covered, rec.Key)
		} else {
			rep.Unreached = append(rep.Unreached, rec.Key)
		}
	}
	return rep
}

// coveredBy reports whether some ancestor of key replayed: its record
// answers for key's whole subtree.
func coveredBy(nodes map[string]*node, key string, replayed map[string]bool) bool {
	for n := nodes[key]; n != nil && n.parent != ""; n = nodes[n.parent] {
		if replayed[n.parent] {
			return true
		}
	}
	return false
}

// driver plays the call tree into the frame.
type driver struct {
	frame   *replay.Frame
	rep     *Report
	count   map[string]int // occurrences fed per scope and counter, mirroring the frame's
	stack   []string       // parents being re-executed, outermost first
	lastTop string
}

// next returns the key the frame derives for n's next occurrence in scope,
// and counts it.
func (d *driver) next(scope string, n *node) string {
	prefix := ""
	if scope != "" {
		prefix = scope + "/"
	}
	b := n.base()
	if b == "" {
		return prefix + "k:" + n.userKey
	}
	k := prefix + b + "#" + strconv.Itoa(d.count[prefix+b])
	d.count[prefix+b]++
	return k
}

// pending returns the occurrence the frame would derive next for n's
// counter in scope, without counting it.
func (d *driver) pending(scope string, n *node) int {
	prefix := ""
	if scope != "" {
		prefix = scope + "/"
	}
	return d.count[prefix+n.base()]
}

// drive plays scope's calls into the frame in order and reports whether the
// replay halted. A park or a divergence ends the attempt; a frontier does
// not: the code executes that call and goes on, and the frame answers the
// later recorded calls from the ledger again.
func (d *driver) drive(ctx context.Context, scope string, kids []*node) (halted bool) {
	last := ""
	for _, n := range kids {
		// Occurrences skipped before this one are calls with no record:
		// play them first, so the frame derives this call's key.
		for n.base() != "" && d.pending(scope, n) < n.occ {
			if d.call(ctx, scope, n, nil, last) {
				return true
			}
		}
		if n.rec == nil {
			if d.reexecute(ctx, scope, n, last) {
				return true
			}
			continue
		}
		if d.call(ctx, scope, n, n.rec, last) {
			return true
		}
		if d.rep.Frontier == nil {
			last = n.key
			if scope == "" {
				d.lastTop = last
			}
		}
	}
	if scope != "" {
		// The recorded calls nested in a re-executed parent all replayed:
		// the parent's next call is one the ledger has never seen.
		d.stop(&Frontier{After: last})
	}
	return false
}

// call plays one call of n's kind and name into the frame: the record rec
// when it has one, or a skipped occurrence when rec is nil. It reports
// whether the replay halted.
func (d *driver) call(ctx context.Context, scope string, n *node, rec *run.Record, last string) bool {
	expected := d.next(scope, n)
	input := []byte(nil)
	if n.kind == run.KindActivity {
		if rec != nil && !validHash(rec.InputHash) {
			d.rep.Divergence = &Finding{Key: rec.Key, Message: fmt.Sprintf(
				"activity record has no valid input hash (%q); a worker's replay can never match it", rec.InputHash)}
			return true
		}
		input = placeholderInput
	}
	var fn func(context.Context) ([]byte, error)
	if n.kind != run.KindEvent {
		fn = func(context.Context) ([]byte, error) {
			d.stop(&Frontier{At: d.frame.ActiveKey(), After: last})
			return nil, errFrontier
		}
	}
	_, err := d.frame.Do(ctx, n.kind, n.name, n.userKey, input, fn)
	var recorded *run.RecordedError
	switch {
	case errors.Is(err, errFrontier):
		return false
	case errors.Is(err, run.ErrParked):
		// The attempt ends here: the workflow returns run.ErrParked.
		d.stop(&Frontier{At: expected, Parks: true, After: last})
		return true
	case rec == nil:
		// A skipped occurrence the frame answered, or refused, from a
		// record: another record holds the key this call derives.
		msg := fmt.Sprintf("the call %s has no record of its own, yet the frame answered it from the ledger", expected)
		if err != nil {
			msg = err.Error()
		}
		d.rep.Divergence = &Finding{Key: expected, Message: msg}
		return true
	case err == nil, errors.As(err, &recorded):
		d.rep.Replayed = append(d.rep.Replayed, Replayed{Key: rec.Key, Kind: rec.Kind, Name: rec.Name, Failed: recorded != nil})
		return false
	default:
		d.rep.Divergence = &Finding{Key: rec.Key, Message: err.Error()}
		return true
	}
}

// reexecute plays a parent whose record is missing. The frame misses on it,
// as it would for the real code, and runs its body; the body plays the
// parent's recorded calls, which replay, and the frontier lies inside. It
// reports whether the replay halted.
func (d *driver) reexecute(ctx context.Context, scope string, n *node, last string) bool {
	d.next(scope, n)
	halted := false
	_, err := d.frame.Do(ctx, run.KindActivity, n.name, n.userKey, placeholderInput, func(ctx context.Context) ([]byte, error) {
		key := d.frame.ActiveKey()
		if key != n.key {
			// The frame derives another key for this call, so its nested
			// records cannot replay under it: the call itself is the
			// frontier.
			d.stop(&Frontier{At: key, After: last})
			return nil, errFrontier
		}
		d.stack = append(d.stack, key)
		halted = d.drive(ctx, key, n.kids)
		d.stack = d.stack[:len(d.stack)-1]
		return nil, errFrontier
	})
	if errors.Is(err, errFrontier) {
		return halted
	}
	// The frame answered, or refused, from a record: another record holds
	// the key this parent derives.
	msg := fmt.Sprintf("the call %s has no record, yet the frame answered it from the ledger", n.key)
	if err != nil {
		msg = err.Error()
	}
	d.rep.Divergence = &Finding{Key: n.key, Message: msg}
	return true
}

// stop records the frontier, with the parents being re-executed; only the
// first one counts.
func (d *driver) stop(f *Frontier) {
	if d.rep.Frontier != nil {
		return
	}
	f.Reexecutes = slices.Clone(d.stack)
	d.rep.Frontier = f
}

// buildTree parses each record's key against its kind and name and returns
// every node by key, the top-level calls in replay order, and the records no
// call could have produced.
func buildTree(records []run.Record) (map[string]*node, []*node, []Finding) {
	nodes := make(map[string]*node, len(records))
	var problems []Finding
	malformed := make(map[string]bool)
	sorted := sortedRecords(records)
	for i := range sorted {
		rec := &sorted[i]
		n, msg := parseRecord(rec)
		if msg != "" {
			problems = append(problems, Finding{Key: rec.Key, Message: msg})
			malformed[rec.Key] = true
			continue
		}
		nodes[n.key] = n
	}

	// Link every node to its parent, conjuring parents whose records are
	// missing from the key alone. A non-empty result says why a node
	// cannot be placed.
	var roots []*node
	var link func(n *node) string
	link = func(n *node) string {
		if n.parent == "" {
			roots = append(roots, n)
			return ""
		}
		if malformed[n.parent] {
			return fmt.Sprintf("nested under %q, a record no call could have produced", n.parent)
		}
		p, ok := nodes[n.parent]
		if !ok {
			p, ok = parseMissingParent(n.parent)
			if !ok {
				return fmt.Sprintf("nested under %q, a key no call could have produced", n.parent)
			}
			if p.kind != run.KindEvent {
				if why := link(p); why != "" {
					return why
				}
				nodes[p.key] = p
			}
		}
		if p.kind == run.KindEvent {
			return fmt.Sprintf("nested under the event %q, which has no body to nest calls in", n.parent)
		}
		p.kids = append(p.kids, n)
		return ""
	}
	keys := make([]string, 0, len(nodes))
	for _, rec := range sorted {
		if _, ok := nodes[rec.Key]; ok {
			keys = append(keys, rec.Key)
		}
	}
	for _, k := range keys {
		if why := link(nodes[k]); why != "" {
			delete(nodes, k)
			problems = append(problems, Finding{Key: k, Message: why})
		}
	}

	// A parent with no record starts before its first nested call records.
	var firstOf func(n *node) time.Time
	firstOf = func(n *node) time.Time {
		if n.rec != nil {
			return n.first
		}
		for _, k := range n.kids {
			if t := firstOf(k); n.first.IsZero() || t.Before(n.first) {
				n.first = t
			}
		}
		return n.first
	}
	var order func(ns []*node)
	order = func(ns []*node) {
		for _, n := range ns {
			firstOf(n)
			order(n.kids)
		}
		sortCalls(ns)
	}
	order(roots)
	return nodes, roots, problems
}

// sortCalls orders one scope's calls by when they were recorded, then puts
// each counter's occurrences in ascending order in the slots that counter's
// calls hold, so clock skew between workers cannot reorder a counter.
func sortCalls(ns []*node) {
	slices.SortStableFunc(ns, func(a, b *node) int {
		if c := a.first.Compare(b.first); c != 0 {
			return c
		}
		return strings.Compare(a.key, b.key)
	})
	slots := make(map[string][]int)
	for i, n := range ns {
		if b := n.base(); b != "" {
			slots[b] = append(slots[b], i)
		}
	}
	for _, idx := range slots {
		group := make([]*node, len(idx))
		for j, i := range idx {
			group[j] = ns[i]
		}
		slices.SortStableFunc(group, func(a, b *node) int { return a.occ - b.occ })
		for j, i := range idx {
			ns[i] = group[j]
		}
	}
}

// parseRecord derives a record's place in the call tree from its key, kind
// and name, the way the frame derives keys: "name#k" for activities and
// steps, "event:name#k" for events, "k:userkey" for keyed activities, each
// under "parentkey/" when nested. msg says why the key could not have been
// produced.
func parseRecord(rec *run.Record) (*node, string) {
	switch rec.Kind {
	case run.KindActivity, run.KindStep, run.KindEvent:
	default:
		return nil, fmt.Sprintf("unknown kind %q", rec.Kind)
	}
	switch rec.Status {
	case run.RecordOK, run.RecordFailed:
	default:
		return nil, fmt.Sprintf("unknown record status %q", rec.Status)
	}
	n := &node{key: rec.Key, rec: rec, kind: rec.Kind, name: rec.Name, occ: -1, first: rec.RecordedAt}

	base := rec.Name
	if rec.Kind == run.KindEvent {
		base = "event:" + rec.Name
	}
	if i := strings.LastIndexByte(rec.Key, '#'); i >= 0 {
		if occ, ok := occurrence(rec.Key[i+1:]); ok {
			if pre, ok := strings.CutSuffix(rec.Key[:i], base); ok {
				if pre == "" {
					n.occ = occ
					return n, ""
				}
				if parent, ok := strings.CutSuffix(pre, "/"); ok && parent != "" {
					n.occ, n.parent = occ, parent
					return n, ""
				}
			}
		}
	}
	if rec.Kind == run.KindActivity {
		// A keyed call: the last "k:" segment. A user key that itself
		// holds "/k:" reads as nested; the key scheme cannot tell them
		// apart.
		j := -1
		if strings.HasPrefix(rec.Key, "k:") {
			j = 0
		}
		if i := strings.LastIndex(rec.Key, "/k:"); i >= 0 {
			j = i + 1
		}
		if j >= 0 && len(rec.Key) > j+2 {
			n.userKey = rec.Key[j+2:]
			if j > 0 {
				n.parent = rec.Key[:j-1]
			}
			return n, ""
		}
	}
	want := `"` + base + `#<n>"`
	if rec.Kind == run.KindActivity {
		want += ` or "k:<key>"`
	}
	return nil, fmt.Sprintf(`key does not derive from %s %q: want %s, optionally under "<parent key>/"`, rec.Kind, rec.Name, want)
}

// parseMissingParent reads a call whose record is missing from its key
// alone: the last segment names it. A name that itself holds "/" reads as
// nested.
func parseMissingParent(key string) (*node, bool) {
	parent, seg := "", key
	if i := strings.LastIndexByte(key, '/'); i >= 0 {
		parent, seg = key[:i], key[i+1:]
		if parent == "" {
			return nil, false
		}
	}
	n := &node{key: key, kind: run.KindActivity, occ: -1, parent: parent}
	if uk, ok := strings.CutPrefix(seg, "k:"); ok && uk != "" {
		n.userKey = uk
		return n, true
	}
	i := strings.LastIndexByte(seg, '#')
	if i < 0 {
		return nil, false
	}
	occ, ok := occurrence(seg[i+1:])
	if !ok {
		return nil, false
	}
	n.name, n.occ = seg[:i], occ
	if strings.HasPrefix(n.name, "event:") {
		n.kind = run.KindEvent
	}
	return n, true
}

// occurrence parses a canonical occurrence number: digits, no leading zero.
func occurrence(s string) (int, bool) {
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 || strconv.Itoa(n) != s {
		return 0, false
	}
	return n, true
}

// validHash reports whether h is a sha256 hex digest, as run.HashInput
// writes.
func validHash(h string) bool {
	if len(h) != 64 {
		return false
	}
	for _, c := range h {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// sortedRecords returns records in recorded order: by recording time, then
// key.
func sortedRecords(records []run.Record) []run.Record {
	out := slices.Clone(records)
	slices.SortStableFunc(out, func(a, b run.Record) int {
		if c := a.RecordedAt.Compare(b.RecordedAt); c != 0 {
			return c
		}
		return strings.Compare(a.Key, b.Key)
	})
	return out
}

// refusingLedger is the frame's ledger: it refuses every write and every
// read. The replay never reaches it, since the frontier hands back
// errFrontier before anything could be recorded; it is there so that
// nothing could be written if it did.
type refusingLedger struct{}

var errReadOnly = errors.New("anchor replay: the ledger is read-only")

func (refusingLedger) Accept(context.Context, run.Run) error { return errReadOnly }
func (refusingLedger) Complete(context.Context, string, int, run.Result) error {
	return errReadOnly
}

func (refusingLedger) Load(context.Context, string) (run.Run, []run.Record, error) {
	return run.Run{}, nil, errReadOnly
}

func (refusingLedger) Record(context.Context, run.Record) (run.Record, error) {
	return run.Record{}, errReadOnly
}

func (refusingLedger) ListPending(context.Context, time.Time, run.PendingCursor, int) ([]run.PendingRun, error) {
	return nil, errReadOnly
}

// contentTypeCodec is the frame's codec: the replay decodes nothing, so all
// the frame needs is the content type to check records against.
type contentTypeCodec string

func codecOf(contentType string) run.Codec { return contentTypeCodec(contentType) }

func (c contentTypeCodec) Marshal(any) ([]byte, error) { return nil, errReadOnly }
func (c contentTypeCodec) Unmarshal([]byte, any) error { return errReadOnly }
func (c contentTypeCodec) ContentType() string         { return string(c) }
