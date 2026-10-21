package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/urmzd/anchor/pkg/run"
)

// textWidth bounds a payload or error in text output; -json prints them
// whole.
const textWidth = 120

// cmdRuns lists runs, newest first.
func cmdRuns(ctx context.Context, o *options, args []string, stdout io.Writer) error {
	if len(args) != 0 {
		return usageError{msg: fmt.Sprintf("runs takes no arguments, got %q", args)}
	}
	status := run.RunStatus(o.status)
	switch status {
	case "", run.StatusPending, run.StatusSucceeded, run.StatusFailed:
	default:
		return usageError{msg: fmt.Sprintf("-status %q: want pending, succeeded or failed", o.status)}
	}
	if o.limit <= 0 {
		return usageError{msg: fmt.Sprintf("-limit %d: want a positive number", o.limit)}
	}
	lgr, closeDB, err := openLedger(ctx, o)
	if err != nil {
		return err
	}
	defer closeDB()

	runs, err := lgr.ListRuns(ctx, status, o.limit)
	if err != nil {
		return err
	}
	if o.json {
		out := struct {
			Runs []runJSON `json:"runs"`
		}{Runs: make([]runJSON, 0, len(runs))}
		for _, r := range runs {
			out.Runs = append(out.Runs, toRunJSON(r))
		}
		return writeJSON(stdout, out)
	}
	if len(runs) == 0 {
		fmt.Fprintln(stdout, "no runs")
		return nil
	}
	tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "RUN\tNAME\tSTATUS\tATTEMPT\tCREATED\tCOMPLETED\tERROR")
	for _, r := range runs {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			cell(r.ID), cell(r.Name), cell(string(r.Status)), attemptText(r.Status, r.Attempt), textTime(r.CreatedAt), textTime(r.CompletedAt), orDash(cell(clip(r.Error, 60))))
	}
	return tw.Flush()
}

// cmdShow prints one run, its records in recorded order, and its status.
func cmdShow(ctx context.Context, o *options, args []string, stdout io.Writer) error {
	runID, err := oneRun("show", args)
	if err != nil {
		return err
	}
	r, records, err := load(ctx, o, runID)
	if err != nil {
		return err
	}
	records = sortedRecords(records)

	if o.json {
		out := struct {
			Run     runJSON      `json:"run"`
			Records []recordJSON `json:"records"`
			Status  string       `json:"status"`
		}{Run: toRunJSON(r), Records: make([]recordJSON, 0, len(records)), Status: string(r.Status)}
		for _, rec := range records {
			out.Records = append(out.Records, toRecordJSON(rec))
		}
		return writeJSON(stdout, out)
	}

	tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	field := func(name, value string) { fmt.Fprintf(tw, "%s\t%s\n", name, value) }
	field("run", cell(r.ID))
	field("name", cell(r.Name))
	field("status", string(r.Status))
	field("attempt", attemptText(r.Status, r.Attempt))
	field("max attempts", strconv.Itoa(r.MaxAttempts))
	field("parent", orDash(cell(r.ParentRunID)))
	field("created", textTime(r.CreatedAt))
	field("completed", textTime(r.CompletedAt))
	field("input", textPayload(r.Input))
	field("output", textPayload(r.Output))
	field("error", orDash(cell(clip(r.Error, textWidth))))
	if err := tw.Flush(); err != nil {
		return err
	}

	fmt.Fprintf(stdout, "\nrecords (%d, in recorded order)\n", len(records))
	if len(records) > 0 {
		tw = tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "  KEY\tKIND\tSTATUS\tATTEMPT\tRECORDED\tRESULT")
		for _, rec := range records {
			result := textPayload(rec.Output)
			if rec.Status == run.RecordFailed {
				result = "error: " + cell(clip(rec.Error, textWidth))
			}
			fmt.Fprintf(tw, "  %s\t%s\t%s\t%d\t%s\t%s\n",
				cell(rec.Key), cell(string(rec.Kind)), cell(string(rec.Status)), rec.Attempt, textTime(rec.RecordedAt), result)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	fmt.Fprintf(stdout, "\nstatus: %s\n", statusLine(r.Status, r.Attempt))
	return nil
}

// cmdReplay replays a run's recorded calls without side effects.
func cmdReplay(ctx context.Context, o *options, args []string, stdout io.Writer) error {
	runID, err := oneRun("replay", args)
	if err != nil {
		return err
	}
	if o.codec == "" {
		return usageError{msg: "-codec: want a content type"}
	}
	r, records, err := load(ctx, o, runID)
	if err != nil {
		return err
	}
	rep := replayRun(ctx, r, records, o.codec)
	if o.json {
		err = writeJSON(stdout, toReplayJSON(rep))
	} else {
		err = writeReplay(stdout, rep)
	}
	if err != nil {
		return err
	}
	if !rep.OK() {
		return divergedError{}
	}
	return nil
}

func oneRun(cmd string, args []string) (string, error) {
	if len(args) != 1 || args[0] == "" {
		return "", usageError{msg: fmt.Sprintf("%s takes one RUN argument, got %d", cmd, len(args))}
	}
	return args[0], nil
}

// load reads one run and all of its records.
func load(ctx context.Context, o *options, runID string) (run.Run, []run.Record, error) {
	lgr, closeDB, err := openLedger(ctx, o)
	if err != nil {
		return run.Run{}, nil, err
	}
	defer closeDB()
	r, records, err := lgr.Load(ctx, runID)
	if errors.Is(err, run.ErrNotFound) {
		return run.Run{}, nil, fmt.Errorf("run %q not found in %s", runID, o.runsTable)
	}
	return r, records, err
}

// writeReplay prints a replay report as text.
func writeReplay(w io.Writer, rep *Report) error {
	fmt.Fprintf(w, "run %s (%s), %s, %d %s\n", cell(rep.RunID), cell(rep.Name), cell(string(rep.Status)), rep.Records, plural(rep.Records, "record", "records"))
	if len(rep.Replayed) > 0 {
		fmt.Fprintln(w, "\nreplayed from the ledger, nothing executed:")
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		for _, c := range rep.Replayed {
			result := "recorded"
			if c.Failed {
				result = "recorded failure, replays as *run.RecordedError"
			}
			fmt.Fprintf(tw, "  %s\t%s\t%s\n", cell(c.Key), c.Kind, result)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	fmt.Fprintln(w)
	if n := len(rep.Covered); n > 0 {
		fmt.Fprintf(w, "covered: %d nested %s, answered by the recorded %s nested in\n",
			n, plural(n, "record", "records"), plural(n, "call it is", "calls they are"))
	}
	if len(rep.Unreached) > 0 {
		fmt.Fprintf(w, "not reached: %s\n", strings.Join(cells(rep.Unreached), ", "))
	}
	for _, p := range rep.Problems {
		fmt.Fprintf(w, "problem: %s: %s\n", cell(p.Key), cell(p.Message))
	}

	switch {
	case rep.Complete():
		fmt.Fprintf(w, "complete: %s; a worker settles it without replaying\n", statusLine(rep.Status, rep.Attempt))
	case rep.Frontier != nil:
		fmt.Fprintf(w, "resume: %s\n", frontierText(rep.Frontier))
	}
	if d := rep.Divergence; d != nil {
		consequence := "a worker's next claim fails the run there"
		switch {
		case rep.Complete():
			consequence = "the run is already terminal, but its history does not replay"
		case rep.Frontier != nil:
			consequence = "after the frontier, the run fails there"
		}
		fmt.Fprintf(w, "diverges: %s: %s\n  %s\n", cell(d.Key), cell(d.Message), consequence)
	}
	return nil
}

// frontierText says where a worker resumes.
func frontierText(f *Frontier) string {
	var b strings.Builder
	for _, k := range f.Reexecutes {
		fmt.Fprintf(&b, "%s has no record and re-executes; inside it, ", cell(k))
	}
	switch {
	case f.Parks:
		fmt.Fprintf(&b, "%s has no record, so the run parks there until the event is written", cell(f.At))
	case f.At != "":
		fmt.Fprintf(&b, "%s has no record and executes for real", cell(f.At))
	case f.After != "":
		fmt.Fprintf(&b, "the next call after %s executes for real, or parks the run if it awaits an event", cell(f.After))
	default:
		b.WriteString("the first call executes for real, or parks the run if it awaits an event")
	}
	return b.String()
}

// statusLine is a run's status, with the attempt that wrote a terminal one.
func statusLine(status run.RunStatus, attempt int) string {
	if status.Terminal() {
		return fmt.Sprintf("%s at attempt %d", status, attempt)
	}
	return string(status)
}

// attemptText is the attempt that wrote a terminal result; a pending run
// has none yet (the live attempt is queue state).
func attemptText(status run.RunStatus, attempt int) string {
	if !status.Terminal() && attempt == 0 {
		return "-"
	}
	return strconv.Itoa(attempt)
}

// JSON shapes. Payloads that are valid JSON (the default codec) appear as
// JSON under "input" and "output"; any other bytes appear base64-encoded
// under "input_base64" and "output_base64".

type runJSON struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	Status       string          `json:"status"`
	Attempt      int             `json:"attempt"`
	MaxAttempts  int             `json:"max_attempts"`
	ParentRunID  string          `json:"parent_run_id,omitempty"`
	CreatedAt    string          `json:"created_at,omitempty"`
	CompletedAt  string          `json:"completed_at,omitempty"`
	Input        json.RawMessage `json:"input,omitempty"`
	InputBase64  []byte          `json:"input_base64,omitempty"`
	Output       json.RawMessage `json:"output,omitempty"`
	OutputBase64 []byte          `json:"output_base64,omitempty"`
	Error        string          `json:"error,omitempty"`
}

type recordJSON struct {
	Key          string          `json:"key"`
	Kind         string          `json:"kind"`
	Name         string          `json:"name"`
	Status       string          `json:"status"`
	Attempt      int             `json:"attempt"`
	Codec        string          `json:"codec,omitempty"`
	InputHash    string          `json:"input_hash,omitempty"`
	RecordedAt   string          `json:"recorded_at,omitempty"`
	Output       json.RawMessage `json:"output,omitempty"`
	OutputBase64 []byte          `json:"output_base64,omitempty"`
	Error        string          `json:"error,omitempty"`
}

type replayJSON struct {
	RunID      string         `json:"run_id"`
	Name       string         `json:"name"`
	Status     string         `json:"status"`
	Records    int            `json:"records"`
	OK         bool           `json:"ok"`
	Complete   bool           `json:"complete"`
	Replayed   []replayedJSON `json:"replayed"`
	Covered    []string       `json:"covered"`
	Frontier   *frontierJSON  `json:"frontier,omitempty"`
	Unreached  []string       `json:"unreached"`
	Divergence *findingJSON   `json:"divergence,omitempty"`
	Problems   []findingJSON  `json:"problems"`
}

type replayedJSON struct {
	Key    string `json:"key"`
	Kind   string `json:"kind"`
	Name   string `json:"name"`
	Failed bool   `json:"failed,omitempty"`
}

type frontierJSON struct {
	Reexecutes []string `json:"reexecutes"`
	At         string   `json:"at,omitempty"`
	Parks      bool     `json:"parks,omitempty"`
	After      string   `json:"after,omitempty"`
}

type findingJSON struct {
	Key     string `json:"key"`
	Message string `json:"message"`
}

func toRunJSON(r run.Run) runJSON {
	out := runJSON{
		ID: r.ID, Name: r.Name, Status: string(r.Status), Attempt: r.Attempt, MaxAttempts: r.MaxAttempts,
		ParentRunID: r.ParentRunID, CreatedAt: jsonTime(r.CreatedAt), CompletedAt: jsonTime(r.CompletedAt), Error: r.Error,
	}
	out.Input, out.InputBase64 = jsonPayload(r.Input)
	out.Output, out.OutputBase64 = jsonPayload(r.Output)
	return out
}

func toRecordJSON(rec run.Record) recordJSON {
	out := recordJSON{
		Key: rec.Key, Kind: string(rec.Kind), Name: rec.Name, Status: string(rec.Status), Attempt: rec.Attempt,
		Codec: rec.Codec, InputHash: rec.InputHash, RecordedAt: jsonTime(rec.RecordedAt), Error: rec.Error,
	}
	out.Output, out.OutputBase64 = jsonPayload(rec.Output)
	return out
}

func toReplayJSON(rep *Report) replayJSON {
	out := replayJSON{
		RunID: rep.RunID, Name: rep.Name, Status: string(rep.Status), Records: rep.Records,
		OK: rep.OK(), Complete: rep.Complete(),
		Replayed: make([]replayedJSON, 0, len(rep.Replayed)), Covered: nonNil(rep.Covered),
		Unreached: nonNil(rep.Unreached), Problems: make([]findingJSON, 0, len(rep.Problems)),
	}
	if d := rep.Divergence; d != nil {
		out.Divergence = &findingJSON{Key: d.Key, Message: d.Message}
	}
	for _, c := range rep.Replayed {
		out.Replayed = append(out.Replayed, replayedJSON{Key: c.Key, Kind: string(c.Kind), Name: c.Name, Failed: c.Failed})
	}
	if f := rep.Frontier; f != nil {
		out.Frontier = &frontierJSON{Reexecutes: nonNil(f.Reexecutes), At: f.At, Parks: f.Parks, After: f.After}
	}
	for _, p := range rep.Problems {
		out.Problems = append(out.Problems, findingJSON(p))
	}
	return out
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

// jsonPayload splits a payload into its JSON form, when it is JSON, or its
// raw bytes otherwise.
func jsonPayload(b []byte) (json.RawMessage, []byte) {
	if len(b) == 0 {
		return nil, nil
	}
	if json.Valid(b) {
		return json.RawMessage(b), nil
	}
	return nil, b
}

// textPayload renders a payload on one line: compact JSON, a quoted string
// for other text, base64 for binary, clipped to textWidth.
func textPayload(b []byte) string {
	if len(b) == 0 {
		return "-"
	}
	var s string
	var buf bytes.Buffer
	switch {
	case json.Valid(b) && json.Compact(&buf, b) == nil:
		s = buf.String()
	case utf8.Valid(b):
		s = strconv.Quote(string(b))
	default:
		s = "base64:" + base64.StdEncoding.EncodeToString(b)
	}
	if utf8.RuneCountInString(s) > textWidth {
		return clip(s, textWidth) + fmt.Sprintf(" (%d bytes)", len(b))
	}
	return s
}

// clip shortens s to at most n runes, marking the cut.
func clip(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return string(r[:n-3]) + "..."
}

// cell makes a value safe for one line of text output: control characters,
// newlines and tabs among them, are escaped so a stored value cannot break
// the table or the terminal.
func cell(s string) string {
	if strings.IndexFunc(s, unicode.IsControl) < 0 {
		return s
	}
	q := strconv.QuoteToGraphic(s)
	return q[1 : len(q)-1]
}

func cells(ss []string) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = cell(s)
	}
	return out
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func textTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

func jsonTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

func nonNil(ss []string) []string {
	if ss == nil {
		return []string{}
	}
	return ss
}
