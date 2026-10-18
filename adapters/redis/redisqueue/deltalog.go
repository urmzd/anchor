package redisqueue

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/urmzd/anchor/pkg/queue"
	"github.com/urmzd/anchor/pkg/run"
)

// Stream entry field names. The payload travels as raw bytes; everything
// else is its decimal or literal string form.
const (
	fieldKey     = "key"
	fieldAttempt = "attempt"
	fieldKind    = "kind"
	fieldPayload = "payload"
)

// readBatch is how many entries a Watch pulls per catch-up XRANGE.
const readBatch = 64

// Append implements queue.DeltaLog. The append is fenced in the same script
// that writes it: a delta carrying an attempt below the run's current
// counter gets run.ErrSuperseded and writes nothing.
func (q *Queue) Append(ctx context.Context, d run.Delta) error {
	res, err := scriptAppend.Run(ctx, q.client,
		[]string{q.keyAttempt, q.deltaKey(d.RunID)},
		d.RunID, d.Attempt, d.RecordKey, string(d.Kind), d.Payload,
	).Int64()
	if err != nil {
		return fmt.Errorf("redisqueue: append %q attempt %d: %w", d.RunID, d.Attempt, err)
	}
	if res == supersededReply {
		return wrapSuperseded("append", d.RunID, d.Attempt)
	}
	return nil
}

// Read implements queue.DeltaLog: a bounded snapshot from the cursor, in
// append order, plus the cursor to continue from. The cursor is the stream
// ID of the last delta returned; when nothing is returned the input cursor
// comes back unchanged. limit <= 0 means no bound.
func (q *Queue) Read(ctx context.Context, runID string, from queue.Cursor, limit int) ([]run.Delta, queue.Cursor, error) {
	key := q.deltaKey(runID)
	start := rangeStart(from)

	var msgs []redis.XMessage
	var err error
	if limit > 0 {
		msgs, err = q.client.XRangeN(ctx, key, start, "+", int64(limit)).Result()
	} else {
		msgs, err = q.client.XRange(ctx, key, start, "+").Result()
	}
	if err != nil {
		return nil, "", fmt.Errorf("redisqueue: read %q from %q: %w", runID, from, err)
	}

	deltas := make([]run.Delta, 0, len(msgs))
	next := from
	for _, m := range msgs {
		d, err := deltaFromValues(runID, m.Values)
		if err != nil {
			return nil, "", fmt.Errorf("redisqueue: read %q entry %s: %w", runID, m.ID, err)
		}
		deltas = append(deltas, d)
		next = queue.Cursor(m.ID)
	}
	return deltas, next, nil
}

// Watch implements queue.DeltaLog. The returned Stream serves recorded
// history from the cursor first, then poll-tails the stream at PollInterval
// until ctx is done. Watchers are independent: each holds only its own
// cursor.
func (q *Queue) Watch(ctx context.Context, runID string, from queue.Cursor) (queue.Stream, error) {
	return &stream{q: q, runID: runID, cursor: from}, nil
}

// Trim implements queue.DeltaLog: it drops the run's whole stream. Queue
// state and the attempt counter are untouched.
func (q *Queue) Trim(ctx context.Context, runID string) error {
	if err := q.client.Del(ctx, q.deltaKey(runID)).Err(); err != nil {
		return fmt.Errorf("redisqueue: trim %q: %w", runID, err)
	}
	return nil
}

// stream is a Watch handle: a cursor into one run's delta stream plus a
// buffer of fetched-but-undelivered entries. Not safe for concurrent Recv.
type stream struct {
	q      *Queue
	runID  string
	cursor queue.Cursor
	buf    []bufferedDelta
}

// bufferedDelta pairs a decoded delta with the cursor it advances to.
type bufferedDelta struct {
	d   run.Delta
	cur queue.Cursor
}

// Recv implements queue.Stream. It drains the local buffer, refills it with
// an exclusive-start XRANGE, and otherwise sleeps one poll interval — always
// honoring ctx between rounds.
func (s *stream) Recv(ctx context.Context) (run.Delta, queue.Cursor, error) {
	for {
		if err := ctx.Err(); err != nil {
			return run.Delta{}, "", err
		}
		if len(s.buf) > 0 {
			next := s.buf[0]
			s.buf = s.buf[1:]
			s.cursor = next.cur
			return next.d, next.cur, nil
		}

		msgs, err := s.q.client.XRangeN(ctx, s.q.deltaKey(s.runID), rangeStart(s.cursor), "+", readBatch).Result()
		if err != nil {
			return run.Delta{}, "", fmt.Errorf("redisqueue: watch %q: %w", s.runID, err)
		}
		for _, m := range msgs {
			d, err := deltaFromValues(s.runID, m.Values)
			if err != nil {
				return run.Delta{}, "", fmt.Errorf("redisqueue: watch %q entry %s: %w", s.runID, m.ID, err)
			}
			s.buf = append(s.buf, bufferedDelta{d: d, cur: queue.Cursor(m.ID)})
		}
		if len(s.buf) > 0 {
			continue
		}

		timer := time.NewTimer(s.q.pollDelay())
		select {
		case <-ctx.Done():
			timer.Stop()
			return run.Delta{}, "", ctx.Err()
		case <-timer.C:
		}
	}
}

// rangeStart maps a cursor to an XRANGE start: the beginning for the empty
// cursor, else exclusive of the last-delivered entry ID.
func rangeStart(from queue.Cursor) string {
	if from == "" {
		return "-"
	}
	return "(" + string(from)
}

// deltaFromValues decodes one stream entry's field map into a run.Delta.
func deltaFromValues(runID string, values map[string]any) (run.Delta, error) {
	key, err := stringField(values, fieldKey)
	if err != nil {
		return run.Delta{}, err
	}
	kind, err := stringField(values, fieldKind)
	if err != nil {
		return run.Delta{}, err
	}
	attemptStr, err := stringField(values, fieldAttempt)
	if err != nil {
		return run.Delta{}, err
	}
	attempt, err := strconv.Atoi(attemptStr)
	if err != nil {
		return run.Delta{}, fmt.Errorf("invalid attempt %q: %w", attemptStr, err)
	}
	payload, err := stringField(values, fieldPayload)
	if err != nil {
		return run.Delta{}, err
	}
	return run.Delta{
		RunID:     runID,
		RecordKey: key,
		Attempt:   attempt,
		Kind:      run.DeltaKind(kind),
		Payload:   []byte(payload),
	}, nil
}

// stringField extracts one field, tolerating both string and []byte values.
func stringField(values map[string]any, field string) (string, error) {
	v, ok := values[field]
	if !ok {
		return "", fmt.Errorf("missing field %q", field)
	}
	switch s := v.(type) {
	case string:
		return s, nil
	case []byte:
		return string(s), nil
	default:
		return "", fmt.Errorf("field %q has unexpected type %T", field, v)
	}
}
