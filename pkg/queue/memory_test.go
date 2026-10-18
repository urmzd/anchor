package queue_test

import (
	"testing"

	"github.com/urmzd/anchor/pkg/queue"
	"github.com/urmzd/anchor/pkg/queue/queuetest"
)

func TestMemoryQueue(t *testing.T) {
	queuetest.Run(t, func(t *testing.T) queue.Queue {
		return queue.NewMemory()
	})
}

func TestMemoryDeltaLog(t *testing.T) {
	queuetest.RunDeltaLog(t, func(t *testing.T) queue.DeltaLog {
		return queue.NewMemory()
	})
}
