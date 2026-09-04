package worker_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/doguhanniltextra/distributed-queue/queue"
	"github.com/doguhanniltextra/distributed-queue/worker"
)

func TestWorkerPoolStartAndGracefulStop(t *testing.T) {
	q, dlq, dl := setupTestEnvironment()

	pool := worker.NewPool(q, dlq, dl)
	pool.AddWorker("worker-1", worker.WorkerConfig{Concurrency: 2})
	pool.AddWorker("worker-2", worker.WorkerConfig{Concurrency: 2})

	var processedCount int64
	pool.RegisterHandler("pool_task", func(ctx context.Context, task *queue.Task) error {
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt64(&processedCount, 1)
		return nil
	})

	for i := 0; i < 4; i++ {
		_ = q.Push(queue.NewTask("pool_task", nil))
	}

	ctx, cancel := context.WithCancel(context.Background())

	pool.Start(ctx)

	// Wait for processing to begin and complete
	time.Sleep(200 * time.Millisecond)

	// Signal shutdown
	cancel()
	pool.GracefulStop()

	if atomic.LoadInt64(&processedCount) != 4 {
		t.Errorf("expected 4 processed tasks, got %d", atomic.LoadInt64(&processedCount))
	}

	stats := pool.Stats()
	if len(stats) != 2 {
		t.Fatalf("expected stats for 2 workers, got %d", len(stats))
	}

	var totalCompleted int64
	for _, s := range stats {
		totalCompleted += s.Completed
	}
	if totalCompleted != 4 {
		t.Errorf("expected totalCompleted 4 across pool, got %d", totalCompleted)
	}
}
