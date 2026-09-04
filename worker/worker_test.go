package worker_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/doguhanniltextra/distributed-queue/lock"
	"github.com/doguhanniltextra/distributed-queue/queue"
	"github.com/doguhanniltextra/distributed-queue/store"
	"github.com/doguhanniltextra/distributed-queue/worker"
)

func setupTestEnvironment() (*queue.Queue, *queue.DLQ, *lock.DistributedLock) {
	ms := store.NewMemoryStore()
	q := queue.NewQueue(ms)
	dlq := queue.NewDLQ()
	dl := lock.NewDistributedLock()
	return q, dlq, dl
}

func TestWorkerSuccess(t *testing.T) {
	q, dlq, dl := setupTestEnvironment()

	w := worker.NewWorker("worker-1", q, dlq, dl, worker.WorkerConfig{
		Concurrency:    2,
		HandlerTimeout: 1 * time.Second,
		LockTTL:        5 * time.Second,
	})

	var processed int64
	w.RegisterHandler("simple_task", func(ctx context.Context, task *queue.Task) error {
		atomic.AddInt64(&processed, 1)
		return nil
	})

	_ = q.Push(queue.NewTask("simple_task", nil))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	// Wait for processing
	time.Sleep(200 * time.Millisecond)
	cancel()

	if atomic.LoadInt64(&processed) != 1 {
		t.Fatalf("expected 1 processed task, got %d", atomic.LoadInt64(&processed))
	}

	stats := w.Stats()
	if stats.Completed != 1 {
		t.Errorf("expected Completed=1, got %d", stats.Completed)
	}
}

func TestWorkerConcurrencyLimit(t *testing.T) {
	q, dlq, dl := setupTestEnvironment()

	const maxConcurrent = 3
	w := worker.NewWorker("worker-concurrency", q, dlq, dl, worker.WorkerConfig{
		Concurrency:    maxConcurrent,
		HandlerTimeout: 2 * time.Second,
		LockTTL:        5 * time.Second,
	})

	var active int64
	var maxObserved int64
	var mu sync.Mutex

	w.RegisterHandler("slow_task", func(ctx context.Context, task *queue.Task) error {
		current := atomic.AddInt64(&active, 1)
		mu.Lock()
		if current > maxObserved {
			maxObserved = current
		}
		mu.Unlock()

		time.Sleep(100 * time.Millisecond)
		atomic.AddInt64(&active, -1)
		return nil
	})

	for i := 0; i < 9; i++ {
		_ = q.Push(queue.NewTask("slow_task", nil))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	time.Sleep(500 * time.Millisecond)
	cancel()

	mu.Lock()
	observed := maxObserved
	mu.Unlock()

	if observed > int64(maxConcurrent) {
		t.Fatalf("concurrency limit exceeded: max allowed %d, observed %d", maxConcurrent, observed)
	}
}

func TestWorkerPanicRecovery(t *testing.T) {
	q, dlq, dl := setupTestEnvironment()

	w := worker.NewWorker("worker-panic", q, dlq, dl, worker.WorkerConfig{
		Concurrency:    1,
		HandlerTimeout: 1 * time.Second,
		LockTTL:        5 * time.Second,
	})

	w.RegisterHandler("panic_task", func(ctx context.Context, task *queue.Task) error {
		panic("simulated fatal panic")
	})

	task := queue.NewTask("panic_task", nil)
	task.MaxRetries = 1 // Fail immediately to DLQ
	_ = q.Push(task)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	time.Sleep(200 * time.Millisecond)
	cancel()

	stats := w.Stats()
	if stats.Panicked != 1 {
		t.Errorf("expected Panicked=1, got %d", stats.Panicked)
	}
	if dlq.Size() != 1 {
		t.Errorf("expected task to be quarantined in DLQ, dlq size: %d", dlq.Size())
	}
}

func TestWorkerTimeout(t *testing.T) {
	q, dlq, dl := setupTestEnvironment()

	w := worker.NewWorker("worker-timeout", q, dlq, dl, worker.WorkerConfig{
		Concurrency:    1,
		HandlerTimeout: 100 * time.Millisecond,
		LockTTL:        5 * time.Second,
	})

	w.RegisterHandler("sleep_forever", func(ctx context.Context, task *queue.Task) error {
		select {
		case <-time.After(1 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	task := queue.NewTask("sleep_forever", nil)
	task.MaxRetries = 1 // 1 attempt -> directly DLQ on failure
	_ = q.Push(task)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	time.Sleep(300 * time.Millisecond)
	cancel()

	stats := w.Stats()
	if stats.Failed != 1 {
		t.Errorf("expected Failed=1, got %d", stats.Failed)
	}
	if dlq.Size() != 1 {
		t.Errorf("expected task in DLQ after timeout, DLQ size = %d", dlq.Size())
	}
}

func TestWorkerRetryAndDLQ(t *testing.T) {
	q, dlq, dl := setupTestEnvironment()

	w := worker.NewWorker("worker-retry", q, dlq, dl, worker.WorkerConfig{
		Concurrency:    1,
		HandlerTimeout: 500 * time.Millisecond,
		LockTTL:        5 * time.Second,
	})

	var attempts int64
	w.RegisterHandler("failing_task", func(ctx context.Context, task *queue.Task) error {
		atomic.AddInt64(&attempts, 1)
		return errors.New("hard failure")
	})

	task := queue.NewTask("failing_task", nil)
	task.MaxRetries = 2
	_ = q.Push(task)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	// First attempt runs immediately
	time.Sleep(200 * time.Millisecond)

	// Retried with backoff 2^1 = 2 seconds
	if atomic.LoadInt64(&attempts) != 1 {
		t.Fatalf("expected 1 attempt, got %d", atomic.LoadInt64(&attempts))
	}
	if dlq.Size() != 0 {
		t.Errorf("should not be in DLQ yet")
	}

	cancel()
}
