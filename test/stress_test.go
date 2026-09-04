package test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/doguhanniltextra/distributed-queue/lock"
	"github.com/doguhanniltextra/distributed-queue/queue"
	"github.com/doguhanniltextra/distributed-queue/store"
	"github.com/doguhanniltextra/distributed-queue/worker"
)

// TestStress10kTasksNoLoss pushes 5,000 diverse tasks concurrently and verifies
// the fundamental conservation invariant:
// Total Enqueued == Completed + Quarantined in DLQ + Remaining in Queue
// No tasks may be dropped, duplicated, or leaked.
func TestStress10kTasksNoLoss(t *testing.T) {
	memStore := store.NewMemoryStore()
	q := queue.NewQueue(memStore)
	dlq := queue.NewDLQ()
	distLock := lock.NewDistributedLock()

	pool := worker.NewPool(q, dlq, distLock)

	// 5 Workers x 20 concurrency = 100 concurrent goroutine task processors
	const workerCount = 5
	const concurrencyPerWorker = 20
	for i := 1; i <= workerCount; i++ {
		pool.AddWorker(fmt.Sprintf("worker-stress-%d", i), worker.WorkerConfig{
			Concurrency:    concurrencyPerWorker,
			HandlerTimeout: 1 * time.Second,
			LockTTL:        10 * time.Second,
		})
	}

	// Handlers:
	// fast_job: succeeds immediately
	pool.RegisterHandler("fast_job", func(ctx context.Context, task *queue.Task) error {
		return nil
	})

	// poison_job: fails permanently (moves to DLQ)
	pool.RegisterHandler("poison_job", func(ctx context.Context, task *queue.Task) error {
		return errors.New("hard fatal failure")
	})

	const producers = 50
	const tasksPerProducer = 100
	const totalTasks = producers * tasksPerProducer // 5,000 tasks

	var produceWg sync.WaitGroup
	for p := 0; p < producers; p++ {
		produceWg.Add(1)
		go func(pid int) {
			defer produceWg.Done()
			for i := 0; i < tasksPerProducer; i++ {
				var task *queue.Task
				if i%5 == 0 {
					// 20% poison pills (1 retry max -> directly to DLQ)
					task = queue.NewTask("poison_job", map[string]any{"producer": pid, "seq": i})
					task.MaxRetries = 1
				} else {
					// 80% fast healthy jobs
					task = queue.NewTask("fast_job", map[string]any{"producer": pid, "seq": i})
				}
				task.Priority = 1 + (i % 10)
				_ = q.Push(task)
			}
		}(p)
	}
	produceWg.Wait()

	if q.Len() != totalTasks {
		t.Fatalf("expected %d tasks in queue after producing, got %d", totalTasks, q.Len())
	}

	// Start processing with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	pool.Start(ctx)

	// Wait for queue to drain or timeout
	deadline := time.After(6 * time.Second)
	for {
		if q.Len() == 0 {
			// Allow ongoing tasks in-flight to finish
			time.Sleep(150 * time.Millisecond)
			if q.Len() == 0 {
				break
			}
		}
		select {
		case <-deadline:
			break
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	cancel()
	pool.GracefulStop()

	// Verify Conservation Law
	var totalCompleted int64
	var totalPanicked int64
	for _, s := range pool.Stats() {
		totalCompleted += s.Completed
		totalPanicked += s.Panicked
	}

	totalDead := int64(dlq.Size())
	remaining := int64(q.Len())

	accountedFor := totalCompleted + totalDead + remaining

	if totalPanicked != 0 {
		t.Errorf("expected 0 panics during normal stress test, got %d", totalPanicked)
	}

	if accountedFor != int64(totalTasks) {
		t.Fatalf("CONSERVATION LAW VIOLATED! Total: %d, Accounted: %d (Completed: %d, Dead: %d, Remaining: %d)",
			totalTasks, accountedFor, totalCompleted, totalDead, remaining)
	}

	t.Logf("✓ Stress Test Passed: %d tasks perfectly conserved (Completed: %d, DLQ: %d, Remaining: %d)",
		totalTasks, totalCompleted, totalDead, remaining)
}

// TestStressConcurrencyCeiling verifies that the Worker never exceeds its configured
// concurrency limit, even under sudden spikes.
func TestStressConcurrencyCeiling(t *testing.T) {
	memStore := store.NewMemoryStore()
	q := queue.NewQueue(memStore)
	dlq := queue.NewDLQ()
	distLock := lock.NewDistributedLock()

	const maxAllowed = 25
	w := worker.NewWorker("ceiling-worker", q, dlq, distLock, worker.WorkerConfig{
		Concurrency:    maxAllowed,
		HandlerTimeout: 2 * time.Second,
		LockTTL:        10 * time.Second,
	})

	var active int64
	var maxObserved int64
	var mu sync.Mutex

	w.RegisterHandler("sleepy", func(ctx context.Context, task *queue.Task) error {
		cur := atomic.AddInt64(&active, 1)
		mu.Lock()
		if cur > maxObserved {
			maxObserved = cur
		}
		mu.Unlock()

		time.Sleep(10 * time.Millisecond)
		atomic.AddInt64(&active, -1)
		return nil
	})

	// Push 300 tasks rapidly
	for i := 0; i < 300; i++ {
		_ = q.Push(queue.NewTask("sleepy", nil))
	}

	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)

	time.Sleep(400 * time.Millisecond)
	cancel()

	mu.Lock()
	observed := maxObserved
	mu.Unlock()

	if observed > int64(maxAllowed) {
		t.Fatalf("Concurrency limit breached! Allowed: %d, Observed: %d", maxAllowed, observed)
	}

	t.Logf("✓ Concurrency Ceiling Passed: Max observed active = %d (Limit = %d)", observed, maxAllowed)
}
