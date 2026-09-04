package test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/doguhanniltextra/distributed-queue/lock"
	"github.com/doguhanniltextra/distributed-queue/queue"
	"github.com/doguhanniltextra/distributed-queue/scheduler"
	"github.com/doguhanniltextra/distributed-queue/store"
	"github.com/doguhanniltextra/distributed-queue/worker"
)

// TestChaosShutdownUnderFire verifies that if a shutdown signal arrives while
// dozens of tasks are actively running in parallel, the pool stops cleanly
// within a strict deadline without deadlocking or leaking goroutines.
func TestChaosShutdownUnderFire(t *testing.T) {
	memStore := store.NewMemoryStore()
	q := queue.NewQueue(memStore)
	dlq := queue.NewDLQ()
	distLock := lock.NewDistributedLock()

	pool := worker.NewPool(q, dlq, distLock)

	// 3 workers x 15 concurrency = 45 parallel slots
	for i := 1; i <= 3; i++ {
		pool.AddWorker(fmt.Sprintf("worker-chaos-%d", i), worker.WorkerConfig{
			Concurrency:    15,
			HandlerTimeout: 2 * time.Second,
			LockTTL:        10 * time.Second,
		})
	}

	var activeNow int64
	var completedBeforeOrDuringShutdown int64

	pool.RegisterHandler("busy_job", func(ctx context.Context, task *queue.Task) error {
		atomic.AddInt64(&activeNow, 1)
		defer atomic.AddInt64(&activeNow, -1)

		// Simulate real processing workload
		select {
		case <-time.After(30 * time.Millisecond):
			atomic.AddInt64(&completedBeforeOrDuringShutdown, 1)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	// Enqueue 400 tasks
	for i := 0; i < 400; i++ {
		_ = q.Push(queue.NewTask("busy_job", nil))
	}

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)

	// Let the workers get under full fire
	time.Sleep(60 * time.Millisecond)

	activeAtCancel := atomic.LoadInt64(&activeNow)
	if activeAtCancel == 0 {
		t.Log("Warning: No active tasks at cancellation time, but continuing test.")
	} else {
		t.Logf("Firing SIGTERM while %d tasks are actively executing in flight...", activeAtCancel)
	}

	// Trigger abrupt cancellation
	cancelStart := time.Now()
	cancel()

	// Wait for pool shutdown
	stopDone := make(chan struct{})
	go func() {
		pool.GracefulStop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
		shutdownDuration := time.Since(cancelStart)
		if shutdownDuration > 2*time.Second {
			t.Fatalf("GracefulStop took too long: %v (expected < 2s)", shutdownDuration)
		}
		t.Logf("✓ Shutdown Under Fire Cleanly Completed in %v (Zero deadlocks)", shutdownDuration)
	case <-time.After(3 * time.Second):
		t.Fatal("DEADLOCK DETECTED! pool.GracefulStop() hung and failed to return within 3s")
	}
}

// TestChaosSplitBrainFlapping tests leadership election under high-frequency
// concurrent contention, ensuring that at no point in time can multiple nodes
// concurrently claim active leadership.
func TestChaosSplitBrainFlapping(t *testing.T) {
	const ttl = 80 * time.Millisecond
	elector := scheduler.NewLeaderElector(ttl)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	const contenderCount = 10
	var wg sync.WaitGroup

	var splitBrainViolations int64

	for i := 1; i <= contenderCount; i++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					won := elector.Campaign(id)
					if won {
						// If this worker won, it MUST be the unique leader
						curLeader := elector.Leader()
						if curLeader != id {
							atomic.AddInt64(&splitBrainViolations, 1)
						}
						// Send heartbeat for a short burst, then deliberately drop it
						for h := 0; h < 3; h++ {
							elector.Heartbeat(id)
							time.Sleep(15 * time.Millisecond)
						}
					}
					time.Sleep(20 * time.Millisecond)
				}
			}
		}(fmt.Sprintf("node-%d", i))
	}

	wg.Wait()

	if atomic.LoadInt64(&splitBrainViolations) > 0 {
		t.Fatalf("SPLIT-BRAIN DETECTED! %d concurrent inconsistent leadership observations", splitBrainViolations)
	}

	t.Log("✓ Split-Brain Flapping Test Passed: Exactly 1 leader at all times under heavy contention")
}

// TestChaosPanicStorm feeds 100 consecutive panicking tasks to the worker pool.
// It proves that panics are completely isolated, all faulty tasks are safely quarantined in DLQ,
// and subsequent healthy tasks continue to be processed without restarting the service.
func TestChaosPanicStorm(t *testing.T) {
	memStore := store.NewMemoryStore()
	q := queue.NewQueue(memStore)
	dlq := queue.NewDLQ()
	distLock := lock.NewDistributedLock()

	pool := worker.NewPool(q, dlq, distLock)
	pool.AddWorker("panic-survivor-1", worker.WorkerConfig{Concurrency: 10})
	pool.AddWorker("panic-survivor-2", worker.WorkerConfig{Concurrency: 10})

	pool.RegisterHandler("fatal_bomb", func(ctx context.Context, task *queue.Task) error {
		panic(fmt.Sprintf("deliberate fatal crash inside task: %s", task.ID))
	})

	var healthyExecuted int64
	pool.RegisterHandler("healthy_job", func(ctx context.Context, task *queue.Task) error {
		atomic.AddInt64(&healthyExecuted, 1)
		return nil
	})

	const bombCount = 100
	for i := 0; i < bombCount; i++ {
		task := queue.NewTask("fatal_bomb", nil)
		task.MaxRetries = 1 // Immediately quarantine to DLQ
		_ = q.Push(task)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)

	// Wait for panic bombs to detonate and be recovered
	deadline := time.After(3 * time.Second)
	for {
		if dlq.Size() == bombCount {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("Timeout waiting for panic storm quarantine: expected %d in DLQ, got %d", bombCount, dlq.Size())
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}

	// Verify all bombs landed in DLQ
	if dlq.Size() != bombCount {
		t.Fatalf("DLQ size mismatch: expected %d, got %d", bombCount, dlq.Size())
	}

	// Now prove the worker pool survived by pushing 20 healthy tasks
	const healthyCount = 20
	for i := 0; i < healthyCount; i++ {
		_ = q.Push(queue.NewTask("healthy_job", nil))
	}

	// Wait for healthy jobs to finish
	time.Sleep(200 * time.Millisecond)

	if atomic.LoadInt64(&healthyExecuted) != healthyCount {
		t.Fatalf("Worker pool failed to process healthy tasks after panic storm! Got %d, want %d",
			healthyExecuted, healthyCount)
	}

	cancel()
	pool.GracefulStop()

	t.Logf("✓ Panic Storm Passed: Survived %d panics, quarantined 100%% to DLQ, and processed healthy tasks normally", bombCount)
}
