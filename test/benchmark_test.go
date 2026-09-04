package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/doguhanniltextra/distributed-queue/lock"
	"github.com/doguhanniltextra/distributed-queue/queue"
	"github.com/doguhanniltextra/distributed-queue/store"
	"github.com/doguhanniltextra/distributed-queue/worker"
)

// BenchmarkQueuePushPop measures the raw throughput of pushing and popping tasks.
func BenchmarkQueuePushPop(b *testing.B) {
	memStore := store.NewMemoryStore()
	q := queue.NewQueue(memStore)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		task := queue.NewTask("bench_job", nil)
		_ = q.Push(task)
		_, _ = q.Pop()
	}
}

// BenchmarkDistributedLock measures the speed of Acquire and Release cycles.
func BenchmarkDistributedLock(b *testing.B) {
	dl := lock.NewDistributedLock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resourceID := fmt.Sprintf("res-%d", i%100)
		if dl.Acquire(resourceID, "bench-worker", 5*time.Second) {
			dl.Release(resourceID, "bench-worker")
		}
	}
}

// BenchmarkWorkerPoolThroughput measures the end-to-end capacity of the worker pool
// processing lightweight tasks.
func BenchmarkWorkerPoolThroughput(b *testing.B) {
	memStore := store.NewMemoryStore()
	q := queue.NewQueue(memStore)
	dlq := queue.NewDLQ()
	distLock := lock.NewDistributedLock()

	pool := worker.NewPool(q, dlq, distLock)
	pool.AddWorker("bench-w1", worker.WorkerConfig{Concurrency: 10})
	pool.AddWorker("bench-w2", worker.WorkerConfig{Concurrency: 10})

	pool.RegisterHandler("noop", func(ctx context.Context, task *queue.Task) error {
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Push(queue.NewTask("noop", nil))
	}

	for q.Len() > 0 {
		time.Sleep(1 * time.Millisecond)
	}
	b.StopTimer()

	cancel()
	pool.GracefulStop()
}
