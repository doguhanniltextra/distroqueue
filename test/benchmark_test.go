package test

import (
	"context"
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

// BenchmarkInMemoryDistLockSim measures the raw in-memory synchronization speed of Acquire and Release cycles
// without network overhead (0 allocs, 0 B/op).
func BenchmarkInMemoryDistLockSim(b *testing.B) {
	dl := lock.NewDistributedLock()
	const resourceID = "benchmark-resource-key"
	const holderID = "benchmark-worker-id"
	const ttl = 5 * time.Second

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if dl.Acquire(resourceID, holderID, ttl) {
			dl.Release(resourceID, holderID)
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
