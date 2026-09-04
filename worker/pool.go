package worker

import (
	"context"
	"fmt"
	"sync"

	"github.com/doguhanniltextra/distributed-queue/lock"
	"github.com/doguhanniltextra/distributed-queue/queue"
)

// WorkerPool manages the lifecycle of multiple Worker instances.
// It coordinates their execution and provides graceful shutdown capabilities.
type WorkerPool struct {
	mu      sync.Mutex
	workers []*Worker
	wg      sync.WaitGroup
	queue   *queue.Queue
	dlq     *queue.DLQ
	lock    *lock.DistributedLock
}

// NewPool initializes and returns a new WorkerPool.
func NewPool(q *queue.Queue, dlq *queue.DLQ, l *lock.DistributedLock) *WorkerPool {
	return &WorkerPool{
		queue:   q,
		dlq:     dlq,
		lock:    l,
		workers: make([]*Worker, 0),
	}
}

// AddWorker instantiates a new Worker, adds it to the pool, and returns it.
func (p *WorkerPool) AddWorker(id string, cfg WorkerConfig) *Worker {
	p.mu.Lock()
	defer p.mu.Unlock()

	w := NewWorker(id, p.queue, p.dlq, p.lock, cfg)
	p.workers = append(p.workers, w)
	return w
}

// Workers returns a snapshot slice of all workers currently in the pool.
func (p *WorkerPool) Workers() []*Worker {
	p.mu.Lock()
	defer p.mu.Unlock()

	workers := make([]*Worker, len(p.workers))
	copy(workers, p.workers)
	return workers
}

// RegisterHandler registers a task handler function across all workers currently in the pool.
func (p *WorkerPool) RegisterHandler(name string, fn HandlerFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, w := range p.workers {
		w.RegisterHandler(name, fn)
	}
}

// Start launches all workers in their own goroutines under the provided context.
func (p *WorkerPool) Start(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, w := range p.workers {
		p.wg.Add(1)
		go func(worker *Worker) {
			defer p.wg.Done()
			worker.Run(ctx)
		}(w)
	}
}

// GracefulStop waits for all worker Run loops to complete after context cancellation.
func (p *WorkerPool) GracefulStop() {
	p.wg.Wait()
	fmt.Println("[WorkerPool] All workers stopped cleanly.")
}

// Stats gathers and returns the current statistics of each worker in the pool.
func (p *WorkerPool) Stats() []WorkerStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	stats := make([]WorkerStats, 0, len(p.workers))
	for _, w := range p.workers {
		stats = append(stats, w.Stats())
	}
	return stats
}
