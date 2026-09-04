package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/doguhanniltextra/distributed-queue/lock"
	"github.com/doguhanniltextra/distributed-queue/queue"
)

// HandlerFunc defines the signature of a task processing function.
type HandlerFunc func(ctx context.Context, task *queue.Task) error

// WorkerConfig specifies configuration parameters for a Worker instance.
type WorkerConfig struct {
	Concurrency    int           // Maximum concurrent tasks (default: 5)
	HandlerTimeout time.Duration // Maximum execution time per task (default: 30s)
	LockTTL        time.Duration // Distributed lock duration (default: 60s)
}

// WorkerStats holds execution counters for a Worker instance.
type WorkerStats struct {
	ID        string `json:"id"`
	Completed int64  `json:"completed"`
	Failed    int64  `json:"failed"`
	Panicked  int64  `json:"panicked"`
}

// Worker executes tasks popped from the Queue while honoring concurrency limits,
// distributed locking, timeouts, panic recovery, and exponential backoff retry semantics.
type Worker struct {
	ID             string
	concurrency    int
	handlerTimeout time.Duration
	lockTTL        time.Duration
	queue          *queue.Queue
	dlq            *queue.DLQ
	lock           *lock.DistributedLock
	handlers       map[string]HandlerFunc
	sem            chan struct{}  // Concurrency limiter semaphore
	wg             sync.WaitGroup // Tracks running process() goroutines

	// Atomic counters
	completed int64
	failed    int64
	panicked  int64
}

// NewWorker initializes and returns a new Worker instance.
func NewWorker(id string, q *queue.Queue, dlq *queue.DLQ, l *lock.DistributedLock, cfg WorkerConfig) *Worker {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 5
	}
	if cfg.HandlerTimeout <= 0 {
		cfg.HandlerTimeout = 30 * time.Second
	}
	if cfg.LockTTL <= 0 {
		cfg.LockTTL = 60 * time.Second
	}

	return &Worker{
		ID:             id,
		concurrency:    cfg.Concurrency,
		handlerTimeout: cfg.HandlerTimeout,
		lockTTL:        cfg.LockTTL,
		queue:          q,
		dlq:            dlq,
		lock:           l,
		handlers:       make(map[string]HandlerFunc),
		sem:            make(chan struct{}, cfg.Concurrency),
	}
}

// RegisterHandler binds a handler function to a task name.
func (w *Worker) RegisterHandler(name string, fn HandlerFunc) {
	w.handlers[name] = fn
}

// Run is the main polling loop of the worker.
// It continuously pops tasks from the queue and spawns processing goroutines.
// Upon context cancellation, it ceases accepting new tasks and waits for active tasks to complete.
func (w *Worker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// Shutdown requested: wait for all in-flight tasks to finish and exit
			w.wg.Wait()
			return
		default:
		}

		task, ok := w.queue.Pop()
		if !ok {
			time.Sleep(100 * time.Millisecond) // Avoid busy-waiting CPU burn
			continue
		}

		// At-most-once execution: attempt to acquire distributed lock
		if !w.lock.Acquire(task.ID, w.ID, w.lockTTL) {
			// Lock not acquired (another worker is processing this task); requeue and continue
			_ = w.queue.Push(task)
			continue
		}

		// Wait for an available concurrency slot.
		// Kept outside of the select-default to ensure we do not bypass concurrency limits.
		w.sem <- struct{}{}

		w.wg.Add(1)
		go w.process(ctx, task)
	}
}

// process executes a single task with panic safety, timeout, and lock management.
func (w *Worker) process(ctx context.Context, task *queue.Task) {
	// LIFO defer ordering:
	// 4. (Last executed): decrement WaitGroup
	defer w.wg.Done()

	// 3. (Third executed): release semaphore concurrency slot
	defer func() { <-w.sem }()

	// 2. (Second executed): release distributed lock
	defer w.lock.Release(task.ID, w.ID)

	// 1. (First executed): recover from panics, mark failure, handle retry/DLQ
	defer func() {
		if r := recover(); r != nil {
			task.Error = fmt.Sprintf("panic: %v", r)
			atomic.AddInt64(&w.panicked, 1)
			w.handleFailure(task)
		}
	}()

	handlerCtx, cancel := context.WithTimeout(ctx, w.handlerTimeout)
	defer cancel()

	handler, exists := w.handlers[task.Name]
	if !exists {
		task.Error = fmt.Sprintf("no handler registered for: %s", task.Name)
		w.handleFailure(task)
		return
	}

	task.Status = queue.Running
	err := handler(handlerCtx, task)

	if err != nil {
		task.Error = err.Error()
		w.handleFailure(task)
		return
	}

	task.Status = queue.Done
	atomic.AddInt64(&w.completed, 1)
	fmt.Printf("[%s] ✓ done: %s\n", w.ID, task.Name)
}

// handleFailure applies retry logic with exponential backoff, or moves poisoned tasks to DLQ.
func (w *Worker) handleFailure(task *queue.Task) {
	task.Retries++
	atomic.AddInt64(&w.failed, 1)

	maxRetries := task.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}

	if task.Retries >= maxRetries {
		task.Status = queue.Dead
		fmt.Printf("[%s] 💀 dead: %s (after %d retries, error: %s)\n", w.ID, task.Name, task.Retries, task.Error)
		w.dlq.Push(task)
		return
	}

	// Exponential backoff: 2^retries seconds
	backoff := time.Duration(1<<task.Retries) * time.Second

	task.Status = queue.Pending
	task.ExecuteAt = time.Now().Add(backoff)
	task.Error = ""

	fmt.Printf("[%s] ↩ retry #%d/%d in %v: %s\n",
		w.ID, task.Retries, maxRetries, backoff, task.Name)

	_ = w.queue.Push(task)
}

// Stats returns a point-in-time snapshot of the worker execution statistics.
func (w *Worker) Stats() WorkerStats {
	return WorkerStats{
		ID:        w.ID,
		Completed: atomic.LoadInt64(&w.completed),
		Failed:    atomic.LoadInt64(&w.failed),
		Panicked:  atomic.LoadInt64(&w.panicked),
	}
}
