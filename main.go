package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/doguhanniltextra/distributed-queue/handlers"
	"github.com/doguhanniltextra/distributed-queue/lock"
	"github.com/doguhanniltextra/distributed-queue/queue"
	"github.com/doguhanniltextra/distributed-queue/scheduler"
	"github.com/doguhanniltextra/distributed-queue/store"
	"github.com/doguhanniltextra/distributed-queue/worker"
)

func main() {
	fmt.Println("==================================================")
	fmt.Println("   DISTRIBUTED TASK QUEUE ENGINE STARTING        ")
	fmt.Println("==================================================")

	// ── 1. INFRASTRUCTURE ───────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	memStore := store.NewMemoryStore()
	q := queue.NewQueue(memStore)
	dlq := queue.NewDLQ()
	distLock := lock.NewDistributedLock()
	elector := scheduler.NewLeaderElector(10 * time.Second)

	// ── 2. WORKER POOL ──────────────────────────────────────────────
	pool := worker.NewPool(q, dlq, distLock)

	// Add 3 workers, each with 5 concurrent execution slots
	for i := 1; i <= 3; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		pool.AddWorker(workerID, worker.WorkerConfig{
			Concurrency:    5,
			HandlerTimeout: 5 * time.Second,
			LockTTL:        30 * time.Second,
		})
	}

	// Register all scenario handlers across all workers
	handlers.RegisterAll(pool)

	// ── 3. LEADER ELECTION ──────────────────────────────────────────
	// All workers campaign for leadership; leader schedules cron jobs
	for _, w := range pool.Workers() {
		if elector.Campaign(w.ID) {
			fmt.Printf("[LEADER] %s elected as leader\n", w.ID)
		}
		go elector.StartHeartbeating(ctx, w.ID, 3*time.Second)
	}

	// ── 4. SCHEDULER ────────────────────────────────────────────────
	firstWorkerID := pool.Workers()[0].ID
	sched := scheduler.NewScheduler(q, elector, firstWorkerID)
	sched.AddJob("generate_invoice", map[string]any{"type": "periodic", "source": "cron"}, 10*time.Second)
	go sched.Start(ctx)

	// ── 5. SEED DEMO TASKS ──────────────────────────────────────────
	seedDemoTasks(q)

	// ── 6. START WORKER POOL ────────────────────────────────────────
	pool.Start(ctx)

	// ── 7. GRACEFUL SHUTDOWN HANDLING ───────────────────────────────
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	fmt.Println("\n[SHUTDOWN] SIGTERM / Interrupt received. Initiating graceful shutdown...")

	cancel()            // Signal all workers and schedulers to stop accepting new work
	pool.GracefulStop() // Wait for running tasks to complete cleanly

	// ── 8. SHUTDOWN REPORT ──────────────────────────────────────────
	printShutdownReport(pool, dlq, q)
}

// seedDemoTasks populates the queue with diverse tasks to demonstrate priority, delay,
// retry, timeout, panic recovery, and dead-letter quarantine behaviors.
func seedDemoTasks(q *queue.Queue) {
	fmt.Println("\n--- Enqueuing Demo Tasks ---")

	tasks := []*queue.Task{
		queue.NewTask("send_email", map[string]any{
			"to":      "alice@example.com",
			"subject": "Welcome to Distributed Task Queue!",
		}),
		queue.NewTask("generate_invoice", map[string]any{
			"order_id": "ORD-2026-9901",
			"amount":   "1250.00",
		}),
		queue.NewTask("heavy_report", map[string]any{
			"report_type": "annual_financial_audit",
		}),
		queue.NewTask("poison_pill", map[string]any{
			"reason": "external API permanent outage",
		}),
		queue.NewTask("panic_job", map[string]any{
			"trigger": "nil_pointer_bug",
		}),
	}

	// Priority demonstration: Invoice has critical priority (10), email has normal priority (5)
	tasks[0].Priority = 5
	tasks[1].Priority = 10

	// Delay demonstration: heavy_report scheduled 3 seconds in the future
	tasks[2].ExecuteAt = time.Now().Add(3 * time.Second)

	for _, t := range tasks {
		_ = q.Push(t)
		delayMsg := ""
		if t.ExecuteAt.After(time.Now()) {
			delayMsg = fmt.Sprintf(", delay: %v", time.Until(t.ExecuteAt).Round(time.Second))
		}
		fmt.Printf("[QUEUE] enqueued: %s (priority: %d%s)\n", t.Name, t.Priority, delayMsg)
	}
	fmt.Println("----------------------------")
}

// printShutdownReport summarizes execution metrics and health status upon shutdown.
func printShutdownReport(pool *worker.WorkerPool, dlq *queue.DLQ, q *queue.Queue) {
	stats := pool.Stats()
	var totalCompleted, totalFailed, totalPanicked int64
	for _, s := range stats {
		totalCompleted += s.Completed
		totalFailed += s.Failed
		totalPanicked += s.Panicked
	}

	fmt.Println("\n══════════════════════════════════════")
	fmt.Println("         SHUTDOWN REPORT              ")
	fmt.Println("══════════════════════════════════════")
	fmt.Printf("Total Completed : %d\n", totalCompleted)
	fmt.Printf("Total Failed    : %d\n", totalFailed)
	fmt.Printf("Total Panicked  : %d\n", totalPanicked)
	fmt.Printf("DLQ Size        : %d\n", dlq.Size())
	fmt.Printf("Queue Remaining : %d\n", q.Len())
	fmt.Println()
	fmt.Println("Per-Worker Stats:")
	for _, s := range stats {
		fmt.Printf("  %s: ✓ %d done, ✗ %d failed, 🔥 %d panic\n",
			s.ID, s.Completed, s.Failed, s.Panicked)
	}
	fmt.Println("══════════════════════════════════════")
}
