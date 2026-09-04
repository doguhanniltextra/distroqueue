package handlers_test

import (
	"context"
	"testing"
	"time"

	"github.com/doguhanniltextra/distributed-queue/handlers"
	"github.com/doguhanniltextra/distributed-queue/lock"
	"github.com/doguhanniltextra/distributed-queue/queue"
	"github.com/doguhanniltextra/distributed-queue/store"
	"github.com/doguhanniltextra/distributed-queue/worker"
)

func TestSendEmailHandlerSuccess(t *testing.T) {
	task := queue.NewTask("send_email", map[string]any{"to": "alice@example.com"})
	ctx := context.Background()

	err := handlers.SendEmailHandler(ctx, task)
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestSendEmailHandlerContextCancel(t *testing.T) {
	task := queue.NewTask("send_email", nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := handlers.SendEmailHandler(ctx, task)
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestHeavyReportHandlerTimeout(t *testing.T) {
	task := queue.NewTask("heavy_report", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := handlers.HeavyReportHandler(ctx, task)
	if err != context.DeadlineExceeded {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestPoisonPillHandler(t *testing.T) {
	task := queue.NewTask("poison_pill", nil)
	err := handlers.PoisonPillHandler(context.Background(), task)
	if err == nil || err.Error() != "external API is down" {
		t.Errorf("expected 'external API is down' error, got %v", err)
	}
}

func TestPanicHandler(t *testing.T) {
	task := queue.NewTask("panic_job", nil)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic from PanicHandler, but none occurred")
		}
		if r != "unexpected nil pointer dereference" {
			t.Errorf("unexpected panic value: %v", r)
		}
	}()

	_ = handlers.PanicHandler(context.Background(), task)
}

func TestRegisterAll(t *testing.T) {
	q := queue.NewQueue(store.NewMemoryStore())
	dlq := queue.NewDLQ()
	l := lock.NewDistributedLock()

	pool := worker.NewPool(q, dlq, l)
	pool.AddWorker("worker-test-1", worker.WorkerConfig{})
	pool.AddWorker("worker-test-2", worker.WorkerConfig{})

	handlers.RegisterAll(pool)

	workers := pool.Workers()
	if len(workers) != 2 {
		t.Fatalf("expected 2 workers in pool, got %d", len(workers))
	}
}
