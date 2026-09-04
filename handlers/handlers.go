package handlers

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/doguhanniltextra/distributed-queue/queue"
	"github.com/doguhanniltextra/distributed-queue/worker"
)

// SendEmailHandler simulates a normal, healthy I/O bound job (Happy path).
// Sleeps 50-150ms to simulate network latency and returns nil.
func SendEmailHandler(ctx context.Context, task *queue.Task) error {
	to := "user@example.com"
	if task.Payload != nil {
		if val, ok := task.Payload["to"].(string); ok {
			to = val
		}
	}

	delay := time.Duration(50+rand.Intn(100)) * time.Millisecond

	select {
	case <-time.After(delay):
		fmt.Printf("  📧 Email sent to %s\n", to)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GenerateInvoiceHandler simulates transient third-party API / payment gateway failures.
// Fails ~30% of the time with a timeout error to test Worker retry & exponential backoff.
func GenerateInvoiceHandler(ctx context.Context, task *queue.Task) error {
	// Simulate 30% failure rate
	if rand.Float32() < 0.30 {
		return errors.New("payment gateway timeout")
	}

	delay := time.Duration(100+rand.Intn(200)) * time.Millisecond

	select {
	case <-time.After(delay):
		fmt.Printf("  💳 Invoice generated successfully for task: %s\n", task.ID)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// HeavyReportHandler simulates a long-running heavy report query taking 8 seconds.
// It tests whether the Worker's context timeout (default 5s) correctly aborts long-running tasks.
func HeavyReportHandler(ctx context.Context, task *queue.Task) error {
	select {
	case <-time.After(8 * time.Second):
		fmt.Printf("  📊 Heavy report completed for task: %s\n", task.ID)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PoisonPillHandler simulates an unrecoverable bug or dead third-party service.
// Always returns an error to verify DLQ quarantine upon reaching MaxRetries.
func PoisonPillHandler(ctx context.Context, task *queue.Task) error {
	return errors.New("external API is down")
}

// PanicHandler simulates an unexpected runtime panic (e.g. nil pointer dereference).
// Tests whether the Worker's defer recover() mechanism catches it without crashing the process.
func PanicHandler(ctx context.Context, task *queue.Task) error {
	panic("unexpected nil pointer dereference")
}

// RegisterAll registers all demo handlers across each Worker within the WorkerPool.
func RegisterAll(pool *worker.WorkerPool) {
	for _, w := range pool.Workers() {
		w.RegisterHandler("send_email", SendEmailHandler)
		w.RegisterHandler("generate_invoice", GenerateInvoiceHandler)
		w.RegisterHandler("heavy_report", HeavyReportHandler)
		w.RegisterHandler("poison_pill", PoisonPillHandler)
		w.RegisterHandler("panic_job", PanicHandler)
	}
}
