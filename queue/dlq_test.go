package queue_test

import (
	"sync"
	"testing"
	"time"

	"github.com/doguhanniltextra/distributed-queue/queue"
	"github.com/doguhanniltextra/distributed-queue/store"
)

func TestDLQOperations(t *testing.T) {
	dlq := queue.NewDLQ()

	task1 := queue.NewTask("flaky_job_1", map[string]any{"attempt": 3})
	task1.Retries = 3
	task1.Error = "connection refused"

	task2 := queue.NewTask("flaky_job_2", map[string]any{"attempt": 3})
	task2.Retries = 3
	task2.Error = "timeout"

	dlq.Push(task1)
	dlq.Push(task2)

	if dlq.Size() != 2 {
		t.Fatalf("expected DLQ size 2, got %d", dlq.Size())
	}

	if task1.Status != queue.Dead || task2.Status != queue.Dead {
		t.Errorf("expected both tasks to have status Dead, got %s and %s", task1.Status, task2.Status)
	}

	// Test All() returns a copy
	allTasks := dlq.All()
	if len(allTasks) != 2 {
		t.Fatalf("expected 2 tasks from All(), got %d", len(allTasks))
	}
	allTasks[0] = nil
	if dlq.All()[0] == nil {
		t.Error("modifying slice returned from All() must not alter internal DLQ storage")
	}

	// Test Replay
	ms := store.NewMemoryStore()
	q := queue.NewQueue(ms)

	replayedCount := dlq.Replay(q)
	if replayedCount != 2 {
		t.Errorf("Replay() returned %d, want 2", replayedCount)
	}
	if dlq.Size() != 0 {
		t.Errorf("DLQ size after Replay() should be 0, got %d", dlq.Size())
	}
	if q.Len() != 2 {
		t.Errorf("Queue len after Replay() should be 2, got %d", q.Len())
	}

	// Verify reset properties of replayed task
	replayedTask, ok := q.Pop()
	if !ok || replayedTask == nil {
		t.Fatal("expected task from replayed queue")
	}
	if replayedTask.Retries != 0 {
		t.Errorf("replayed task Retries = %d, want 0", replayedTask.Retries)
	}
	if replayedTask.Status != queue.Pending {
		t.Errorf("replayed task Status = %s, want PENDING", replayedTask.Status)
	}
	if replayedTask.Error != "" {
		t.Errorf("replayed task Error = %q, want empty", replayedTask.Error)
	}
	if replayedTask.ExecuteAt.After(time.Now()) {
		t.Error("replayed task ExecuteAt should not be in the future")
	}
}

func TestDLQClear(t *testing.T) {
	dlq := queue.NewDLQ()
	dlq.Push(queue.NewTask("job1", nil))
	dlq.Push(queue.NewTask("job2", nil))

	if dlq.Size() != 2 {
		t.Fatalf("expected DLQ size 2, got %d", dlq.Size())
	}

	dlq.Clear()
	if dlq.Size() != 0 {
		t.Errorf("expected DLQ size 0 after Clear(), got %d", dlq.Size())
	}
	if len(dlq.All()) != 0 {
		t.Errorf("expected All() to return empty slice after Clear()")
	}
}

func TestDLQConcurrentPush(t *testing.T) {
	dlq := queue.NewDLQ()
	const goroutines = 30
	const tasksEach = 15

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < tasksEach; j++ {
				dlq.Push(queue.NewTask("dead_job", nil))
			}
		}()
	}
	wg.Wait()

	if dlq.Size() != goroutines*tasksEach {
		t.Errorf("DLQ size = %d, want %d", dlq.Size(), goroutines*tasksEach)
	}
}
