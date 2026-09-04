package store_test

import (
	"sync"
	"testing"
	"time"

	"github.com/doguhanniltextra/distributed-queue/queue"
	"github.com/doguhanniltextra/distributed-queue/store"
)

func TestMemoryStoreConcurrentPush(t *testing.T) {
	t.Parallel() // race detector'ı aktif eder

	ms := store.NewMemoryStore()

	const goroutines = 50
	const tasksEach = 20
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < tasksEach; j++ {
				_ = ms.Push(queue.NewTask("test", nil))
			}
		}()
	}
	wg.Wait()

	got := ms.Len()
	want := goroutines * tasksEach
	if got != want {
		t.Errorf("Len() = %d, want %d", got, want)
	}
}

func TestMemoryStorePriorityOrder(t *testing.T) {
	ms := store.NewMemoryStore()

	low := queue.NewTask("low", nil)
	low.Priority = 1
	high := queue.NewTask("high", nil)
	high.Priority = 10

	_ = ms.Push(low)
	_ = ms.Push(high)

	got, ok := ms.Pop()
	if !ok {
		t.Fatal("Pop() returned no task")
	}
	if got.Name != "high" {
		t.Errorf("expected high priority first, got %q", got.Name)
	}
}

func TestMemoryStoreDelayedTask(t *testing.T) {
	ms := store.NewMemoryStore()

	future := queue.NewTask("future", nil)
	future.ExecuteAt = time.Now().Add(10 * time.Second) // 10sn sonra hazır

	_ = ms.Push(future)

	got, ok := ms.Pop()
	if ok {
		t.Errorf("delayed task should not be returned yet, got %q", got.Name)
	}
}

func TestMemoryStoreDrain(t *testing.T) {
	ms := store.NewMemoryStore()
	_ = ms.Push(queue.NewTask("a", nil))
	_ = ms.Push(queue.NewTask("b", nil))

	drained := ms.Drain()
	if len(drained) != 2 {
		t.Errorf("Drain() returned %d tasks, want 2", len(drained))
	}
	if ms.Len() != 0 {
		t.Error("Len() should be 0 after Drain()")
	}
}
