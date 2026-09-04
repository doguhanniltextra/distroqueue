package queue_test

import (
	"sync"
	"testing"
	"time"

	"github.com/doguhanniltextra/distributed-queue/queue"
	"github.com/doguhanniltextra/distributed-queue/store"
)

func TestQueueBasicOperations(t *testing.T) {
	ms := store.NewMemoryStore()
	q := queue.NewQueue(ms)

	t1 := queue.NewTask("email", nil)
	t2 := queue.NewTask("invoice", nil)

	_ = q.Push(t1)
	_ = q.Push(t2)

	if q.Len() != 2 {
		t.Fatalf("expected queue length 2, got %d", q.Len())
	}

	stats := q.Stats()
	if stats.TotalPushed != 2 {
		t.Errorf("TotalPushed = %d, want 2", stats.TotalPushed)
	}
	if stats.TotalPopped != 0 {
		t.Errorf("TotalPopped = %d, want 0", stats.TotalPopped)
	}

	popped, ok := q.Pop()
	if !ok || popped == nil {
		t.Fatal("expected task from Pop, got none")
	}

	stats = q.Stats()
	if stats.TotalPopped != 1 {
		t.Errorf("TotalPopped = %d, want 1", stats.TotalPopped)
	}
	if stats.CurrentLen != 1 {
		t.Errorf("CurrentLen = %d, want 1", stats.CurrentLen)
	}
}

func TestQueueConcurrentPushPop(t *testing.T) {
	ms := store.NewMemoryStore()
	q := queue.NewQueue(ms)

	const producers = 20
	const consumers = 20
	const tasksPerProducer = 50

	var wg sync.WaitGroup

	// Start producers
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < tasksPerProducer; j++ {
				_ = q.Push(queue.NewTask("work", nil))
			}
		}()
	}

	// Start consumers
	for i := 0; i < consumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			timeout := time.After(2 * time.Second)
			for {
				select {
				case <-timeout:
					return
				default:
					if _, ok := q.Pop(); !ok {
						time.Sleep(2 * time.Millisecond)
					}
				}
			}
		}()
	}

	wg.Wait()

	stats := q.Stats()
	if stats.TotalPushed != producers*tasksPerProducer {
		t.Errorf("TotalPushed = %d, want %d", stats.TotalPushed, producers*tasksPerProducer)
	}
}
