package lock_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/doguhanniltextra/distributed-queue/lock"
)

func TestLockAtMostOnce(t *testing.T) {
	dl := lock.NewDistributedLock()

	var (
		winners int
		mu      sync.Mutex
		wg      sync.WaitGroup
	)

	const contenders = 20
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			holderID := fmt.Sprintf("worker-%d", id)
			if dl.Acquire("task-payment-101", holderID, 5*time.Second) {
				mu.Lock()
				winners++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	if winners != 1 {
		t.Fatalf("expected exactly 1 winner for lock, got %d", winners)
	}
}

func TestLockOwnershipAndRelease(t *testing.T) {
	dl := lock.NewDistributedLock()

	// Worker-1 acquires
	if !dl.Acquire("task-123", "worker-1", 5*time.Second) {
		t.Fatal("worker-1 should successfully acquire lock")
	}

	// Worker-2 cannot acquire
	if dl.Acquire("task-123", "worker-2", 5*time.Second) {
		t.Error("worker-2 should not be able to acquire while held by worker-1")
	}

	// Worker-2 cannot release worker-1's lock
	if dl.Release("task-123", "worker-2") {
		t.Error("non-owner worker-2 should not be able to release lock")
	}

	// Worker-1 releases
	if !dl.Release("task-123", "worker-1") {
		t.Error("owner worker-1 should successfully release lock")
	}

	// Now worker-2 can acquire
	if !dl.Acquire("task-123", "worker-2", 5*time.Second) {
		t.Error("worker-2 should acquire lock after release")
	}
}

func TestLockTTLExpiry(t *testing.T) {
	dl := lock.NewDistributedLock()

	// Worker-1 acquires with a short 50ms TTL
	if !dl.Acquire("resource-temp", "worker-1", 50*time.Millisecond) {
		t.Fatal("worker-1 failed to acquire")
	}

	if !dl.IsHeld("resource-temp") {
		t.Error("lock should be reported as held")
	}

	// Wait for TTL to expire
	time.Sleep(70 * time.Millisecond)

	if dl.IsHeld("resource-temp") {
		t.Error("lock should not be reported as held after TTL expired")
	}

	// Worker-2 can now acquire the expired lock lazily
	if !dl.Acquire("resource-temp", "worker-2", 5*time.Second) {
		t.Error("worker-2 should acquire after TTL expired")
	}
}
