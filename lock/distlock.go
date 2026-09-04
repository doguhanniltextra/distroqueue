package lock

import (
	"sync"
	"time"
)

// lockEntry tracks which worker owns the lock and when it expires.
type lockEntry struct {
	holderID  string
	expiresAt time.Time
}

// DistributedLock provides mutual exclusion across workers with a Time-To-Live (TTL).
// It simulates the core semantics of Redis Redlock / etcd leases in memory.
type DistributedLock struct {
	mu      sync.Mutex
	holders map[string]lockEntry // resource -> lockEntry
}

// NewDistributedLock initializes and returns a new DistributedLock manager.
func NewDistributedLock() *DistributedLock {
	return &DistributedLock{
		holders: make(map[string]lockEntry),
	}
}

// Acquire attempts to claim exclusive ownership of a resource for holderID for duration ttl.
// If the lock is held by another worker and has not expired, it returns false.
// If the existing lock has expired (TTL passed), it lazily overwrites it and grants the lock.
func (l *DistributedLock) Acquire(resource, holderID string, ttl time.Duration) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	if entry, exists := l.holders[resource]; exists {
		// If still within valid TTL and held by someone else, cannot acquire
		if now.Before(entry.expiresAt) {
			return false
		}
		// If TTL has passed, the previous lock has expired (stale entry)
	}

	l.holders[resource] = lockEntry{
		holderID:  holderID,
		expiresAt: now.Add(ttl),
	}
	return true
}

// Release frees the lock on the specified resource.
// Only the current owner (matching holderID) can successfully release the lock.
func (l *DistributedLock) Release(resource, holderID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry, exists := l.holders[resource]
	if !exists {
		return false
	}

	if entry.holderID != holderID {
		// Non-owner cannot release another worker's lock
		return false
	}

	delete(l.holders, resource)
	return true
}

// IsHeld checks whether a resource is currently locked under a valid, unexpired lease.
func (l *DistributedLock) IsHeld(resource string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry, exists := l.holders[resource]
	if !exists {
		return false
	}

	// An expired lock is treated as not held
	if time.Now().After(entry.expiresAt) {
		return false
	}

	return true
}
