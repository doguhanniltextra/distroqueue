package store

import (
	"sort"
	"sync"

	"github.com/doguhanniltextra/distributed-queue/queue"
)

// Compile-time check to verify that MemoryStore implements queue.QueueStore.
var _ queue.QueueStore = (*MemoryStore)(nil)

// MemoryStore is an in-memory, thread-safe implementation of queue.QueueStore.
type MemoryStore struct {
	mu    sync.Mutex
	tasks []*queue.Task
}

// NewMemoryStore initializes and returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		tasks: make([]*queue.Task, 0),
	}
}

// Push adds a task into the in-memory queue and maintains priority ordering (descending).
func (m *MemoryStore) Push(task *queue.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.tasks = append(m.tasks, task)

	// Sort so higher priority tasks come first.
	// SliceStable preserves original FIFO order among tasks with equal priority.
	sort.SliceStable(m.tasks, func(i, j int) bool {
		return m.tasks[i].Priority > m.tasks[j].Priority
	})
	return nil
}

// Pop extracts the first ready task (highest priority first).
// Returns nil, false if no tasks are ready.
func (m *MemoryStore) Pop() (*queue.Task, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, t := range m.tasks {
		if t.IsReady() {
			m.tasks = append(m.tasks[:i], m.tasks[i+1:]...)
			return t, true
		}
	}
	return nil, false
}

// Len returns the total count of stored tasks.
func (m *MemoryStore) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.tasks)
}

// Drain removes and returns a snapshot copy of all tasks in the queue.
func (m *MemoryStore) Drain() []*queue.Task {
	m.mu.Lock()
	defer m.mu.Unlock()

	drained := make([]*queue.Task, len(m.tasks))
	copy(drained, m.tasks)
	m.tasks = make([]*queue.Task, 0)
	return drained
}
