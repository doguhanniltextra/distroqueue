package queue

import "sync/atomic"

// QueueStats holds real-time snapshot metrics of queue activity.
type QueueStats struct {
	TotalPushed int64 `json:"total_pushed"`
	TotalPopped int64 `json:"total_popped"`
	CurrentLen  int   `json:"current_len"`
}

// Queue wraps a QueueStore implementation and adds thread-safe atomic metrics.
// Notice: It depends strictly on the QueueStore interface and does NOT import store/.
type Queue struct {
	store QueueStore

	// Atomic counters for lock-free metric collection.
	totalPushed int64
	totalPopped int64
}

// NewQueue creates and initializes a new Queue with the provided QueueStore driver.
func NewQueue(store QueueStore) *Queue {
	return &Queue{
		store: store,
	}
}

// Push delegates task insertion to the underlying store and increments totalPushed metric.
func (q *Queue) Push(task *Task) error {
	if err := q.store.Push(task); err != nil {
		return err
	}
	atomic.AddInt64(&q.totalPushed, 1)
	return nil
}

// Pop delegates task retrieval to the underlying store.
// If a ready task was found, it increments totalPopped metric.
func (q *Queue) Pop() (*Task, bool) {
	task, ok := q.store.Pop()
	if ok {
		atomic.AddInt64(&q.totalPopped, 1)
	}
	return task, ok
}

// Len returns the current number of tasks in the underlying store.
func (q *Queue) Len() int {
	return q.store.Len()
}

// Drain removes and returns all tasks currently held in the underlying store.
func (q *Queue) Drain() []*Task {
	return q.store.Drain()
}

// Stats returns a point-in-time snapshot of the queue metrics.
func (q *Queue) Stats() QueueStats {
	return QueueStats{
		TotalPushed: atomic.LoadInt64(&q.totalPushed),
		TotalPopped: atomic.LoadInt64(&q.totalPopped),
		CurrentLen:  q.store.Len(),
	}
}
