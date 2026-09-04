package queue

// QueueStore defines the storage operations required for the task queue.
// Any backend implementing this interface (MemoryStore, RedisStore, PostgresStore, etc.)
// can be utilized interchangeably by the worker execution engine.
type QueueStore interface {
	// Push adds a task to the queue. Implementations must ensure thread-safety.
	// Priority ordering is handled by the underlying implementation.
	Push(task *Task) error

	// Pop retrieves and removes the highest priority ready (IsReady() == true) task.
	// If no ready task exists, it immediately returns nil, false without blocking.
	Pop() (*Task, bool)

	// Len returns the total count of tasks currently in the queue (including not yet ready).
	Len() int

	// Drain removes and returns all tasks currently held in the queue.
	// Primarily used for shutdown processing and state reporting.
	Drain() []*Task
}
