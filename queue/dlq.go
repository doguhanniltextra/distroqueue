package queue

import (
	"fmt"
	"sync"
	"time"
)

// DLQ (Dead Letter Queue) isolates poisoned or permanently failed tasks from the main queue.
// It keeps them quarantined in memory for manual inspection, debugging, alerting, or replay.
type DLQ struct {
	mu    sync.Mutex
	tasks []*Task
}

// NewDLQ creates and initializes a new Dead Letter Queue.
func NewDLQ() *DLQ {
	return &DLQ{
		tasks: make([]*Task, 0),
	}
}

// Push appends a task to the DLQ, marks its status as Dead, and logs the quarantined event.
func (d *DLQ) Push(task *Task) {
	d.mu.Lock()
	defer d.mu.Unlock()

	task.Status = Dead
	d.tasks = append(d.tasks, task)

	fmt.Printf("[DLQ] Task quarantined: ID=%s Name=%s Error=%q Retries=%d/%d\n",
		task.ID, task.Name, task.Error, task.Retries, task.MaxRetries)
}

// All returns an isolated snapshot copy of all quarantined tasks.
// Returns a new slice so modifications by callers do not cause data races or mutate internal state.
func (d *DLQ) All() []*Task {
	d.mu.Lock()
	defer d.mu.Unlock()

	cp := make([]*Task, len(d.tasks))
	copy(cp, d.tasks)
	return cp
}

// Size returns the total count of tasks currently in quarantine.
func (d *DLQ) Size() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return len(d.tasks)
}

// Replay resets quarantined tasks and re-enqueues them into the specified Queue.
// Returns the number of tasks replayed, and clears the DLQ.
func (d *DLQ) Replay(q *Queue) int {
	d.mu.Lock()
	defer d.mu.Unlock()

	count := len(d.tasks)
	for _, task := range d.tasks {
		task.Retries = 0
		task.Status = Pending
		task.Error = ""
		task.ExecuteAt = time.Now() // Immediately ready for processing

		_ = q.Push(task)
	}

	d.tasks = nil
	return count
}

// Clear wipes all tasks from the DLQ.
func (d *DLQ) Clear() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.tasks = nil
}
