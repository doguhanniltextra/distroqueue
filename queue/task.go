package queue

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Status represents the current execution state of a Task.
type Status int

const (
	Pending Status = iota // Enqueued, waiting for a worker
	Running               // Claimed by a worker, currently processing
	Done                  // Successfully finished
	Failed                // Failed on last attempt, awaiting retry
	Dead                  // Exceeded MaxRetries, moved to DLQ
)

// String returns a human-readable representation of the Status.
func (s Status) String() string {
	switch s {
	case Pending:
		return "PENDING"
	case Running:
		return "RUNNING"
	case Done:
		return "DONE"
	case Failed:
		return "FAILED"
	case Dead:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
}

// Task represents a unit of work within the distributed queue.
type Task struct {
	ID         string         `json:"id"`
	Name       string         `json:"name"`
	Payload    map[string]any `json:"payload"`
	Priority   int            `json:"priority"`
	ExecuteAt  time.Time      `json:"execute_at"`
	MaxRetries int            `json:"max_retries"`
	Retries    int            `json:"retries"`
	Status     Status         `json:"status"`
	CreatedAt  time.Time      `json:"created_at"`
	Error      string         `json:"error,omitempty"`
}

// NewTask initializes and returns a new Task with default values.
func NewTask(name string, payload map[string]any) *Task {
	now := time.Now()
	return &Task{
		ID:         uuid.New().String(),
		Name:       name,
		Payload:    payload,
		Priority:   1,
		ExecuteAt:  now,
		MaxRetries: 3,
		Retries:    0,
		Status:     Pending,
		CreatedAt:  now,
	}
}

// String returns a log-friendly summary of the Task.
func (t *Task) String() string {
	if t == nil {
		return "<nil Task>"
	}
	return fmt.Sprintf("Task[ID=%s, Name=%s, Priority=%d, Status=%s, Retries=%d/%d]",
		t.ID, t.Name, t.Priority, t.Status, t.Retries, t.MaxRetries)
}

// IsReady reports whether the task is ready to be executed based on ExecuteAt.
func (t *Task) IsReady() bool {
	if t == nil {
		return false
	}
	now := time.Now()
	return now.After(t.ExecuteAt) || now.Equal(t.ExecuteAt)
}
