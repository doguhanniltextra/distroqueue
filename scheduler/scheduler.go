package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/doguhanniltextra/distributed-queue/queue"
)

// ScheduledJob defines a recurring task configuration with its own interval.
type ScheduledJob struct {
	Name     string
	Payload  map[string]any
	Interval time.Duration
}

// Scheduler produces periodic tasks into the Queue, but ONLY if the assigned worker is the leader.
type Scheduler struct {
	queue    *queue.Queue
	elector  *LeaderElector
	workerID string
	jobs     []ScheduledJob
	mu       sync.Mutex
}

// NewScheduler creates and returns a new recurring task Scheduler.
func NewScheduler(q *queue.Queue, elector *LeaderElector, workerID string) *Scheduler {
	return &Scheduler{
		queue:    q,
		elector:  elector,
		workerID: workerID,
		jobs:     make([]ScheduledJob, 0),
	}
}

// AddJob registers a new recurring job to be scheduled at the given interval.
func (s *Scheduler) AddJob(name string, payload map[string]any, interval time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs = append(s.jobs, ScheduledJob{
		Name:     name,
		Payload:  payload,
		Interval: interval,
	})
}

// Start spawns a goroutine with its own independent ticker for each registered job.
// It checks leadership before enqueuing to prevent duplicate execution across nodes.
func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	jobsCopy := make([]ScheduledJob, len(s.jobs))
	copy(jobsCopy, s.jobs)
	s.mu.Unlock()

	var wg sync.WaitGroup
	for _, job := range jobsCopy {
		wg.Add(1)
		// Explicitly pass job as an argument to avoid loop variable capture bugs
		go func(j ScheduledJob) {
			defer wg.Done()
			ticker := time.NewTicker(j.Interval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return // Graceful shutdown
				case <-ticker.C:
					if !s.elector.IsLeader(s.workerID) {
						continue // Not the leader, skip enqueuing
					}
					task := queue.NewTask(j.Name, j.Payload)
					_ = s.queue.Push(task)
					fmt.Printf("[SCHEDULER] enqueued: %s\n", j.Name)
				}
			}
		}(job)
	}
	wg.Wait()
}
