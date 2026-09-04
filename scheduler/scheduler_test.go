package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/doguhanniltextra/distributed-queue/queue"
	"github.com/doguhanniltextra/distributed-queue/scheduler"
	"github.com/doguhanniltextra/distributed-queue/store"
)

func TestSchedulerOnlyLeaderEnqueues(t *testing.T) {
	ms := store.NewMemoryStore()
	q := queue.NewQueue(ms)
	le := scheduler.NewLeaderElector(1 * time.Second)

	// Worker-1 is leader
	if !le.Campaign("worker-1") {
		t.Fatal("worker-1 failed to become leader")
	}

	// Scheduler tied to non-leader worker-2
	followerSched := scheduler.NewScheduler(q, le, "worker-2")
	followerSched.AddJob("heartbeat_job", nil, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	go followerSched.Start(ctx)

	time.Sleep(150 * time.Millisecond)
	cancel()

	if q.Len() != 0 {
		t.Fatalf("expected 0 tasks from non-leader scheduler, got %d", q.Len())
	}

	// Now start scheduler tied to leader worker-1
	leaderSched := scheduler.NewScheduler(q, le, "worker-1")
	leaderSched.AddJob("leader_job", nil, 50*time.Millisecond)

	ctx2, cancel2 := context.WithCancel(context.Background())
	go leaderSched.Start(ctx2)

	time.Sleep(150 * time.Millisecond)
	cancel2()

	if q.Len() == 0 {
		t.Fatal("expected leader scheduler to enqueue jobs, but queue is empty")
	}
}
