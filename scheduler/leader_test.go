package scheduler_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/doguhanniltextra/distributed-queue/scheduler"
)

func TestLeaderElectionOnlyOne(t *testing.T) {
	le := scheduler.NewLeaderElector(1 * time.Second)

	var (
		winners int
		mu      sync.Mutex
		wg      sync.WaitGroup
	)

	const candidates = 15
	for i := 0; i < candidates; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			candidateID := fmt.Sprintf("worker-%d", id)
			if le.Campaign(candidateID) {
				mu.Lock()
				winners++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	if winners != 1 {
		t.Fatalf("expected exactly 1 elected leader, got %d", winners)
	}

	leader := le.Leader()
	if leader == "" {
		t.Fatal("expected leader ID, got empty string")
	}

	if !le.IsLeader(leader) {
		t.Errorf("IsLeader(%q) should be true", leader)
	}
}

func TestLeaderFailoverAfterTTLExpiry(t *testing.T) {
	// 200ms leader TTL
	le := scheduler.NewLeaderElector(200 * time.Millisecond)

	if !le.Campaign("worker-1") {
		t.Fatal("worker-1 should win initial campaign")
	}

	if le.Campaign("worker-2") {
		t.Error("worker-2 should not win while worker-1 is active leader")
	}

	if le.Leader() != "worker-1" {
		t.Errorf("leader should be worker-1, got %q", le.Leader())
	}

	// Do not send heartbeat, wait for TTL to expire
	time.Sleep(300 * time.Millisecond)

	// Now worker-2 campaigns and should become new leader
	if !le.Campaign("worker-2") {
		t.Fatal("worker-2 should win campaign after worker-1 TTL expiry")
	}

	if le.Leader() != "worker-2" {
		t.Errorf("new leader should be worker-2, got %q", le.Leader())
	}
}

func TestLeaderStartHeartbeating(t *testing.T) {
	le := scheduler.NewLeaderElector(200 * time.Millisecond)

	if !le.Campaign("worker-1") {
		t.Fatal("worker-1 should win initial campaign")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Send heartbeats every 50ms (well within 200ms TTL)
	go le.StartHeartbeating(ctx, "worker-1", 50*time.Millisecond)

	// Sleep 350ms - more than leader TTL, but heartbeats keep leader alive
	time.Sleep(350 * time.Millisecond)

	if le.Campaign("worker-2") {
		t.Error("worker-2 should not win because worker-1 is continuously heartbeating")
	}

	// Stop heartbeating by canceling context
	cancel()
	time.Sleep(300 * time.Millisecond)

	// Now worker-2 should be able to take over
	if !le.Campaign("worker-2") {
		t.Error("worker-2 should take over leadership after heartbeating stopped")
	}
	if le.Leader() != "worker-2" {
		t.Errorf("new leader should be worker-2, got %q", le.Leader())
	}
}
