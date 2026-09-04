package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// LeaderElector coordinates single-leader status across workers using heartbeats and TTLs.
// It ensures that only one node at a time actively schedules periodic jobs.
type LeaderElector struct {
	mu            sync.Mutex
	currentLeader atomic.Value  // holds string: worker ID of current leader
	lastHeartbeat sync.Map      // workerID (string) -> time.Time
	leaderTTL     time.Duration // duration after which an inactive leader is declared dead
}

// NewLeaderElector creates a new election coordinator with the specified leader TTL.
func NewLeaderElector(ttl time.Duration) *LeaderElector {
	le := &LeaderElector{
		leaderTTL: ttl,
	}
	le.currentLeader.Store("")
	return le
}

// Campaign attempts to elect worker `id` as the leader.
// If there is currently no leader or the existing leader has missed heartbeats past leaderTTL,
// worker `id` claims the leadership, records its first heartbeat, and returns true.
// If the calling worker is already the leader, it refreshes its heartbeat and returns true.
// Otherwise, it returns false.
func (le *LeaderElector) Campaign(id string) bool {
	le.mu.Lock()
	defer le.mu.Unlock()

	leaderID, _ := le.currentLeader.Load().(string)

	if leaderID == "" || le.isLeaderDead(leaderID) {
		le.currentLeader.Store(id)
		le.Heartbeat(id)
		return true
	}

	if leaderID == id {
		le.Heartbeat(id)
		return true
	}

	return false
}

// Heartbeat registers the current timestamp as the latest alive signal from worker `id`.
func (le *LeaderElector) Heartbeat(id string) {
	le.lastHeartbeat.Store(id, time.Now())
}

// IsLeader reports whether worker `id` is the currently recognized leader.
func (le *LeaderElector) IsLeader(id string) bool {
	leader, ok := le.currentLeader.Load().(string)
	return ok && leader == id
}

// Leader returns the worker ID of the current leader, or an empty string if none is elected.
func (le *LeaderElector) Leader() string {
	leader, ok := le.currentLeader.Load().(string)
	if !ok {
		return ""
	}
	return leader
}

// isLeaderDead checks whether the given leaderID has failed to send a heartbeat within leaderTTL.
func (le *LeaderElector) isLeaderDead(leaderID string) bool {
	val, ok := le.lastHeartbeat.Load(leaderID)
	if !ok {
		return true
	}
	lastTime, ok := val.(time.Time)
	if !ok {
		return true
	}
	return time.Since(lastTime) > le.leaderTTL
}

// StartHeartbeating launches a background loop sending periodic heartbeats for worker `id`
// until the provided context is canceled.
func (le *LeaderElector) StartHeartbeating(ctx context.Context, id string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			le.Heartbeat(id)
		}
	}
}
