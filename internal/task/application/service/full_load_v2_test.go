package service

import (
	"testing"

	"mysql-to-sync/internal/sync/fullload"
)

func TestFullLoadStatsLifecycle(t *testing.T) {
	const taskID = "v2-stats-lifecycle"
	clearFullLoadStats(taskID)
	stats := &fullload.Stats{}
	setFullLoadStats(taskID, stats)
	if _, ok := fullLoadStatsSnapshot(taskID); !ok {
		t.Fatal("expected stored V2 stats")
	}
	clearFullLoadStats(taskID)
	if _, ok := fullLoadStatsSnapshot(taskID); ok {
		t.Fatal("deleted task must not retain V2 stats")
	}
}
