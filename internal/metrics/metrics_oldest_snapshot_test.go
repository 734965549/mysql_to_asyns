package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestSetTaskFullLoadOldestSnapshotAgeMillisUsesMaxAcrossTasks(t *testing.T) {
	m := GetMetrics()
	m.ClearTaskFullLoadOldestSnapshotAge("task-a")
	m.ClearTaskFullLoadOldestSnapshotAge("task-b")

	m.SetTaskFullLoadOldestSnapshotAgeMillis("task-a", 1000)
	m.SetTaskFullLoadOldestSnapshotAgeMillis("task-b", 5000)
	assert.InDelta(t, 5000, testutil.ToFloat64(m.FullLoadOldestSnapshotMs), 0.01)

	// 较短任务结束写 0 不得清零全局 max。
	m.SetTaskFullLoadOldestSnapshotAgeMillis("task-a", 0)
	assert.InDelta(t, 5000, testutil.ToFloat64(m.FullLoadOldestSnapshotMs), 0.01)

	m.SetTaskFullLoadOldestSnapshotAgeMillis("task-b", 0)
	assert.InDelta(t, 0, testutil.ToFloat64(m.FullLoadOldestSnapshotMs), 0.01)
}
