package service

import (
	"encoding/json"
	"testing"

	taskEntity "mysql-to-sync/internal/task/domain/entity"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// === 续传游标辅助函数 ===

func TestStringifyKeyVal(t *testing.T) {
	assert.Equal(t, "", stringifyKeyVal(nil))
	assert.Equal(t, "abc", stringifyKeyVal("abc"))
	assert.Equal(t, "abc", stringifyKeyVal([]byte("abc")))
	assert.Equal(t, "123", stringifyKeyVal(int64(123)))
	assert.Equal(t, "123", stringifyKeyVal(123))
}

func TestResumeKeyFromValue(t *testing.T) {
	rk := resumeKeyFromValue(int64(42))
	require.NotNil(t, rk)
	assert.Equal(t, []string{"42"}, rk.Vals)

	rk = resumeKeyFromValue([]byte("user-7"))
	assert.Equal(t, []string{"user-7"}, rk.Vals)
}

func TestResumeKeyFromValues(t *testing.T) {
	rk := resumeKeyFromValues([]interface{}{int64(1), "b", []byte("c")})
	assert.Equal(t, []string{"1", "b", "c"}, rk.Vals)
}

func TestLastIDFromResumeKey(t *testing.T) {
	// 单列主键返回标量
	rk := &taskEntity.ResumeKey{Vals: []string{"100"}}
	assert.Equal(t, "100", lastIDFromResumeKey(rk, 1))

	// 复合主键返回 []interface{}
	rk = &taskEntity.ResumeKey{Vals: []string{"1", "2"}}
	got := lastIDFromResumeKey(rk, 2)
	assert.Equal(t, []interface{}{"1", "2"}, got)

	// 空/nil 返回 nil
	assert.Nil(t, lastIDFromResumeKey(nil, 1))
	assert.Nil(t, lastIDFromResumeKey(&taskEntity.ResumeKey{}, 1))
}

func TestResumeKeyToInt64(t *testing.T) {
	n, ok := resumeKeyToInt64(&taskEntity.ResumeKey{Vals: []string{"  98765 "}})
	require.True(t, ok)
	assert.Equal(t, int64(98765), n)

	_, ok = resumeKeyToInt64(&taskEntity.ResumeKey{Vals: []string{"not-a-number"}})
	assert.False(t, ok)

	_, ok = resumeKeyToInt64(nil)
	assert.False(t, ok)
}

func TestFullSyncTableKey(t *testing.T) {
	assert.Equal(t, "db1.users", fullSyncTableKey("db1", "users"))
}

func TestResumeEnabled(t *testing.T) {
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "t1"})
	assert.False(t, resumeEnabled(task))

	task.Config.EnableDropTableBeforeDDL = true
	assert.False(t, resumeEnabled(task))

	assert.False(t, resumeEnabled(nil))
}

// === ResumeKey JSON 往返序列化（含复合主键 / 二进制值）===

func TestTableSyncProgressJSONRoundTrip(t *testing.T) {
	orig := &taskEntity.TableSyncProgress{
		Done:         false,
		ReadPath:     "range",
		IntraWorkers: 4,
		Cursor:       &taskEntity.ResumeKey{Vals: []string{"500"}},
		ShardCursors: map[int]*taskEntity.ResumeKey{
			0: {Vals: []string{"125"}},
			3: {Vals: []string{"480", "x"}},
		},
		ProcessedRows: 1234,
	}

	data, err := json.Marshal(orig)
	require.NoError(t, err)

	var got taskEntity.TableSyncProgress
	require.NoError(t, json.Unmarshal(data, &got))

	assert.Equal(t, orig.ReadPath, got.ReadPath)
	assert.Equal(t, orig.IntraWorkers, got.IntraWorkers)
	assert.Equal(t, orig.Cursor.Vals, got.Cursor.Vals)
	assert.Equal(t, orig.ShardCursors[0].Vals, got.ShardCursors[0].Vals)
	assert.Equal(t, orig.ShardCursors[3].Vals, got.ShardCursors[3].Vals)
	assert.Equal(t, orig.ProcessedRows, got.ProcessedRows)
}

// === range 分片划分确定性：相同 min/max/workers 推导一致 ===

// rangeShards 复现 range 路径的分片边界划分逻辑（与 task_service.go 保持一致）。
func rangeShards(minPK, maxPK int64, workers int) [][2]int64 {
	span := maxPK - minPK + 1
	if span < int64(workers) {
		workers = int(span)
		if workers < 1 {
			workers = 1
		}
	}
	chunk := (span + int64(workers) - 1) / int64(workers)
	out := make([][2]int64, 0, workers)
	for w := 0; w < workers; w++ {
		start := minPK + int64(w)*chunk
		if start > maxPK {
			break
		}
		end := maxPK + 1
		if w < workers-1 {
			next := minPK + int64(w+1)*chunk
			if next < end {
				end = next
			}
		}
		out = append(out, [2]int64{start, end})
	}
	return out
}

func TestRangeShardDeterminism(t *testing.T) {
	a := rangeShards(1, 1000, 4)
	b := rangeShards(1, 1000, 4)
	assert.Equal(t, a, b, "相同 min/max/workers 应得到一致的分片划分")

	// 分片应覆盖整个 [min, max] 且互不重叠
	require.Len(t, a, 4)
	assert.Equal(t, int64(1), a[0][0])
	assert.Equal(t, int64(1001), a[len(a)-1][1])
	for i := 1; i < len(a); i++ {
		assert.Equal(t, a[i-1][1], a[i][0], "相邻分片应首尾相接")
	}
}

// === 实体续传断点状态机 ===

func TestTableSyncProgressLifecycle(t *testing.T) {
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "t1"})
	key := "db.users"

	// 初始无进度
	assert.Nil(t, task.GetTableProgress(key))

	task.InitTableProgress(key, "range", 4)
	p := task.GetTableProgress(key)
	require.NotNil(t, p)
	assert.Equal(t, "range", p.ReadPath)
	assert.Equal(t, 4, p.IntraWorkers)
	assert.False(t, p.Done)

	task.SetShardCursor(key, 0, &taskEntity.ResumeKey{Vals: []string{"100"}})
	task.SetShardCursor(key, 1, &taskEntity.ResumeKey{Vals: []string{"300"}})
	p = task.GetTableProgress(key)
	assert.Equal(t, []string{"100"}, p.ShardCursors[0].Vals)
	assert.Equal(t, []string{"300"}, p.ShardCursors[1].Vals)

	// 标记完成应清理游标
	task.MarkTableDone(key)
	p = task.GetTableProgress(key)
	assert.True(t, p.Done)
	assert.Nil(t, p.ShardCursors)
	assert.Nil(t, p.Cursor)

	// 重置清空所有断点
	task.ResetFullSyncResume()
	assert.Nil(t, task.GetTableProgress(key))
}

func TestSetTableCursor(t *testing.T) {
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "t1"})
	key := "db.orders"
	task.SetTableCursor(key, &taskEntity.ResumeKey{Vals: []string{"abc"}})
	p := task.GetTableProgress(key)
	require.NotNil(t, p)
	assert.Equal(t, []string{"abc"}, p.Cursor.Vals)
}

// === 服务层续传辅助：记录 / 跳过 / 生命周期清理 ===

func TestServiceRecordResumeCursorAndSkip(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "resume_task", Name: "Resume"})
	require.NoError(t, err)
	key := fullSyncTableKey("db", "users")

	// 单线程 keyset 游标（shard=-1）
	ts.recordResumeCursor("resume_task", key, -1, &taskEntity.ResumeKey{Vals: []string{"500"}})
	p := ts.getTableProgress("resume_task", key)
	require.NotNil(t, p)
	assert.Equal(t, []string{"500"}, p.Cursor.Vals)

	// 分片游标
	ts.recordResumeCursor("resume_task", key, 2, &taskEntity.ResumeKey{Vals: []string{"800"}})
	p = ts.getTableProgress("resume_task", key)
	assert.Equal(t, []string{"800"}, p.ShardCursors[2].Vals)

	// nil key 不写入
	ts.recordResumeCursor("resume_task", key, 5, nil)
	p = ts.getTableProgress("resume_task", key)
	_, exists := p.ShardCursors[5]
	assert.False(t, exists)

	// 标记完成
	ts.markTableDone("resume_task", key)
	p = ts.getTableProgress("resume_task", key)
	assert.True(t, p.Done)

	_ = task
}

func TestResetResumeIfFresh(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "fresh_task", Name: "Fresh"})
	require.NoError(t, err)
	key := fullSyncTableKey("db", "users")

	// 全新任务（SyncPhaseInit）：非续传中间态 -> 清空
	ts.recordResumeCursor("fresh_task", key, -1, &taskEntity.ResumeKey{Vals: []string{"10"}})
	ts.resetResumeIfFresh("fresh_task")
	assert.Nil(t, ts.getTableProgress("fresh_task", key))

	// 处于"全量未完成中间态"也不再保留断点：全量普通 INSERT 不支持续传
	task.Context.SyncPhase = taskEntity.SyncPhaseFullStarted
	ts.recordResumeCursor("fresh_task", key, -1, &taskEntity.ResumeKey{Vals: []string{"20"}})
	ts.resetResumeIfFresh("fresh_task")
	assert.Nil(t, ts.getTableProgress("fresh_task", key))

	// 开启 DROP TABLE -> 同样清空
	task.Config.EnableDropTableBeforeDDL = true
	ts.recordResumeCursor("fresh_task", key, -1, &taskEntity.ResumeKey{Vals: []string{"30"}})
	ts.resetResumeIfFresh("fresh_task")
	assert.Nil(t, ts.getTableProgress("fresh_task", key))
}

func TestClearFullSyncResume(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	_, err := ts.CreateTask(taskEntity.TaskConfig{ID: "clear_task", Name: "Clear"})
	require.NoError(t, err)
	key := fullSyncTableKey("db", "users")

	ts.recordResumeCursor("clear_task", key, -1, &taskEntity.ResumeKey{Vals: []string{"1"}})
	require.NotNil(t, ts.getTableProgress("clear_task", key))

	ts.clearFullSyncResume("clear_task")
	assert.Nil(t, ts.getTableProgress("clear_task", key))
}

// TestFullSyncRestartBlockedError 覆盖全量中断后重启的拦截判定：
//   - 开启 enable_drop_table_before_ddl -> 放行（目标端会被重建）
//   - 未开启且全量未完成（FULL_STARTED / FULL_FAILED）+ FULL/ALL 模式 -> 拒绝
//   - 已完成全量 / INCREMENTAL 模式 / nil 任务 -> 放行
func TestFullSyncRestartBlockedError(t *testing.T) {
	makeTask := func(mode taskEntity.SyncMode, dropDDL bool, phase taskEntity.SyncPhase) *taskEntity.SyncTask {
		task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
			ID:                       "block_chk",
			Mode:                     mode,
			EnableDropTableBeforeDDL: dropDDL,
		})
		task.Context.SyncPhase = phase
		return task
	}

	tests := []struct {
		name    string
		task    *taskEntity.SyncTask
		wantErr bool
	}{
		{
			name:    "drop_ddl enabled with FULL_STARTED -> allowed",
			task:    makeTask(taskEntity.SyncModeFull, true, taskEntity.SyncPhaseFullStarted),
			wantErr: false,
		},
		{
			name:    "drop_ddl enabled with FULL_FAILED -> allowed",
			task:    makeTask(taskEntity.SyncModeAll, true, taskEntity.SyncPhaseFullFailed),
			wantErr: false,
		},
		{
			name:    "drop_ddl false + FULL_STARTED + FULL mode -> blocked",
			task:    makeTask(taskEntity.SyncModeFull, false, taskEntity.SyncPhaseFullStarted),
			wantErr: true,
		},
		{
			name:    "drop_ddl false + FULL_FAILED + ALL mode -> blocked",
			task:    makeTask(taskEntity.SyncModeAll, false, taskEntity.SyncPhaseFullFailed),
			wantErr: true,
		},
		{
			name:    "drop_ddl false + FULL_COMPLETED -> allowed (already completed)",
			task:    makeTask(taskEntity.SyncModeFull, false, taskEntity.SyncPhaseFullCompleted),
			wantErr: false,
		},
		{
			name:    "drop_ddl false + INCREMENTAL_STARTED -> allowed",
			task:    makeTask(taskEntity.SyncModeAll, false, taskEntity.SyncPhaseIncrementalStarted),
			wantErr: false,
		},
		{
			name:    "drop_ddl false + FULL_STARTED + INCREMENTAL mode -> allowed (mode not FULL/ALL)",
			task:    makeTask(taskEntity.SyncModeIncremental, false, taskEntity.SyncPhaseFullStarted),
			wantErr: false,
		},
		{
			name:    "drop_ddl false + Init phase + FULL mode -> allowed (never started)",
			task:    makeTask(taskEntity.SyncModeFull, false, taskEntity.SyncPhaseInit),
			wantErr: false,
		},
		{
			name:    "nil task -> allowed",
			task:    nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := fullSyncRestartBlockedError(tt.task)
			if tt.wantErr {
				require.Error(t, err, "expected restart to be blocked")
				assert.Contains(t, err.Error(), "enable_drop_table_before_ddl=false",
					"error message should explain the drop_ddl condition")
				assert.Contains(t, err.Error(), "full sync was interrupted",
					"error message should explain the interruption")
			} else {
				assert.NoError(t, err, "expected restart to be allowed")
			}
		})
	}
}
