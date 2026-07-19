package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"mysql-to-sync/internal/config"
	sink "mysql-to-sync/internal/sync/domain/sink"
	taskEntity "mysql-to-sync/internal/task/domain/entity"
)

// makeAllTaskAtIncremental 构造一个 RUNNING + ALL + INCREMENTAL_STARTED 的任务并放入服务。
func makeAllTaskAtIncremental(t *testing.T, ts *TaskService, taskID string) *taskEntity.SyncTask {
	t.Helper()
	task, err := ts.CreateTask(taskEntity.TaskConfig{
		ID:             taskID,
		Name:           "end-task-test",
		Mode:           taskEntity.SyncModeAll,
		SyncLevel:      taskEntity.SyncLevelTable,
		SourceSchema:   "src_db",
		TargetSchema:   "tgt_db",
		Tables:         []string{"users"},
		TargetTables:   []string{"users"},
	})
	require.NoError(t, err)
	task.Start()
	task.MarkIncrementalStarted()
	ts.tasks[taskID] = task
	return task
}

// createAndStageTask 创建任务并通过 mutator 调整状态后放回服务映射，返回任务指针。
func createAndStageTask(t *testing.T, ts *TaskService, cfg taskEntity.TaskConfig, stage func(*taskEntity.SyncTask)) *taskEntity.SyncTask {
	t.Helper()
	task, err := ts.CreateTask(cfg)
	require.NoError(t, err)
	stage(task)
	ts.tasks[cfg.ID] = task
	return task
}

// TestEndTask_SetsStopped 验证 EndTask 在合法条件下设置 STOPPED 终态、清除调度配置，
// 并从运行时映射摘除增量服务和 runtime。
func TestEndTask_SetsStopped(t *testing.T) {
	ts := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()}})
	defer ts.Close()

	taskID := "end_ok"
	makeAllTaskAtIncremental(t, ts, taskID)
	// 注入增量服务与 runtime 引用，验证 EndTask 会从映射摘除
	ts.incrementalSyncs[taskID] = nil // 占位：EndTask 仅 delete，不调用 nil.Stop()
	ts.runtimes[taskID] = &taskRuntime{}

	err := ts.EndTask(taskID)
	require.NoError(t, err)

	task, ok := ts.GetTask(taskID)
	require.True(t, ok)
	assert.Equal(t, taskEntity.TaskStatusStopped, task.Context.Status)
	assert.False(t, task.Context.EndTime.IsZero(), "end_time must be set")
	assert.False(t, task.Context.LastUpdateTime.IsZero())
	// 调度配置应被清除
	assert.Equal(t, "", task.Context.ScheduleMode)
	assert.Equal(t, "", task.Context.CronExpression)
	assert.Nil(t, task.Context.ScheduledAt)
	// runtime 应已从映射移除
	_, hasRuntime := ts.runtimes[taskID]
	assert.False(t, hasRuntime, "runtime should be removed from map")
	// 增量服务应已从映射移除
	_, hasIncr := ts.incrementalSyncs[taskID]
	assert.False(t, hasIncr, "incremental sync service should be removed from map")
}

// TestEndTask_NotFound
func TestEndTask_NotFound(t *testing.T) {
	ts := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()}})
	defer ts.Close()

	err := ts.EndTask("missing")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "task not found")
}

// TestEndTask_NotRunning 暂停状态不允许结束。
func TestEndTask_NotRunning(t *testing.T) {
	ts := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()}})
	defer ts.Close()

	createAndStageTask(t, ts, taskEntity.TaskConfig{
		ID: "end_paused", Mode: taskEntity.SyncModeAll, SourceSchema: "s", TargetSchema: "t",
	}, func(task *taskEntity.SyncTask) {
		task.Start()
		task.MarkIncrementalStarted()
		task.Pause() // 暂停后非 RUNNING
	})

	err := ts.EndTask("end_paused")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be ended")
}

// TestEndTask_FullModeNotAllowed FULL 模式不允许结束。
func TestEndTask_FullModeNotAllowed(t *testing.T) {
	ts := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()}})
	defer ts.Close()

	createAndStageTask(t, ts, taskEntity.TaskConfig{
		ID: "end_full", Mode: taskEntity.SyncModeFull, SourceSchema: "s", TargetSchema: "t",
	}, func(task *taskEntity.SyncTask) {
		task.Start()
		task.MarkFullSyncCompleted()
	})

	err := ts.EndTask("end_full")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only ALL mode")
}

// TestEndTask_NotIncrementalPhase 增量阶段之外不允许结束。
func TestEndTask_NotIncrementalPhase(t *testing.T) {
	ts := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()}})
	defer ts.Close()

	createAndStageTask(t, ts, taskEntity.TaskConfig{
		ID: "end_phase", Mode: taskEntity.SyncModeAll, SourceSchema: "s", TargetSchema: "t",
	}, func(task *taskEntity.SyncTask) {
		task.Start()
		task.MarkFullSyncStarted("") // 全量阶段，非增量
	})

	err := ts.EndTask("end_phase")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "incremental phase")
}

// TestStartTask_RejectsStopped STOPPED 任务不能重新启动。
func TestStartTask_RejectsStopped(t *testing.T) {
	ts := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()}})
	defer ts.Close()

	makeAllTaskAtIncremental(t, ts, "stopped_restart")
	require.NoError(t, ts.EndTask("stopped_restart"))

	err := ts.StartTask(t.Context(), "stopped_restart")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be restarted")
}

// TestUpdateTask_RejectsStopped STOPPED 任务不能编辑。
func TestUpdateTask_RejectsStopped(t *testing.T) {
	ts := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()}})
	defer ts.Close()

	makeAllTaskAtIncremental(t, ts, "stopped_edit")
	require.NoError(t, ts.EndTask("stopped_edit"))

	// 尝试更新（编辑）
	updated := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID: "stopped_edit", Name: "renamed", Mode: taskEntity.SyncModeAll,
		SourceSchema: "s", TargetSchema: "t",
	})
	err := ts.UpdateTask(updated)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be edited")
}

// TestScheduleTask_RejectsStopped STOPPED 任务不能设置定时调度。
func TestScheduleTask_RejectsStopped(t *testing.T) {
	ts := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()}})
	defer ts.Close()

	makeAllTaskAtIncremental(t, ts, "stopped_sched")
	require.NoError(t, ts.EndTask("stopped_sched"))

	err := ts.ScheduleTask("stopped_sched", time.Now().Add(time.Hour))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot schedule")
}

// TestStartRowCountComparison_Validation 覆盖启动前各类校验失败与成功路径。
func TestStartRowCountComparison_Validation(t *testing.T) {
	ts := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()}})
	defer ts.Close()

	t.Run("not found", func(t *testing.T) {
		err := ts.StartRowCountComparison("missing")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "task not found")
	})

	t.Run("wrong status running", func(t *testing.T) {
		makeAllTaskAtIncremental(t, ts, "cmp_running")
		err := ts.StartRowCountComparison("cmp_running")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not in a comparable status")
	})

	t.Run("wrong mode incremental", func(t *testing.T) {
		createAndStageTask(t, ts, taskEntity.TaskConfig{
			ID: "cmp_incr", Mode: taskEntity.SyncModeIncremental, SourceSchema: "s", TargetSchema: "t",
		}, func(task *taskEntity.SyncTask) {
			task.Start()
			task.MarkIncrementalStarted()
			task.Complete()
		})

		err := ts.StartRowCountComparison("cmp_incr")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not support row count comparison")
	})

	t.Run("full not completed", func(t *testing.T) {
		// FULL 模式任务尚未完成全量（SyncPhase=FULL_STARTED）-> HasFullSyncEverCompleted 为假
		createAndStageTask(t, ts, taskEntity.TaskConfig{
			ID: "cmp_no_full", Mode: taskEntity.SyncModeFull, SourceSchema: "s", TargetSchema: "t",
		}, func(task *taskEntity.SyncTask) {
			task.Start()
			task.MarkFullSyncStarted("")
			task.Complete() // COMPLETED 但 SyncPhase=FULL_STARTED
		})

		err := ts.StartRowCountComparison("cmp_no_full")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "has not completed yet")
	})

	t.Run("non mysql target rejected", func(t *testing.T) {
		createAndStageTask(t, ts, taskEntity.TaskConfig{
			ID: "cmp_kafka", Mode: taskEntity.SyncModeAll, SourceSchema: "s", TargetSchema: "t",
			SinkConfigs: []sink.SinkConfig{{Type: sink.SinkTypeKAFKA}},
		}, func(task *taskEntity.SyncTask) {
			task.Start()
			task.MarkIncrementalStarted()
			task.Stop() // STOPPED
		})

		err := ts.StartRowCountComparison("cmp_kafka")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires MySQL target")
	})

	t.Run("stopped all task accepted", func(t *testing.T) {
		// STOPPED + ALL + 增量已完成 -> 允许启动核对（后台 goroutine 会因无真实 DB 失败，但启动本身成功）
		makeAllTaskAtIncremental(t, ts, "cmp_stopped_ok")
		require.NoError(t, ts.EndTask("cmp_stopped_ok"))

		err := ts.StartRowCountComparison("cmp_stopped_ok")
		require.NoError(t, err)

		// 等待后台 goroutine 进入 CHECKING 并最终 FAILED（无真实数据库连接）
		require.Eventually(t, func() bool {
			t, ok := ts.GetTask("cmp_stopped_ok")
			if !ok || t.Context.RowCountComparison == nil {
				return false
			}
			return t.Context.RowCountComparison.Status == taskEntity.RowCountComparisonFailed
		}, 5*time.Second, 50*time.Millisecond, "comparison should fail due to no real DB")

		// 重复启动应被拦截（409 already in progress）——但此时已 FAILED，所以重新启动允许。
		// 这里验证 FAILED 后任务终态不被修改为非 STOPPED。
		task2, ok := ts.GetTask("cmp_stopped_ok")
		require.True(t, ok)
		assert.Equal(t, taskEntity.TaskStatusStopped, task2.Context.Status, "comparison failure must not change task terminal status")
	})
}

// TestStartRowCountComparison_DuplicateRejected 已有 CHECKING 核对时拒绝重复启动。
func TestStartRowCountComparison_DuplicateRejected(t *testing.T) {
	ts := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()}})
	defer ts.Close()

	task := makeAllTaskAtIncremental(t, ts, "cmp_dup")
	require.NoError(t, ts.EndTask("cmp_dup"))

	// 手动将存档状态置为 CHECKING 模拟进行中
	now := time.Now()
	task.Context.RowCountComparison = &taskEntity.RowCountComparison{
		Status:    taskEntity.RowCountComparisonChecking,
		StartedAt: &now,
	}
	ts.tasks["cmp_dup"] = task

	err := ts.StartRowCountComparison("cmp_dup")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already in progress")
}

// TestAggregateRowCountComparison 覆盖汇总逻辑：一致 / 不一致 / 失败 / 部分失败 / 全失败。
func TestAggregateRowCountComparison(t *testing.T) {
	s100 := int64(100)
	t99 := int64(99)
	t100 := int64(100)
	dneg := int64(-1)
	dzero := int64(0)

	t.Run("all matched", func(t *testing.T) {
		done := []taskEntity.RowCountComparisonTable{
			{SourceRows: &s100, TargetRows: &t100, Difference: &dzero, Matched: true},
			{SourceRows: &s100, TargetRows: &t100, Difference: &dzero, Matched: true},
		}
		agg := aggregateRowCountComparison(done)
		assert.Equal(t, 2, agg.matched)
		assert.Equal(t, 0, agg.mismatched)
		assert.Equal(t, 0, agg.failed)
		assert.Equal(t, int64(200), agg.sourceTotal)
		assert.Equal(t, int64(200), agg.targetTotal)

		assert.Equal(t, 2, countMatchedTables(done))
		assert.Equal(t, 0, countMismatchedTables(done))
		assert.Equal(t, 0, countFailedTables(done))
	})

	t.Run("mismatched", func(t *testing.T) {
		done := []taskEntity.RowCountComparisonTable{
			{SourceRows: &s100, TargetRows: &t99, Difference: &dneg, Matched: false},
		}
		agg := aggregateRowCountComparison(done)
		assert.Equal(t, 0, agg.matched)
		assert.Equal(t, 1, agg.mismatched)
		assert.Equal(t, int64(-1), agg.targetTotal-agg.sourceTotal)
	})

	t.Run("partial failure", func(t *testing.T) {
		done := []taskEntity.RowCountComparisonTable{
			{SourceRows: &s100, TargetRows: &t100, Difference: &dzero, Matched: true},
			{SourceRows: nil, TargetRows: nil, Error: "source: connection refused"},
		}
		agg := aggregateRowCountComparison(done)
		assert.Equal(t, 1, agg.matched)
		assert.Equal(t, 1, agg.failed)
		assert.Equal(t, 1, countFailedTables(done))
	})

	t.Run("all failed", func(t *testing.T) {
		done := []taskEntity.RowCountComparisonTable{
			{SourceRows: nil, TargetRows: nil, Error: "source: failed"},
			{SourceRows: nil, TargetRows: nil, Error: "target: failed"},
		}
		agg := aggregateRowCountComparison(done)
		assert.Equal(t, 0, agg.matched)
		assert.Equal(t, 2, agg.failed)
		assert.Equal(t, 2, countFailedTables(done))
	})
}

// TestDeriveRowCountComparisonStatus 覆盖最终汇总状态推导：MATCHED/MISMATCHED（目标多行/少行）/PARTIAL/FAILED（全失败/空表清单）。
// 这是审查发现的 bug 回归测试：所有表失败时应为 FAILED 而非 PARTIAL。
func TestDeriveRowCountComparisonStatus(t *testing.T) {
	s100 := int64(100)
	t99 := int64(99)   // 目标少行
	t101 := int64(101) // 目标多行
	dNeg := int64(-1)
	dPos := int64(1)
	dZero := int64(0)

	t.Run("all matched", func(t *testing.T) {
		done := []taskEntity.RowCountComparisonTable{
			{SourceRows: &s100, TargetRows: &s100, Difference: &dZero, Matched: true},
		}
		status, reason := deriveRowCountComparisonStatus(done)
		assert.Equal(t, taskEntity.RowCountComparisonMatched, status)
		assert.Empty(t, reason)
	})

	t.Run("target fewer rows -> mismatched", func(t *testing.T) {
		done := []taskEntity.RowCountComparisonTable{
			{SourceRows: &s100, TargetRows: &t99, Difference: &dNeg, Matched: false},
		}
		status, _ := deriveRowCountComparisonStatus(done)
		assert.Equal(t, taskEntity.RowCountComparisonMismatched, status)
	})

	t.Run("target more rows -> mismatched", func(t *testing.T) {
		done := []taskEntity.RowCountComparisonTable{
			{SourceRows: &s100, TargetRows: &t101, Difference: &dPos, Matched: false},
		}
		status, _ := deriveRowCountComparisonStatus(done)
		assert.Equal(t, taskEntity.RowCountComparisonMismatched, status)
	})

	t.Run("both zero rows -> matched", func(t *testing.T) {
		zero := int64(0)
		done := []taskEntity.RowCountComparisonTable{
			{SourceRows: &zero, TargetRows: &zero, Difference: &zero, Matched: true},
		}
		status, _ := deriveRowCountComparisonStatus(done)
		assert.Equal(t, taskEntity.RowCountComparisonMatched, status)
	})

	t.Run("partial failure -> PARTIAL", func(t *testing.T) {
		done := []taskEntity.RowCountComparisonTable{
			{SourceRows: &s100, TargetRows: &s100, Difference: &dZero, Matched: true},
			{SourceRows: nil, TargetRows: nil, Error: "source: connection refused"},
		}
		status, _ := deriveRowCountComparisonStatus(done)
		assert.Equal(t, taskEntity.RowCountComparisonPartial, status)
	})

	t.Run("source failed only -> FAILED", func(t *testing.T) {
		done := []taskEntity.RowCountComparisonTable{
			{SourceRows: nil, TargetRows: &s100, Error: "source: timeout"},
		}
		status, _ := deriveRowCountComparisonStatus(done)
		assert.Equal(t, taskEntity.RowCountComparisonFailed, status)
	})

	t.Run("target failed only -> FAILED", func(t *testing.T) {
		done := []taskEntity.RowCountComparisonTable{
			{SourceRows: &s100, TargetRows: nil, Error: "target: timeout"},
		}
		status, _ := deriveRowCountComparisonStatus(done)
		assert.Equal(t, taskEntity.RowCountComparisonFailed, status)
	})

	t.Run("all tables failed -> FAILED (regression for PARTIAL bug)", func(t *testing.T) {
		done := []taskEntity.RowCountComparisonTable{
			{SourceRows: nil, TargetRows: nil, Error: "source: failed"},
			{SourceRows: nil, TargetRows: nil, Error: "target: failed"},
		}
		status, reason := deriveRowCountComparisonStatus(done)
		assert.Equal(t, taskEntity.RowCountComparisonFailed, status)
		assert.Contains(t, reason, "所有表均无法完成核对")
	})

	t.Run("empty table list -> FAILED", func(t *testing.T) {
		status, reason := deriveRowCountComparisonStatus(nil)
		assert.Equal(t, taskEntity.RowCountComparisonFailed, status)
		assert.Contains(t, reason, "待核对表清单为空")
	})

	t.Run("mixed matched and mismatched -> MISMATCHED", func(t *testing.T) {
		done := []taskEntity.RowCountComparisonTable{
			{SourceRows: &s100, TargetRows: &s100, Difference: &dZero, Matched: true},
			{SourceRows: &s100, TargetRows: &t99, Difference: &dNeg, Matched: false},
		}
		status, _ := deriveRowCountComparisonStatus(done)
		assert.Equal(t, taskEntity.RowCountComparisonMismatched, status)
	})
}

// TestQuoteIdentifierForCount 验证反引号转义（含双写）。
func TestQuoteIdentifierForCount(t *testing.T) {
	assert.Equal(t, "`users`", quoteIdentifierForCount("users"))
	assert.Equal(t, "`weird``name`", quoteIdentifierForCount("weird`name"))
	assert.Equal(t, "`schema`.`table`",
		quoteIdentifierForCount("schema")+"."+quoteIdentifierForCount("table"))
}

// TestCountRowsExact 验证精确 COUNT(*) 查询与反引号转义。
func TestCountRowsExact(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	expectedQuery := "SELECT COUNT(*) FROM `src_db`.`users`"
	mock.ExpectQuery(expectedQuery).
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(int64(100)))

	count, err := countRowsExact(context.Background(), db, "src_db", "users")
	require.NoError(t, err)
	assert.Equal(t, int64(100), count)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestCountRowsExact_ZeroRows 验证真实 0 行（区分失败 nil）。
func TestCountRowsExact_ZeroRows(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT COUNT(*) FROM `s`.`empty`").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(int64(0)))

	count, err := countRowsExact(context.Background(), db, "s", "empty")
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestCountRowsExact_QuoteEscaping 验证含反引号的表名被双写转义。
func TestCountRowsExact_QuoteEscaping(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	// weird`name -> `weird``name`
	mock.ExpectQuery("SELECT COUNT(*) FROM `s`.`weird``name`").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(int64(7)))

	count, err := countRowsExact(context.Background(), db, "s", "weird`name")
	require.NoError(t, err)
	assert.Equal(t, int64(7), count)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestCountRowsExact_QueryError 验证查询失败返回错误。
func TestCountRowsExact_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT COUNT(*) FROM `s`.`missing`").
		WillReturnError(assert.AnError)

	_, err = countRowsExact(context.Background(), db, "s", "missing")
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestListBaseTables 验证库级任务从 information_schema 获取 BASE TABLE 列表。
func TestListBaseTables(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("\n\t\tSELECT TABLE_NAME\n\t\tFROM information_schema.TABLES\n\t\tWHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'\n\t\tORDER BY TABLE_NAME\n\t").
		WithArgs("src_db").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME"}).
			AddRow("users").AddRow("orders"))

	names, err := listBaseTables(context.Background(), db, "src_db")
	require.NoError(t, err)
	assert.Equal(t, []string{"users", "orders"}, names)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestResolveComparisonTables_SingleDBTableLevel 指定表 + 单库 + TargetTables 重命名映射。
func TestResolveComparisonTables_SingleDBTableLevel(t *testing.T) {
	ts := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()}})
	defer ts.Close()

	task, err := ts.CreateTask(taskEntity.TaskConfig{
		ID:           "cmp_tables_single",
		Mode:         taskEntity.SyncModeAll,
		SyncLevel:    taskEntity.SyncLevelTable,
		SourceSchema: "src_db",
		TargetSchema: "tgt_db",
		Tables:       []string{"users", "orders"},
		TargetTables: []string{"users_new", "orders"}, // users 重命名
	})
	require.NoError(t, err)

	tables, err := ts.resolveComparisonTables(context.Background(), task, nil)
	require.NoError(t, err)
	require.Len(t, tables, 2)
	assert.Equal(t, "src_db", tables[0].sourceSchema)
	assert.Equal(t, "users", tables[0].sourceTable)
	assert.Equal(t, "tgt_db", tables[0].targetSchema)
	assert.Equal(t, "users_new", tables[0].targetTable)
	assert.Equal(t, "orders", tables[1].targetTable)
}

// TestResolveComparisonTables_MultiDB 多库映射 + 库级列举 BASE TABLE。
func TestResolveComparisonTables_MultiDB(t *testing.T) {
	ts := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()}})
	defer ts.Close()

	task, err := ts.CreateTask(taskEntity.TaskConfig{
		ID:              "cmp_tables_multi",
		Mode:            taskEntity.SyncModeAll,
		SyncLevel:       taskEntity.SyncLevelDatabase,
		SourceDatabases: []string{"src_a", "src_b"},
		TargetDatabases: []string{"tgt_a", "tgt_b"},
	})
	require.NoError(t, err)

	// 用 sqlmock 模拟源端连接列举两个库的表
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery(`SELECT TABLE_NAME FROM information_schema.TABLES`).
		WithArgs("src_a").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME"}).AddRow("t1"))
	mock.ExpectQuery(`SELECT TABLE_NAME FROM information_schema.TABLES`).
		WithArgs("src_b").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME"}).AddRow("t2"))

	tables, err := ts.resolveComparisonTables(context.Background(), task, db)
	require.NoError(t, err)
	require.Len(t, tables, 2)

	// src_a.t1 -> tgt_a.t1, src_b.t2 -> tgt_b.t2
	assert.Equal(t, "src_a", tables[0].sourceSchema)
	assert.Equal(t, "t1", tables[0].sourceTable)
	assert.Equal(t, "tgt_a", tables[0].targetSchema)
	assert.Equal(t, "src_b", tables[1].sourceSchema)
	assert.Equal(t, "t2", tables[1].sourceTable)
	assert.Equal(t, "tgt_b", tables[1].targetSchema)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestLoadTasks_RecoversCheckingComparison 服务重启时 CHECKING 行数对比应恢复为 FAILED。
func TestLoadTasks_RecoversCheckingComparison(t *testing.T) {
	dataDir := t.TempDir()
	ts1 := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: dataDir}})
	task := makeAllTaskAtIncremental(t, ts1, "restart_cmp")
	require.NoError(t, ts1.EndTask("restart_cmp"))
	// 手动写入 CHECKING 状态
	now := time.Now()
	task.Context.RowCountComparison = &taskEntity.RowCountComparison{
		Status:    taskEntity.RowCountComparisonChecking,
		StartedAt: &now,
	}
	require.NoError(t, ts1.storage.Save(task))
	require.NoError(t, ts1.Close())

	// 重新加载
	ts2 := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: dataDir}})
	defer ts2.Close()

	loaded, ok := ts2.GetTask("restart_cmp")
	require.True(t, ok)
	require.NotNil(t, loaded.Context.RowCountComparison)
	assert.Equal(t, taskEntity.RowCountComparisonFailed, loaded.Context.RowCountComparison.Status)
	assert.Contains(t, loaded.Context.RowCountComparison.FailureReason, "服务重启")
}

// TestFinalizeRowCountComparison_OverwritesOldResult 验证核对完成后覆盖旧结果（不保存历史版本）。
func TestFinalizeRowCountComparison_OverwritesOldResult(t *testing.T) {
	ts := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()}})
	defer ts.Close()

	makeAllTaskAtIncremental(t, ts, "overwrite_cmp")
	require.NoError(t, ts.EndTask("overwrite_cmp"))

	// 先注入一个旧的 MISMATCHED 结果
	oldNow := time.Now()
	oldTask, _ := ts.GetTask("overwrite_cmp")
	srcOld := int64(100)
	tgtOld := int64(99)
	dOld := int64(-1)
	oldTask.Context.RowCountComparison = &taskEntity.RowCountComparison{
		Status:      taskEntity.RowCountComparisonMismatched,
		StartedAt:   &oldNow,
		SourceTotal: 100,
		TargetTotal: 99,
		Tables: []taskEntity.RowCountComparisonTable{
			{SourceSchema: "s", SourceTable: "t", TargetSchema: "s", TargetTable: "t",
				SourceRows: &srcOld, TargetRows: &tgtOld, Difference: &dOld, Matched: false},
		},
	}
	require.NoError(t, ts.storage.Save(oldTask))

	// 调用 finalize 覆盖为新结果（MATCHED）
	tables := []comparisonTableTask{{sourceSchema: "s", sourceTable: "t", targetSchema: "s", targetTable: "t"}}
	s100 := int64(100)
	dZero := int64(0)
	results := []taskEntity.RowCountComparisonTable{
		{SourceSchema: "s", SourceTable: "t", TargetSchema: "s", TargetTable: "t",
			SourceRows: &s100, TargetRows: &s100, Difference: &dZero, Matched: true},
	}
	ts.finalizeRowCountComparison("overwrite_cmp", oldNow, tables, results, "")

	loaded, ok := ts.GetTask("overwrite_cmp")
	require.True(t, ok)
	require.NotNil(t, loaded.Context.RowCountComparison)
	// 新结果应覆盖旧结果
	assert.Equal(t, taskEntity.RowCountComparisonMatched, loaded.Context.RowCountComparison.Status)
	assert.Equal(t, int64(100), loaded.Context.RowCountComparison.SourceTotal)
	assert.Equal(t, int64(100), loaded.Context.RowCountComparison.TargetTotal)
	assert.Equal(t, int64(0), loaded.Context.RowCountComparison.Difference)
	require.Len(t, loaded.Context.RowCountComparison.Tables, 1)
	assert.True(t, loaded.Context.RowCountComparison.Tables[0].Matched)
	// 任务终态不被修改
	assert.Equal(t, taskEntity.TaskStatusStopped, loaded.Context.Status)
}

// TestDeleteTask_CancelsComparison 验证删除正在核对的任务时取消并等待后台 goroutine 退出，
// 且结果不写回已删除任务。
func TestDeleteTask_CancelsComparison(t *testing.T) {
	ts := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()}})
	defer ts.Close()

	makeAllTaskAtIncremental(t, ts, "del_cmp")
	require.NoError(t, ts.EndTask("del_cmp"))

	// 启动核对（后台 goroutine 会因无真实 DB 很快失败）
	require.NoError(t, ts.StartRowCountComparison("del_cmp"))

	// 立即删除任务，应取消并等待 goroutine 退出而不死锁
	require.NoError(t, ts.DeleteTask("del_cmp"))

	// 任务应已被删除
	_, ok := ts.GetTask("del_cmp")
	assert.False(t, ok)
	// comparison cancel 映射应已清理（goroutine defer 在 cancel 后退出并 delete）
	ts.comparisonMu.Lock()
	_, hasCancel := ts.comparisonCancels["del_cmp"]
	_, hasWg := ts.comparisonWgs["del_cmp"]
	ts.comparisonMu.Unlock()
	assert.False(t, hasCancel, "comparison cancel should be cleaned up")
	assert.False(t, hasWg, "comparison wg should be cleaned up")
}

// TestClose_CancelsComparison 验证服务关闭时取消并等待核对 goroutine 退出。
func TestClose_CancelsComparison(t *testing.T) {
	ts := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()}})

	makeAllTaskAtIncremental(t, ts, "close_cmp")
	require.NoError(t, ts.EndTask("close_cmp"))
	require.NoError(t, ts.StartRowCountComparison("close_cmp"))

	// 关闭服务应正常返回，不因等待 goroutine 死锁
	require.NoError(t, ts.Close())
}

// TestGetTaskSnapshot_ConcurrentRowCountComparison 验证详情读取与后台核对进度写入无 data race。
func TestGetTaskSnapshot_ConcurrentRowCountComparison(t *testing.T) {
	ts := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()}})
	defer ts.Close()

	taskID := "snapshot_cmp"
	makeAllTaskAtIncremental(t, ts, taskID)
	require.NoError(t, ts.EndTask(taskID))

	startedAt := time.Now()
	tables := []comparisonTableTask{{sourceSchema: "src_db", sourceTable: "users", targetSchema: "tgt_db", targetTable: "users"}}
	srcRows := int64(1)
	tgtRows := int64(2)
	diff := int64(1)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			done := []taskEntity.RowCountComparisonTable{{
				SourceSchema: "src_db", SourceTable: "users", TargetSchema: "tgt_db", TargetTable: "users",
				SourceRows: &srcRows, TargetRows: &tgtRows, Difference: &diff, Matched: false,
			}}
			ts.persistRowCountComparisonProgress(taskID, startedAt, tables, done)
		}
	}()

	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				snap, ok := ts.GetTaskSnapshot(taskID)
				if !ok {
					continue
				}
				rc := snap.Context.RowCountComparison
				if rc == nil {
					continue
				}
				_ = rc.Status
				_ = rc.CheckedTables
				for _, tbl := range rc.Tables {
					if tbl.SourceRows != nil {
						_ = *tbl.SourceRows
					}
					if tbl.TargetRows != nil {
						_ = *tbl.TargetRows
					}
				}
			}
		}()
	}

	wg.Wait()
}
