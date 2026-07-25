package service

import (
	"context"
	"testing"

	"mysql-to-sync/internal/sync/fullload"
	taskEntity "mysql-to-sync/internal/task/domain/entity"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
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

// TestSyncDatabasePairV2_SkipsPublishedTableBeforeDDL 验证 resume 契约：
// Phase=PUBLISHED 的表必须在任何 DDL（含 DROP TABLE）之前被跳过。
// 见 syncDatabasePairV2 阶段1 的 PUBLISHED 检查（full_load_v2.go L98-102）。
// 若该跳过失效，enable_drop_table_before_ddl=true 会重建并清空已发布的目标表。
func TestSyncDatabasePairV2_SkipsPublishedTableBeforeDDL(t *testing.T) {
	cases := []struct {
		name           string
		dbLevelRebuilt bool
	}{
		{"dropEnabled", false},   // effectiveDropBeforeDDL=true：若不跳过会触发 DROP
		{"dbLevelRebuilt", true}, // 整库重建：effectiveDropBeforeDDL=false，但 PUBLISHED 仍应跳过
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := NewTaskService(newDefaultConfig())
			defer ts.Close()

			task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
				ID:                       "resume-published-" + tc.name,
				Mode:                     taskEntity.SyncModeAll,
				EnableDropTableBeforeDDL: true,
			})
			// syncDatabasePairV2 在阶段边界调 abortFullSyncIfCancelled -> isTaskStopped：
			// task 必须存在于 s.tasks 且 Status=Running，否则被判为 stopped。
			task.Context.Status = taskEntity.TaskStatusRunning
			task.SetFullLoadV2TableState("src.t1", &taskEntity.FullLoadV2TableState{
				Phase:     "PUBLISHED",
				AttemptID: 1,
			})
			ts.tasks[task.Config.ID] = task

			sourceDB, _, _ := sqlmock.New()
			targetDB, tgtMock, _ := sqlmock.New()
			defer sourceDB.Close()
			defer targetDB.Close()

			runtime := &taskRuntime{
				sourceDB: sourceDB,
				targetDB: targetDB,
				analyzer: &mockAnalyzer{}, // 不应被调用：t1 在 AnalyzeTable 之前被跳过
			}

			var pending []pendingIndexRestore
			err := ts.syncDatabasePairV2(
				context.Background(), task, runtime,
				"src", "tgt", []string{"t1"},
				&pending, tc.dbLevelRebuilt, nil,
			)

			assert.NoError(t, err)
			assert.Empty(t, pending, "no pending index restores for skipped table")
			// 目标库不应收到任何查询：PUBLISHED 表在 ensureTargetTable 之前被跳过
			if err := tgtMock.ExpectationsWereMet(); err != nil {
				t.Fatalf("target DB received unexpected query (DROP leaked?): %v", err)
			}
		})
	}
}

// TestCollectStaleStagingTableRefsFromTask 验证生产恢复路径按持久化 StagingTable 精确收集残留表。
func TestCollectStaleStagingTableRefsFromTask(t *testing.T) {
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:             "stg-cleanup",
		FullLoadEngine: "v2",
		SourceSchema:   "src",
		TargetSchema:   "tgt",
	})
	task.SetFullLoadV2TableState("src.orders", &taskEntity.FullLoadV2TableState{
		Phase:        "COPYING",
		AttemptID:    3,
		StagingTable: "__mts_staging_orders_3",
	})
	task.SetFullLoadV2TableState("src.done", &taskEntity.FullLoadV2TableState{
		Phase:        "PUBLISHED",
		AttemptID:    1,
		StagingTable: "__mts_staging_done_1", // PUBLISHED 不应清理
	})
	task.SetFullLoadV2TableState("src.empty", &taskEntity.FullLoadV2TableState{
		Phase:     "FAILED",
		AttemptID: 2,
		// StagingTable 空：跳过
	})

	refs := collectStaleStagingTableRefsFromTask(task)
	assert.Len(t, refs, 1)
	assert.Equal(t, "tgt", refs[0].Schema)
	assert.Equal(t, "__mts_staging_orders_3", refs[0].Table)
}

// TestCleanupStaleStagingTablesForTask_DropsExactRefs 验证任务建连后按清单 DROP 残留 staging。
func TestCleanupStaleStagingTablesForTask_DropsExactRefs(t *testing.T) {
	ts := NewTaskService(newDefaultConfig())
	defer ts.Close()

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:             "stg-drop",
		FullLoadEngine: "v2",
		SourceSchema:   "src",
		TargetSchema:   "tgt",
	})
	task.SetFullLoadV2TableState("src.t1", &taskEntity.FullLoadV2TableState{
		Phase:        "FAILED",
		AttemptID:    2,
		StagingTable: "__mts_staging_t1_2",
	})

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT 1 FROM information_schema.TABLES").
		WithArgs("tgt", "__mts_staging_t1_2").
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectExec("DROP TABLE IF EXISTS .*__mts_staging_t1_2.*").
		WillReturnResult(sqlmock.NewResult(0, 0))

	ts.cleanupStaleStagingTablesForTask(context.Background(), db, task)
	assert.NoError(t, mock.ExpectationsWereMet())
}
