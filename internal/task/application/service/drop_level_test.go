package service

import (
	"context"
	"errors"
	"regexp"
	"testing"

	taskEntity "mysql-to-sync/internal/task/domain/entity"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

// expectTargetTableMissingCreateLike 模拟"库级别重建后目标表不存在"的场景：
// 目标库已存在、目标表不存在，直接 CREATE TABLE ... LIKE 创建，不执行 DROP TABLE。
func expectTargetTableMissingCreateLike(mock sqlmock.Sqlmock, targetSchema, table string) {
	mock.ExpectQuery("SELECT schema_name FROM information_schema.schemata WHERE schema_name = \\?").
		WithArgs(targetSchema).
		WillReturnRows(sqlmock.NewRows([]string{"schema_name"}).AddRow(targetSchema))
	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT table_name FROM information_schema.tables WHERE table_schema = '" + targetSchema + "' AND table_name = '" + table + "'",
	)).WillReturnRows(sqlmock.NewRows([]string{"table_name"})) // 无行 → 表不存在
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=0").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE `" + targetSchema + "`.`" + table + "` LIKE `" + fullSyncSrcSchema + "`.`" + table + "`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=1").
		WillReturnResult(sqlmock.NewResult(0, 0))
}

// TestRebuildTargetDatabases_RebuildsEachUniqueDatabase 验证库级别重建：
// 对每个目标库执行 DROP DATABASE + CREATE DATABASE，且不执行任何 DROP TABLE。
func TestRebuildTargetDatabases_RebuildsEachUniqueDatabase(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()

	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"task-1": {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusRunning}},
		},
	}
	runtime := &taskRuntime{targetDB: db}

	mock.ExpectExec(regexp.QuoteMeta("DROP DATABASE IF EXISTS `tgt_a`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("CREATE DATABASE IF NOT EXISTS `tgt_a` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	require.NoError(t, ts.rebuildTargetDatabases(runtime, "task-1", []string{"tgt_a"}))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRebuildTargetDatabases_DedupesDuplicateTargets 验证多库映射去重：
// 两个源库映射到同一目标库时，目标库只重建一次。
func TestRebuildTargetDatabases_DedupesDuplicateTargets(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()

	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"task-1": {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusRunning}},
		},
	}
	runtime := &taskRuntime{targetDB: db}

	// tgt_shared 出现两次，应只重建一次；tgt_b 重建一次
	mock.ExpectExec(regexp.QuoteMeta("DROP DATABASE IF EXISTS `tgt_shared`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("CREATE DATABASE IF NOT EXISTS `tgt_shared` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("DROP DATABASE IF EXISTS `tgt_b`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("CREATE DATABASE IF NOT EXISTS `tgt_b` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	require.NoError(t, ts.rebuildTargetDatabases(runtime, "task-1", []string{"tgt_shared", "tgt_b", "tgt_shared"}))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRebuildTargetDatabases_DropFailsReturnsError 验证 DROP DATABASE 失败立即终止并返回错误。
func TestRebuildTargetDatabases_DropFailsReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()

	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"task-1": {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusRunning}},
		},
	}
	runtime := &taskRuntime{targetDB: db}

	mock.ExpectExec(regexp.QuoteMeta("DROP DATABASE IF EXISTS `tgt_a`")).
		WillReturnError(errors.New("drop database denied"))

	err = ts.rebuildTargetDatabases(runtime, "task-1", []string{"tgt_a"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to drop target database tgt_a")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRebuildTargetDatabases_CreateFailsReturnsError 验证 CREATE DATABASE 失败立即终止并返回错误。
func TestRebuildTargetDatabases_CreateFailsReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()

	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"task-1": {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusRunning}},
		},
	}
	runtime := &taskRuntime{targetDB: db}

	mock.ExpectExec(regexp.QuoteMeta("DROP DATABASE IF EXISTS `tgt_a`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("CREATE DATABASE IF NOT EXISTS `tgt_a` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")).
		WillReturnError(errors.New("create database denied"))

	err = ts.rebuildTargetDatabases(runtime, "task-1", []string{"tgt_a"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to recreate target database tgt_a")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRebuildTargetDatabases_EmptySchemasSkipped 验证空库名被跳过、不执行任何语句。
func TestRebuildTargetDatabases_EmptySchemasSkipped(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()

	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"task-1": {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusRunning}},
		},
	}
	runtime := &taskRuntime{targetDB: db}

	// 全部为空，无任何期望
	require.NoError(t, ts.rebuildTargetDatabases(runtime, "task-1", []string{"", "  ", ""}))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRebuildTargetDatabases_RespectsStopSignal 验证库级别重建过程中收到停止信号时，
// 不应继续删除后续目标库，并返回 errFullSyncStoppedByUser。
func TestRebuildTargetDatabases_RespectsStopSignal(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()

	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"task-1": {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusPaused}},
		},
	}
	runtime := &taskRuntime{targetDB: db}

	// 任务已暂停，不应执行任何 DROP/CREATE
	err = ts.rebuildTargetDatabases(runtime, "task-1", []string{"tgt_a", "tgt_b"})
	require.ErrorIs(t, err, errFullSyncStoppedByUser)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestSyncDatabasePair_DatabaseLevelRebuilt_NoDropTable 验证库级别重建后，
// ensureTargetTable 不再执行 DROP TABLE，直接 CREATE TABLE LIKE；写入仍使用普通 INSERT。
func TestSyncDatabasePair_DatabaseLevelRebuilt_NoDropTable(t *testing.T) {
	identity := pkUsersIdentity()
	tableName := "users"

	// 开启删除但库级已重建 → 普通INSERT、无 DROP TABLE
	insertSQL := insertPlainSQL("INSERT IGNORE INTO `" + fullSyncTgtSchema + "`.`" + tableName + "` (`id`, `name`) VALUES (?, ?)")

	sourceDB, sourceMock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer sourceDB.Close()
	sourceMock.MatchExpectationsInOrder(false)

	targetDB, targetMock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer targetDB.Close()
	targetMock.MatchExpectationsInOrder(false)

	// 源：keyset 读取一行后结束
	sourceMock.ExpectQuery("SELECT `id`, `name` FROM `" + fullSyncSrcSchema + "`.`" + tableName + "` ORDER BY `id` ASC LIMIT \\?").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(1), "alice"))
	sourceMock.ExpectQuery("SELECT `id`, `name` FROM `" + fullSyncSrcSchema + "`.`" + tableName + "` WHERE `id` > \\? ORDER BY `id` ASC LIMIT \\?").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))

	// 目标：库已存在、表不存在（库级重建后的状态），直接建表，无 DROP TABLE
	expectTargetTableMissingCreateLike(targetMock, fullSyncTgtSchema, tableName)
	expectTargetWriteSession(targetMock, insertSQL)

	ts := NewTaskServiceWithDB(sourceDB, targetDB, &fixedIdentityAnalyzer{identity: identity})
	ts.SetEnableReadOnly(false)

	cfg := taskEntity.TaskConfig{
		ID:                       "db_level_rebuilt",
		Name:                     "DB Level Rebuilt",
		Mode:                     taskEntity.SyncModeFull,
		SourceSchema:             fullSyncSrcSchema,
		TargetSchema:             fullSyncTgtSchema,
		BatchSize:                100,
		WorkerCount:              1,
		EnableDropTableBeforeDDL: true, // 开启删除，但库级已重建
	}
	task := taskEntity.NewSyncTask(cfg)
	task.Start()
	ts.tasks[task.Config.ID] = task

	runtime := &taskRuntime{
		sourceDB: sourceDB,
		targetDB: targetDB,
		analyzer: &fixedIdentityAnalyzer{identity: identity},
	}

	err = ts.syncDatabasePair(context.Background(), task, runtime, fullSyncSrcSchema, fullSyncTgtSchema, []string{tableName}, nil, true)
	require.NoError(t, err)
	require.NoError(t, sourceMock.ExpectationsWereMet())
	require.NoError(t, targetMock.ExpectationsWereMet())
}
