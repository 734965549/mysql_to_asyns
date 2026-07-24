package service

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"mysql-to-sync/internal/sync/fullload"
	taskEntity "mysql-to-sync/internal/task/domain/entity"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

// expectTargetTableMissingCreateLike ??"????????????"????
// ???????????????? CREATE TABLE ... LIKE ?????? DROP TABLE?
func expectTargetTableMissingCreateLike(mock sqlmock.Sqlmock, targetSchema, table string) {
	mock.ExpectQuery("SELECT schema_name FROM information_schema.schemata WHERE schema_name = \\?").
		WithArgs(targetSchema).
		WillReturnRows(sqlmock.NewRows([]string{"schema_name"}).AddRow(targetSchema))
	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
	)).WithArgs(targetSchema, table).WillReturnRows(sqlmock.NewRows([]string{"table_name"})) // ?? ? ????
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=0").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE `" + targetSchema + "`.`" + table + "` LIKE `" + fullSyncSrcSchema + "`.`" + table + "`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=1").
		WillReturnResult(sqlmock.NewResult(0, 0))
}

// TestRebuildTargetDatabases_RebuildsEachUniqueDatabase ????????
// ???????? DROP DATABASE + CREATE DATABASE??????? DROP TABLE?
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

	require.NoError(t, ts.rebuildTargetDatabases(context.Background(), runtime, "task-1", []string{"tgt_a"}))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRebuildTargetDatabases_DedupesDuplicateTargets ?????????
// ???????????????????????
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

	// tgt_shared ????????????tgt_b ????
	mock.ExpectExec(regexp.QuoteMeta("DROP DATABASE IF EXISTS `tgt_shared`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("CREATE DATABASE IF NOT EXISTS `tgt_shared` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("DROP DATABASE IF EXISTS `tgt_b`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("CREATE DATABASE IF NOT EXISTS `tgt_b` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	require.NoError(t, ts.rebuildTargetDatabases(context.Background(), runtime, "task-1", []string{"tgt_shared", "tgt_b", "tgt_shared"}))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRebuildTargetDatabases_DropFailsReturnsError ?? DROP DATABASE ????????????
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

	err = ts.rebuildTargetDatabases(context.Background(), runtime, "task-1", []string{"tgt_a"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to drop target database tgt_a")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRebuildTargetDatabases_CreateFailsReturnsError ?? CREATE DATABASE ????????????
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

	err = ts.rebuildTargetDatabases(context.Background(), runtime, "task-1", []string{"tgt_a"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to recreate target database tgt_a")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRebuildTargetDatabases_EmptySchemasSkipped ?????????????????
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

	// ??????????
	require.NoError(t, ts.rebuildTargetDatabases(context.Background(), runtime, "task-1", []string{"", "  ", ""}))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRebuildTargetDatabases_RespectsStopSignal ??????????????????
// ??????????????? errFullSyncStoppedByUser?
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

	// ???????????? DROP/CREATE
	err = ts.rebuildTargetDatabases(context.Background(), runtime, "task-1", []string{"tgt_a", "tgt_b"})
	require.ErrorIs(t, err, errFullSyncStoppedByUser)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRebuildTargetDatabases_RespectsSchemaLockLost ???? cause ?????? DDL?
func TestRebuildTargetDatabases_RespectsSchemaLockLost(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()

	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"task-1": {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusRunning}},
		},
	}
	runtime := &taskRuntime{targetDB: db}

	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(fullload.ErrSchemaLockLost)

	err = ts.rebuildTargetDatabases(ctx, runtime, "task-1", []string{"tgt_a", "tgt_b"})
	require.ErrorIs(t, err, fullload.ErrSchemaLockLost)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRebuildTargetDatabases_LockLostDuringDropPreservesCause ?? DROP DATABASE
// ??????????????? ErrSchemaLockLost???? %v ?????????
func TestRebuildTargetDatabases_LockLostDuringDropPreservesCause(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()

	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"task-1": {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusRunning}},
		},
	}
	runtime := &taskRuntime{targetDB: db}

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	mock.ExpectExec(regexp.QuoteMeta("DROP DATABASE IF EXISTS `tgt_a`")).
		WillDelayFor(200 * time.Millisecond).
		WillReturnResult(sqlmock.NewResult(0, 0))

	errCh := make(chan error, 1)
	go func() {
		errCh <- ts.rebuildTargetDatabases(ctx, runtime, "task-1", []string{"tgt_a"})
	}()

	time.Sleep(50 * time.Millisecond)
	cancel(fullload.ErrSchemaLockLost)

	select {
	case err = <-errCh:
	case <-time.After(3 * time.Second):
		t.Fatal("rebuildTargetDatabases did not return after lock-lost cancel")
	}
	require.ErrorIs(t, err, fullload.ErrSchemaLockLost)
}

// TestAbortFullSyncIfCancelled_PrefersLockLostOverStop ????????????? cause?
func TestAbortFullSyncIfCancelled_PrefersLockLostOverStop(t *testing.T) {
	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"task-1": {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusPaused}},
		},
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(fullload.ErrSchemaLockLost)

	err := ts.abortFullSyncIfCancelled(ctx, "task-1")
	require.ErrorIs(t, err, fullload.ErrSchemaLockLost)
}

// TestSyncDatabasePair_DatabaseLevelRebuilt_NoDropTable ?????????
// ensureTargetTable ???? DROP TABLE??? CREATE TABLE LIKE???????? INSERT?
func TestSyncDatabasePair_DatabaseLevelRebuilt_NoDropTable(t *testing.T) {
	identity := pkUsersIdentity()
	tableName := "users"

	// ?????????? ? ??INSERT?? DROP TABLE
	insertSQL := insertPlainSQL("INSERT IGNORE INTO `" + fullSyncTgtSchema + "`.`" + tableName + "` (`id`, `name`) VALUES (?, ?)")

	sourceDB, sourceMock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer sourceDB.Close()
	sourceMock.MatchExpectationsInOrder(false)

	targetDB, targetMock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer targetDB.Close()
	targetMock.MatchExpectationsInOrder(false)

	// ??keyset ???????
	sourceMock.ExpectQuery("SELECT `id`, `name` FROM `" + fullSyncSrcSchema + "`.`" + tableName + "` ORDER BY `id` ASC LIMIT \\?").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(1), "alice"))
	sourceMock.ExpectQuery("SELECT `id`, `name` FROM `" + fullSyncSrcSchema + "`.`" + tableName + "` WHERE `id` > \\? ORDER BY `id` ASC LIMIT \\?").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))

	// ????????????????????????????? DROP TABLE
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
		EnableDropTableBeforeDDL: true, // ???????????
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
