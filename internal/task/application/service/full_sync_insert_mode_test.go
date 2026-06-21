package service

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"mysql-to-sync/internal/metadata/domain/entity"
	taskEntity "mysql-to-sync/internal/task/domain/entity"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

const (
	fullSyncSrcSchema = "src_db"
	fullSyncTgtSchema = "tgt_db"
)

// fixedIdentityAnalyzer 返回固定表身份，用于全量写入路径 sqlmock 集成测试。
type fixedIdentityAnalyzer struct {
	identity *entity.TableIdentity
}

func (a *fixedIdentityAnalyzer) AnalyzeTable(_, tableName string) (*entity.TableIdentity, error) {
	if a.identity == nil {
		return nil, nil
	}
	clone := *a.identity
	clone.TableName = tableName
	return &clone, nil
}

func (a *fixedIdentityAnalyzer) GetAllTables(string) ([]entity.TableInfo, error) {
	return nil, nil
}

func (a *fixedIdentityAnalyzer) GetAllDatabases() ([]string, error) {
	return nil, nil
}

func pkUsersIdentity() *entity.TableIdentity {
	return &entity.TableIdentity{
		TableName:    "users",
		Strategy:     entity.PKStrategy,
		HasPK:        true,
		IdentifyCols: []string{"id"},
		Columns: []entity.ColumnMeta{
			{Name: "id", DataType: "bigint", IsPrimaryKey: true},
			{Name: "name", DataType: "varchar"},
		},
	}
}

func varcharPKIdentity() *entity.TableIdentity {
	return &entity.TableIdentity{
		TableName:    "events",
		Strategy:     entity.PKStrategy,
		HasPK:        true,
		IdentifyCols: []string{"code"},
		Columns: []entity.ColumnMeta{
			{Name: "code", DataType: "varchar", IsPrimaryKey: true},
			{Name: "payload", DataType: "text"},
		},
	}
}

func nopkLogsIdentity() *entity.TableIdentity {
	return &entity.TableIdentity{
		TableName:    "logs",
		Strategy:     entity.FullColumnsStrategy,
		IdentifyCols: []string{"a", "b"},
		Columns: []entity.ColumnMeta{
			{Name: "a", DataType: "varchar"},
			{Name: "b", DataType: "int"},
		},
	}
}

func expectTargetTableAlreadyExists(mock sqlmock.Sqlmock, targetSchema, table string) {
	mock.ExpectQuery("SELECT schema_name FROM information_schema.schemata WHERE schema_name = \\?").
		WithArgs(targetSchema).
		WillReturnRows(sqlmock.NewRows([]string{"schema_name"}).AddRow(targetSchema))
	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT table_name FROM information_schema.tables WHERE table_schema = '" + targetSchema + "' AND table_name = '" + table + "'",
	)).WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow(table))
}

func expectTargetTableRecreateOnDrop(mock sqlmock.Sqlmock, targetSchema, table string) {
	expectTargetTableAlreadyExists(mock, targetSchema, table)
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=0").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("DROP TABLE IF EXISTS `" + targetSchema + "`.`" + table + "`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE `" + targetSchema + "`.`" + table + "` LIKE `" + fullSyncSrcSchema + "`.`" + table + "`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=1").
		WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectTargetWriteSession(mock sqlmock.Sqlmock, insertSQL string) {
	mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(fullSyncVerifyChecksSQL)).
		WillReturnRows(sqlmock.NewRows([]string{"foreign_key_checks", "unique_checks"}).AddRow(0, 0))
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(insertSQL)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec(regexp.QuoteMeta(fullSyncRestoreChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectParallelTargetWriteSessions(mock sqlmock.Sqlmock, insertSQL string, workers int) {
	for i := 0; i < workers; i++ {
		mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableChecksSQL)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(regexp.QuoteMeta(fullSyncVerifyChecksSQL)).
			WillReturnRows(sqlmock.NewRows([]string{"foreign_key_checks", "unique_checks"}).AddRow(0, 0))
		mock.ExpectExec(regexp.QuoteMeta(fullSyncLockWaitTimeoutSQL)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta(fullSyncRestoreChecksSQL)).
			WillReturnResult(sqlmock.NewResult(0, 0))
	}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(insertSQL)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
}

func newFullSyncInsertModeTask(enableDrop bool, extra func(*taskEntity.TaskConfig)) *taskEntity.SyncTask {
	cfg := taskEntity.TaskConfig{
		ID:                       "full_sync_insert_mode",
		Name:                     "Full Sync Insert Mode",
		Mode:                     taskEntity.SyncModeFull,
		SourceSchema:             fullSyncSrcSchema,
		TargetSchema:             fullSyncTgtSchema,
		BatchSize:                100,
		WorkerCount:              1,
		EnableDropTableBeforeDDL: enableDrop,
	}
	if extra != nil {
		extra(&cfg)
	}
	task := taskEntity.NewSyncTask(cfg)
	task.Start()
	return task
}

func runSyncDatabasePairInsertModeTest(
	t *testing.T,
	identity *entity.TableIdentity,
	tableName string,
	enableDrop bool,
	extra func(*taskEntity.TaskConfig),
	setupSource func(sqlmock.Sqlmock),
	setupTarget func(sqlmock.Sqlmock, string),
) {
	t.Helper()

	insertSQL := "INSERT IGNORE INTO `" + fullSyncTgtSchema + "`.`" + tableName + "` (`id`, `name`) VALUES (?, ?)"
	if identity.Strategy == entity.FullColumnsStrategy {
		insertSQL = "INSERT IGNORE INTO `" + fullSyncTgtSchema + "`.`" + tableName + "` (`a`, `b`) VALUES (?, ?)"
	} else if identity.IdentifyCols[0] == "code" {
		insertSQL = "INSERT IGNORE INTO `" + fullSyncTgtSchema + "`.`" + tableName + "` (`code`, `payload`) VALUES (?, ?)"
	}
	if enableDrop {
		insertSQL = insertPlainSQL(insertSQL)
	}

	sourceDB, sourceMock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer sourceDB.Close()
	sourceMock.MatchExpectationsInOrder(false)

	targetDB, targetMock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer targetDB.Close()
	targetMock.MatchExpectationsInOrder(false)

	setupSource(sourceMock)
	if enableDrop {
		expectTargetTableRecreateOnDrop(targetMock, fullSyncTgtSchema, tableName)
	} else {
		expectTargetTableAlreadyExists(targetMock, fullSyncTgtSchema, tableName)
	}
	setupTarget(targetMock, insertSQL)

	ts := NewTaskServiceWithDB(sourceDB, targetDB, &fixedIdentityAnalyzer{identity: identity})
	ts.SetEnableReadOnly(false)

	task := newFullSyncInsertModeTask(enableDrop, extra)
	ts.tasks[task.Config.ID] = task

	runtime := &taskRuntime{
		sourceDB: sourceDB,
		targetDB: targetDB,
		analyzer: &fixedIdentityAnalyzer{identity: identity},
	}

	err = ts.syncDatabasePair(context.Background(), task, runtime, fullSyncSrcSchema, fullSyncTgtSchema, []string{tableName})
	require.NoError(t, err)
	require.NoError(t, sourceMock.ExpectationsWereMet())
	require.NoError(t, targetMock.ExpectationsWereMet())
}

func insertPlainSQL(ignoreSQL string) string {
	return regexp.MustCompile(`INSERT IGNORE `).ReplaceAllString(ignoreSQL, "INSERT ")
}

func TestDisableFullSyncWriteSession_FailsWhenSetFails(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableChecksSQL)).
		WillReturnError(errors.New("access denied"))

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	err = disableFullSyncWriteSession(context.Background(), conn, "tgt.child")
	require.Error(t, err)
	require.Contains(t, err.Error(), "disable foreign key and unique checks")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDisableFullSyncWriteSession_FailsWhenVerifyStillEnabled(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(fullSyncVerifyChecksSQL)).
		WillReturnRows(sqlmock.NewRows([]string{"foreign_key_checks", "unique_checks"}).AddRow(1, 0))

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	err = disableFullSyncWriteSession(context.Background(), conn, "tgt.child")
	require.Error(t, err)
	require.Contains(t, err.Error(), "FOREIGN_KEY_CHECKS=1")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestSyncDatabasePair_KeysetPath_InsertMode 单 worker keyset 回退路径（task_service.go ~3666）。
func TestSyncDatabasePair_KeysetPath_InsertMode(t *testing.T) {
	setupSource := func(mock sqlmock.Sqlmock) {
		rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(1), "alice")
		mock.ExpectQuery("SELECT `id`, `name` FROM `" + fullSyncSrcSchema + "`.`users` ORDER BY `id` ASC LIMIT \\?").
			WillReturnRows(rows)
		mock.ExpectQuery("SELECT `id`, `name` FROM `" + fullSyncSrcSchema + "`.`users` WHERE `id` > \\? ORDER BY `id` ASC LIMIT \\?").
			WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))
	}
	setupTarget := func(mock sqlmock.Sqlmock, insertSQL string) {
		expectTargetWriteSession(mock, insertSQL)
	}

	t.Run("insert_ignore_when_drop_disabled", func(t *testing.T) {
		runSyncDatabasePairInsertModeTest(t, pkUsersIdentity(), "users", false, nil, setupSource, setupTarget)
	})
	t.Run("plain_insert_when_drop_enabled", func(t *testing.T) {
		runSyncDatabasePairInsertModeTest(t, pkUsersIdentity(), "users", true, nil, setupSource, setupTarget)
	})
}

// TestSyncDatabasePair_NoPKPath_InsertMode 无主键流式路径（task_service.go ~2768）。
func TestSyncDatabasePair_NoPKPath_InsertMode(t *testing.T) {
	setupSource := func(mock sqlmock.Sqlmock) {
		rows := sqlmock.NewRows([]string{"a", "b"}).AddRow("x", 1)
		mock.ExpectQuery("SELECT `a`, `b` FROM `" + fullSyncSrcSchema + "`.`logs`").
			WillReturnRows(rows)
	}
	setupTarget := func(mock sqlmock.Sqlmock, insertSQL string) {
		expectTargetWriteSession(mock, insertSQL)
	}

	t.Run("insert_ignore_when_drop_disabled", func(t *testing.T) {
		runSyncDatabasePairInsertModeTest(t, nopkLogsIdentity(), "logs", false, nil, setupSource, setupTarget)
	})
	t.Run("plain_insert_when_drop_enabled", func(t *testing.T) {
		runSyncDatabasePairInsertModeTest(t, nopkLogsIdentity(), "logs", true, nil, setupSource, setupTarget)
	})
}

// TestSyncDatabasePair_ParallelRangePath_InsertMode 数值主键 range 并行路径（task_service.go ~3047）。
func TestSyncDatabasePair_ParallelRangePath_InsertMode(t *testing.T) {
	setupSource := func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SELECT COALESCE\\(MIN\\(`id`\\), 0\\), COALESCE\\(MAX\\(`id`\\), -1\\) FROM `" + fullSyncSrcSchema + "`.`users`").
			WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(int64(1), int64(1)))
		rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(1), "alice")
		mock.ExpectQuery("SELECT `id`, `name` FROM `" + fullSyncSrcSchema + "`.`users` WHERE `id` > \\? ORDER BY `id` ASC LIMIT \\?").
			WillReturnRows(rows)
		mock.ExpectQuery("SELECT `id`, `name` FROM `" + fullSyncSrcSchema + "`.`users` WHERE `id` > \\? ORDER BY `id` ASC LIMIT \\?").
			WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))
	}
	setupTarget := func(mock sqlmock.Sqlmock, insertSQL string) {
		expectParallelTargetWriteSessions(mock, insertSQL, 1)
	}
	extra := func(cfg *taskEntity.TaskConfig) {
		cfg.IntraTableWorkerCount = 2
		cfg.WorkerCount = 2
	}

	t.Run("insert_ignore_when_drop_disabled", func(t *testing.T) {
		runSyncDatabasePairInsertModeTest(t, pkUsersIdentity(), "users", false, extra, setupSource, setupTarget)
	})
	t.Run("plain_insert_when_drop_enabled", func(t *testing.T) {
		runSyncDatabasePairInsertModeTest(t, pkUsersIdentity(), "users", true, extra, setupSource, setupTarget)
	})
}

// TestSyncDatabasePair_ParallelSamplePath_InsertMode 非数值主键 sample 并行路径（task_service.go ~3400）。
func TestSyncDatabasePair_ParallelSamplePath_InsertMode(t *testing.T) {
	setupSource := func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `" + fullSyncSrcSchema + "`.`events`").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(100)))
		mock.ExpectQuery("SELECT `code` FROM `" + fullSyncSrcSchema + "`.`events` ORDER BY `code` LIMIT 1 OFFSET \\?").
			WithArgs(int64(0)).
			WillReturnRows(sqlmock.NewRows([]string{"code"}).AddRow("aaa"))
		mock.ExpectQuery("SELECT `code` FROM `" + fullSyncSrcSchema + "`.`events` ORDER BY `code` LIMIT 1 OFFSET \\?").
			WithArgs(int64(99)).
			WillReturnRows(sqlmock.NewRows([]string{"code"}).AddRow("zzz"))

		rowsW0 := sqlmock.NewRows([]string{"code", "payload"}).AddRow("aaa", "p0")
		mock.ExpectQuery("SELECT `code`, `payload` FROM `" + fullSyncSrcSchema + "`.`events` ORDER BY `code` ASC LIMIT \\?").
			WillReturnRows(rowsW0)
		mock.ExpectQuery("SELECT `code`, `payload` FROM `" + fullSyncSrcSchema + "`.`events` WHERE `code` > \\? ORDER BY `code` ASC LIMIT \\?").
			WillReturnRows(sqlmock.NewRows([]string{"code", "payload"}))

		mock.ExpectQuery("SELECT `code`, `payload` FROM `" + fullSyncSrcSchema + "`.`events` WHERE `code` > \\? ORDER BY `code` ASC LIMIT \\?").
			WillReturnRows(sqlmock.NewRows([]string{"code", "payload"}))
	}
	setupTarget := func(mock sqlmock.Sqlmock, insertSQL string) {
		expectParallelTargetWriteSessions(mock, insertSQL, 2)
	}
	extra := func(cfg *taskEntity.TaskConfig) {
		cfg.IntraTableWorkerCount = 2
		cfg.WorkerCount = 2
	}

	t.Run("insert_ignore_when_drop_disabled", func(t *testing.T) {
		runSyncDatabasePairInsertModeTest(t, varcharPKIdentity(), "events", false, extra, setupSource, setupTarget)
	})
	t.Run("plain_insert_when_drop_enabled", func(t *testing.T) {
		runSyncDatabasePairInsertModeTest(t, varcharPKIdentity(), "events", true, extra, setupSource, setupTarget)
	})
}
