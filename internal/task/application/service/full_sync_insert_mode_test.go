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
		"SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
	)).WithArgs(targetSchema, table).WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow(table))
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
	expectTargetWriteSessionWithBinlog(mock, insertSQL, false)
}

func expectTargetWriteSessionWithBinlog(mock sqlmock.Sqlmock, insertSQL string, skipBinlog bool) {
	mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableForeignKeyChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableUniqueChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(fullSyncVerifyChecksSQL)).
		WillReturnRows(sqlmock.NewRows([]string{"foreign_key_checks", "unique_checks"}).AddRow(0, 0))
	if skipBinlog {
		mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableBinlogSQL)).
			WillReturnResult(sqlmock.NewResult(0, 0))
	}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(insertSQL)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec(regexp.QuoteMeta(fullSyncRestoreForeignKeyChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(fullSyncRestoreUniqueChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	if skipBinlog {
		mock.ExpectExec(regexp.QuoteMeta(fullSyncRestoreBinlogSQL)).
			WillReturnResult(sqlmock.NewResult(0, 0))
	}
}

func expectParallelTargetWriteSessions(mock sqlmock.Sqlmock, insertSQL string, workers int) {
	expectParallelTargetWriteSessionsWithBinlog(mock, insertSQL, workers, false)
}

func expectParallelTargetWriteSessionsWithBinlog(mock sqlmock.Sqlmock, insertSQL string, workers int, skipBinlog bool) {
	for i := 0; i < workers; i++ {
		mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableForeignKeyChecksSQL)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableUniqueChecksSQL)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(regexp.QuoteMeta(fullSyncVerifyChecksSQL)).
			WillReturnRows(sqlmock.NewRows([]string{"foreign_key_checks", "unique_checks"}).AddRow(0, 0))
		if skipBinlog {
			mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableBinlogSQL)).
				WillReturnResult(sqlmock.NewResult(0, 0))
		}
		mock.ExpectExec(regexp.QuoteMeta(fullSyncLockWaitTimeoutSQL)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta(fullSyncRestoreForeignKeyChecksSQL)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta(fullSyncRestoreUniqueChecksSQL)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		if skipBinlog {
			mock.ExpectExec(regexp.QuoteMeta(fullSyncRestoreBinlogSQL)).
				WillReturnResult(sqlmock.NewResult(0, 0))
		}
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

	task := newFullSyncInsertModeTask(enableDrop, extra)

	insertSQL := "INSERT IGNORE INTO `" + fullSyncTgtSchema + "`.`" + tableName + "` (`id`, `name`) VALUES (?, ?)"
	if identity.Strategy == entity.FullColumnsStrategy {
		insertSQL = "INSERT IGNORE INTO `" + fullSyncTgtSchema + "`.`" + tableName + "` (`a`, `b`) VALUES (?, ?)"
	} else if identity.IdentifyCols[0] == "code" {
		insertSQL = "INSERT IGNORE INTO `" + fullSyncTgtSchema + "`.`" + tableName + "` (`code`, `payload`) VALUES (?, ?)"
	}
	// 全量同步统一使用普通 INSERT，目标端由用户保证为空或通过 DDL 前删除重建为空。
	insertSQL = insertPlainSQL(insertSQL)

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

	ts.tasks[task.Config.ID] = task

	runtime := &taskRuntime{
		sourceDB: sourceDB,
		targetDB: targetDB,
		analyzer: &fixedIdentityAnalyzer{identity: identity},
	}

	err = ts.syncDatabasePair(context.Background(), task, runtime, fullSyncSrcSchema, fullSyncTgtSchema, []string{tableName}, nil, false)
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

	mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableForeignKeyChecksSQL)).
		WillReturnError(errors.New("access denied"))

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	err = disableFullSyncWriteSession(context.Background(), conn, "tgt.child", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "disable foreign key checks")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDisableFullSyncWriteSession_FailsWhenVerifyStillEnabled(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableForeignKeyChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableUniqueChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(fullSyncVerifyChecksSQL)).
		WillReturnRows(sqlmock.NewRows([]string{"foreign_key_checks", "unique_checks"}).AddRow(1, 0))
	mock.ExpectExec(regexp.QuoteMeta(fullSyncRestoreForeignKeyChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(fullSyncRestoreUniqueChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	err = disableFullSyncWriteSession(context.Background(), conn, "tgt.child", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "FOREIGN_KEY_CHECKS=1")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDisableFullSyncWriteSession_AllowsUniqueChecksStillEnabled(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableForeignKeyChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableUniqueChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(fullSyncVerifyChecksSQL)).
		WillReturnRows(sqlmock.NewRows([]string{"foreign_key_checks", "unique_checks"}).AddRow(0, 1))

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	err = disableFullSyncWriteSession(context.Background(), conn, "tgt.child", false)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDisableFullSyncWriteSession_SkipBinlog(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableForeignKeyChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableUniqueChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(fullSyncVerifyChecksSQL)).
		WillReturnRows(sqlmock.NewRows([]string{"foreign_key_checks", "unique_checks"}).AddRow(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableBinlogSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(fullSyncRestoreForeignKeyChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(fullSyncRestoreUniqueChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(fullSyncRestoreBinlogSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	err = disableFullSyncWriteSession(context.Background(), conn, "tgt.child", true)
	require.NoError(t, err)
	restoreFullSyncWriteSession(conn, "tgt.child", true)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDisableFullSyncWriteSession_SkipBinlogFailsHard(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableForeignKeyChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableUniqueChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(fullSyncVerifyChecksSQL)).
		WillReturnRows(sqlmock.NewRows([]string{"foreign_key_checks", "unique_checks"}).AddRow(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableBinlogSQL)).
		WillReturnError(errors.New("access denied: SUPER privilege required"))
	mock.ExpectExec(regexp.QuoteMeta(fullSyncRestoreForeignKeyChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(fullSyncRestoreUniqueChecksSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	err = disableFullSyncWriteSession(context.Background(), conn, "tgt.child", true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "disable binlog")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRestoreFullSyncWriteSession_SkipBinlogAttemptsEveryRestore(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta(fullSyncRestoreForeignKeyChecksSQL)).
		WillReturnError(errors.New("restore foreign key checks failed"))
	mock.ExpectExec(regexp.QuoteMeta(fullSyncRestoreUniqueChecksSQL)).
		WillReturnError(errors.New("restore unique checks failed"))
	mock.ExpectExec(regexp.QuoteMeta(fullSyncRestoreBinlogSQL)).
		WillReturnError(errors.New("restore binlog failed"))

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	restoreFullSyncWriteSession(conn, "tgt.child", true)
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

	t.Run("plain_insert_when_drop_disabled", func(t *testing.T) {
		runSyncDatabasePairInsertModeTest(t, pkUsersIdentity(), "users", false, nil, setupSource, setupTarget)
	})
	t.Run("plain_insert_when_drop_enabled", func(t *testing.T) {
		runSyncDatabasePairInsertModeTest(t, pkUsersIdentity(), "users", true, nil, setupSource, setupTarget)
	})
	t.Run("plain_insert_when_table_level_drop_disabled", func(t *testing.T) {
		tableExtra := func(cfg *taskEntity.TaskConfig) {
			cfg.SyncLevel = taskEntity.SyncLevelTable
		}
		runSyncDatabasePairInsertModeTest(t, pkUsersIdentity(), "users", false, tableExtra, setupSource, setupTarget)
	})
	t.Run("skip_binlog_session", func(t *testing.T) {
		extra := func(cfg *taskEntity.TaskConfig) {
			cfg.EnableSkipBinlog = true
		}
		setupTargetWithBinlog := func(mock sqlmock.Sqlmock, insertSQL string) {
			expectTargetWriteSessionWithBinlog(mock, insertSQL, true)
		}
		runSyncDatabasePairInsertModeTest(t, pkUsersIdentity(), "users", false, extra, setupSource, setupTargetWithBinlog)
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

	t.Run("plain_insert_when_drop_disabled", func(t *testing.T) {
		runSyncDatabasePairInsertModeTest(t, nopkLogsIdentity(), "logs", false, nil, setupSource, setupTarget)
	})
	t.Run("plain_insert_when_drop_enabled", func(t *testing.T) {
		runSyncDatabasePairInsertModeTest(t, nopkLogsIdentity(), "logs", true, nil, setupSource, setupTarget)
	})
	t.Run("plain_insert_when_table_level_drop_disabled", func(t *testing.T) {
		tableExtra := func(cfg *taskEntity.TaskConfig) {
			cfg.SyncLevel = taskEntity.SyncLevelTable
		}
		runSyncDatabasePairInsertModeTest(t, nopkLogsIdentity(), "logs", false, tableExtra, setupSource, setupTarget)
	})
	t.Run("skip_binlog_session", func(t *testing.T) {
		extra := func(cfg *taskEntity.TaskConfig) {
			cfg.EnableSkipBinlog = true
		}
		setupTargetWithBinlog := func(mock sqlmock.Sqlmock, insertSQL string) {
			expectTargetWriteSessionWithBinlog(mock, insertSQL, true)
		}
		runSyncDatabasePairInsertModeTest(t, nopkLogsIdentity(), "logs", false, extra, setupSource, setupTargetWithBinlog)
	})
}

// TestSyncDatabasePair_ParallelRangePath_InsertMode 数值主键 range 并行路径（task_service.go ~3047）。
func TestSyncDatabasePair_ParallelRangePath_InsertMode(t *testing.T) {
	setupSource := func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SELECT COALESCE\\(MIN\\(`id`\\), 0\\), COALESCE\\(MAX\\(`id`\\), -1\\) FROM `" + fullSyncSrcSchema + "`.`users`").
			WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(int64(1), int64(1)))
		// minPK=maxPK=1, workers=2 → span=1 < 2 → 实际 worker=1 → 单 worker: start=nil, end=nil
		// ReadBatchByKeyRange(nil, nil, limit) 退化为 ReadBatchByKeys(nil, limit) → 无 WHERE
		rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(1), "alice")
		mock.ExpectQuery("SELECT `id`, `name` FROM `" + fullSyncSrcSchema + "`.`users` ORDER BY `id` ASC LIMIT \\?").
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

	t.Run("plain_insert_when_drop_disabled", func(t *testing.T) {
		runSyncDatabasePairInsertModeTest(t, pkUsersIdentity(), "users", false, extra, setupSource, setupTarget)
	})
	t.Run("plain_insert_when_drop_enabled", func(t *testing.T) {
		runSyncDatabasePairInsertModeTest(t, pkUsersIdentity(), "users", true, extra, setupSource, setupTarget)
	})
	t.Run("plain_insert_when_table_level_drop_disabled", func(t *testing.T) {
		tableExtra := func(cfg *taskEntity.TaskConfig) {
			cfg.IntraTableWorkerCount = 2
			cfg.WorkerCount = 2
			cfg.SyncLevel = taskEntity.SyncLevelTable
		}
		runSyncDatabasePairInsertModeTest(t, pkUsersIdentity(), "users", false, tableExtra, setupSource, setupTarget)
	})
	t.Run("skip_binlog_session", func(t *testing.T) {
		skipExtra := func(cfg *taskEntity.TaskConfig) {
			extra(cfg)
			cfg.EnableSkipBinlog = true
		}
		setupTargetWithBinlog := func(mock sqlmock.Sqlmock, insertSQL string) {
			expectParallelTargetWriteSessionsWithBinlog(mock, insertSQL, 1, true)
		}
		runSyncDatabasePairInsertModeTest(t, pkUsersIdentity(), "users", false, skipExtra, setupSource, setupTargetWithBinlog)
	})
}

// TestSyncDatabasePair_ParallelSamplePath_InsertMode 非数值主键 sample 并行路径（task_service.go ~3400）。
func TestSyncDatabasePair_ParallelSamplePath_InsertMode(t *testing.T) {
	setupSource := func(mock sqlmock.Sqlmock) {
		// 新版：information_schema.TABLE_ROWS 替代 COUNT(*)
		mock.ExpectQuery("SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = \\? AND TABLE_NAME = \\?").
			WithArgs(fullSyncSrcSchema, "events").
			WillReturnRows(sqlmock.NewRows([]string{"TABLE_ROWS"}).AddRow(int64(4))) // step = 4/2 = 2
		// 新版：keyset 步进取边界（仅取 PK 列）。
		// n=2 时循环仅跑 1 轮（i=0），无需 mock i=1；末位 = "mmm"，产出 1 个边界，2 个 worker。
		mock.ExpectQuery("^SELECT `code` FROM `" + fullSyncSrcSchema + "`.`events` ORDER BY `code` ASC LIMIT \\?$").
			WithArgs(int64(2)).
			WillReturnRows(sqlmock.NewRows([]string{"code"}).AddRow("aaa").AddRow("mmm"))

		rowsW0 := sqlmock.NewRows([]string{"code", "payload"}).AddRow("aaa", "p0").AddRow("mmm", "p0b")
		// w0 第一个 batch：startID=nil, endID="mmm" -> WHERE code <= ?（含边界行）
		mock.ExpectQuery("^SELECT `code`, `payload` FROM `" + fullSyncSrcSchema + "`.`events` WHERE `code` <= \\? ORDER BY `code` ASC LIMIT \\?$").
			WillReturnRows(rowsW0)
		// w0 第二个 batch：startID="mmm", endID="mmm" -> WHERE code > ? AND code <= ? -> 空
		mock.ExpectQuery("^SELECT `code`, `payload` FROM `" + fullSyncSrcSchema + "`.`events` WHERE `code` > \\? AND `code` <= \\? ORDER BY `code` ASC LIMIT \\?$").
			WillReturnRows(sqlmock.NewRows([]string{"code", "payload"}))

		// w1 第一个 batch：startID="mmm", endID=nil -> 退化为 WHERE code > ?
		mock.ExpectQuery("^SELECT `code`, `payload` FROM `" + fullSyncSrcSchema + "`.`events` WHERE `code` > \\? ORDER BY `code` ASC LIMIT \\?$").
			WillReturnRows(sqlmock.NewRows([]string{"code", "payload"}))
	}
	setupTarget := func(mock sqlmock.Sqlmock, insertSQL string) {
		expectParallelTargetWriteSessions(mock, insertSQL, 2)
	}
	extra := func(cfg *taskEntity.TaskConfig) {
		cfg.IntraTableWorkerCount = 2
		cfg.WorkerCount = 2
	}

	t.Run("plain_insert_when_drop_disabled", func(t *testing.T) {
		runSyncDatabasePairInsertModeTest(t, varcharPKIdentity(), "events", false, extra, setupSource, setupTarget)
	})
	t.Run("plain_insert_when_drop_enabled", func(t *testing.T) {
		runSyncDatabasePairInsertModeTest(t, varcharPKIdentity(), "events", true, extra, setupSource, setupTarget)
	})
	t.Run("plain_insert_when_table_level_drop_disabled", func(t *testing.T) {
		tableExtra := func(cfg *taskEntity.TaskConfig) {
			cfg.IntraTableWorkerCount = 2
			cfg.WorkerCount = 2
			cfg.SyncLevel = taskEntity.SyncLevelTable
		}
		runSyncDatabasePairInsertModeTest(t, varcharPKIdentity(), "events", false, tableExtra, setupSource, setupTarget)
	})
	t.Run("skip_binlog_session", func(t *testing.T) {
		skipExtra := func(cfg *taskEntity.TaskConfig) {
			extra(cfg)
			cfg.EnableSkipBinlog = true
		}
		setupTargetWithBinlog := func(mock sqlmock.Sqlmock, insertSQL string) {
			expectParallelTargetWriteSessionsWithBinlog(mock, insertSQL, 2, true)
		}
		runSyncDatabasePairInsertModeTest(t, varcharPKIdentity(), "events", false, skipExtra, setupSource, setupTargetWithBinlog)
	})
}
