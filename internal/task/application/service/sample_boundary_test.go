package service

import (
	"context"
	"regexp"
	"testing"

	"mysql-to-sync/internal/metadata/domain/entity"
	taskEntity "mysql-to-sync/internal/task/domain/entity"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// compositePKIdentity 复合主键（int + varchar）表身份，用于 sample 分片边界测试。
func compositePKIdentity() *entity.TableIdentity {
	return &entity.TableIdentity{
		TableName:    "items",
		Strategy:     entity.PKStrategy,
		HasPK:        true,
		IdentifyCols: []string{"tenant_id", "code"},
		CursorCols:   []string{"tenant_id", "code"},
		Columns: []entity.ColumnMeta{
			{Name: "tenant_id", DataType: "int", IsPrimaryKey: true},
			{Name: "code", DataType: "varchar", IsPrimaryKey: true},
			{Name: "payload", DataType: "text"},
		},
	}
}

// expectParallelTargetWriteSessionsN 支持每个 worker 各写入 1 行的场景。
func expectParallelTargetWriteSessionsN(mock sqlmock.Sqlmock, insertSQL string, sessions, writes int) {
	for i := 0; i < sessions; i++ {
		mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableForeignKeyChecksSQL)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta(fullSyncDisableUniqueChecksSQL)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(regexp.QuoteMeta(fullSyncVerifyChecksSQL)).
			WillReturnRows(sqlmock.NewRows([]string{"foreign_key_checks", "unique_checks"}).AddRow(0, 0))
		mock.ExpectExec(regexp.QuoteMeta(fullSyncLockWaitTimeoutSQL)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta(fullSyncRestoreForeignKeyChecksSQL)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta(fullSyncRestoreUniqueChecksSQL)).
			WillReturnResult(sqlmock.NewResult(0, 0))
	}
	for i := 0; i < writes; i++ {
		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta(insertSQL)).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
	}
}

// TestSampleBoundary_VarcharPK_3Workers 验证 varchar 主键 3 worker sample 分片：
// - 边界连续：w0 上界=b1, w1 下界=b1 上界=b2, w2 下界=b2 无上界
// - SQL 模式正确：w0 仅上界、w1 上下界、w2 仅下界
// - 所有 worker 读取并集 = 源表行数
func TestSampleBoundary_VarcharPK_3Workers(t *testing.T) {
	identity := varcharPKIdentity()
	tableName := "events"
	insertSQL := insertPlainSQL("INSERT IGNORE INTO `" + fullSyncTgtSchema + "`.`" + tableName + "` (`code`, `payload`) VALUES (?, ?)")

	sourceDB, sourceMock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer sourceDB.Close()
	sourceMock.MatchExpectationsInOrder(false)

	targetDB, targetMock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer targetDB.Close()
	targetMock.MatchExpectationsInOrder(false)

	// readLimit = syncReadBatchLimit(100) / (2+1 heavy) = 33
	rl := int64(33)

	// === 源端 mock ===
	// 1. information_schema.TABLE_ROWS 估算（step = 6/3 = 2）
	sourceMock.ExpectQuery("SELECT TABLE_ROWS FROM information_schema.TABLES").
		WithArgs(fullSyncSrcSchema, "events").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_ROWS"}).AddRow(int64(6)))

	// 2. keyset 步进取边界（2 步产出 2 个边界，3 个 worker）
	sourceMock.ExpectQuery("^SELECT `code` FROM `" + fullSyncSrcSchema + "`.`events` ORDER BY `code` ASC LIMIT \\?$").
		WithArgs(int64(2)).
		WillReturnRows(sqlmock.NewRows([]string{"code"}).AddRow("aaa").AddRow("bbb"))
	sourceMock.ExpectQuery("^SELECT `code` FROM `"+fullSyncSrcSchema+"`.`events` WHERE `code` > \\? ORDER BY `code` ASC LIMIT \\?$").
		WithArgs("bbb", int64(2)).
		WillReturnRows(sqlmock.NewRows([]string{"code"}).AddRow("ccc").AddRow("ddd"))

	// 3. worker 读取（上界 <= 含边界行，用 WithArgs 区分相同 SQL 模式的不同边界）
	// w0 b1: start=nil, end="bbb" -> WHERE code <= 'bbb' -> aaa, bbb
	sourceMock.ExpectQuery("^SELECT `code`, `payload` FROM `"+fullSyncSrcSchema+"`.`events` WHERE `code` <= \\? ORDER BY `code` ASC LIMIT \\?$").
		WithArgs("bbb", rl).
		WillReturnRows(sqlmock.NewRows([]string{"code", "payload"}).AddRow("aaa", "p0").AddRow("bbb", "p0b"))
	// w0 b2: start="bbb", end="bbb" -> WHERE code > 'bbb' AND code <= 'bbb' -> 空
	sourceMock.ExpectQuery("^SELECT `code`, `payload` FROM `"+fullSyncSrcSchema+"`.`events` WHERE `code` > \\? AND `code` <= \\? ORDER BY `code` ASC LIMIT \\?$").
		WithArgs("bbb", "bbb", rl).
		WillReturnRows(sqlmock.NewRows([]string{"code", "payload"}))

	// w1 b1: start="bbb", end="ddd" -> WHERE code > 'bbb' AND code <= 'ddd' -> ccc, ddd
	sourceMock.ExpectQuery("^SELECT `code`, `payload` FROM `"+fullSyncSrcSchema+"`.`events` WHERE `code` > \\? AND `code` <= \\? ORDER BY `code` ASC LIMIT \\?$").
		WithArgs("bbb", "ddd", rl).
		WillReturnRows(sqlmock.NewRows([]string{"code", "payload"}).AddRow("ccc", "p1").AddRow("ddd", "p1b"))
	// w1 b2: start="ddd", end="ddd" -> 空
	sourceMock.ExpectQuery("^SELECT `code`, `payload` FROM `"+fullSyncSrcSchema+"`.`events` WHERE `code` > \\? AND `code` <= \\? ORDER BY `code` ASC LIMIT \\?$").
		WithArgs("ddd", "ddd", rl).
		WillReturnRows(sqlmock.NewRows([]string{"code", "payload"}))

	// w2 b1: start="ddd", end=nil -> WHERE code > 'ddd'（退化）
	sourceMock.ExpectQuery("^SELECT `code`, `payload` FROM `"+fullSyncSrcSchema+"`.`events` WHERE `code` > \\? ORDER BY `code` ASC LIMIT \\?$").
		WithArgs("ddd", rl).
		WillReturnRows(sqlmock.NewRows([]string{"code", "payload"}).AddRow("eee", "p2"))
	// w2 b2: start="eee" -> 空
	sourceMock.ExpectQuery("^SELECT `code`, `payload` FROM `"+fullSyncSrcSchema+"`.`events` WHERE `code` > \\? ORDER BY `code` ASC LIMIT \\?$").
		WithArgs("eee", rl).
		WillReturnRows(sqlmock.NewRows([]string{"code", "payload"}))

	// === 目标端 mock ===
	expectTargetTableAlreadyExists(targetMock, fullSyncTgtSchema, tableName)
	// 3 个 worker 各 1 个事务（多行 INSERT），共 3 次 Begin/Exec/Commit
	expectParallelTargetWriteSessionsN(targetMock, insertSQL, 3, 3)

	// === 运行 ===
	cfg := taskEntity.TaskConfig{
		ID:                       "sample_boundary_varchar",
		Name:                     "Sample Boundary Varchar",
		Mode:                     taskEntity.SyncModeFull,
		SourceSchema:             fullSyncSrcSchema,
		TargetSchema:             fullSyncTgtSchema,
		BatchSize:                100,
		WorkerCount:              1,
		IntraTableWorkerCount:    3,
		EnableDropTableBeforeDDL: false,
	}
	task := taskEntity.NewSyncTask(cfg)
	task.Start()

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
	assert.NoError(t, sourceMock.ExpectationsWereMet())
	assert.NoError(t, targetMock.ExpectationsWereMet())
}

// TestSampleBoundary_CompositePK_2Workers 验证复合主键（int + varchar）sample 分片：
// - 复合键上界展开为 OR 表达式写入 SQL
// - w0 只有上界，w1 退化为 ReadBatchByKeys
func TestSampleBoundary_CompositePK_2Workers(t *testing.T) {
	identity := compositePKIdentity()
	tableName := "items"
	insertSQL := insertPlainSQL("INSERT IGNORE INTO `" + fullSyncTgtSchema + "`.`" + tableName + "` (`tenant_id`, `code`, `payload`) VALUES (?, ?, ?)")

	sourceDB, sourceMock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer sourceDB.Close()
	sourceMock.MatchExpectationsInOrder(false)

	targetDB, targetMock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer targetDB.Close()
	targetMock.MatchExpectationsInOrder(false)

	rl := int64(33)

	// === 源端 mock ===
	// 1. TABLE_ROWS 估算（step = 4/2 = 2）
	sourceMock.ExpectQuery("SELECT TABLE_ROWS FROM information_schema.TABLES").
		WithArgs(fullSyncSrcSchema, "items").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_ROWS"}).AddRow(int64(4)))

	// 2. keyset 步进：1 步产出 1 个边界 [1, "mmm"]
	sourceMock.ExpectQuery("^SELECT `tenant_id`, `code` FROM `" + fullSyncSrcSchema + "`.`items` ORDER BY `tenant_id`, `code` ASC LIMIT \\?$").
		WithArgs(int64(2)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "code"}).
			AddRow(int64(1), "aaa").AddRow(int64(1), "mmm"))

	// 3. worker 读取（复合键：上界用 <=，下界用 >，用 regex 区分）
	// w0 b1: start=nil, end=[1,"mmm"] -> 上界 <= OR 分支 (id=? AND code=?) OR (id=? AND code<?) OR (id<?)
	sourceMock.ExpectQuery("^SELECT `tenant_id`, `code`, `payload` FROM `"+fullSyncSrcSchema+"`.`items` WHERE \\(\\`tenant_id\\` = \\? AND \\`code\\` = \\?\\) OR \\(\\`tenant_id\\` = \\? AND \\`code\\` < \\?\\) OR \\(\\`tenant_id\\` < \\?\\) ORDER BY `tenant_id`, `code` ASC LIMIT \\?$").
		WithArgs(int64(1), "mmm", int64(1), "mmm", int64(1), rl).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "code", "payload"}).
			AddRow(int64(1), "aaa", "p0").AddRow(int64(1), "mmm", "p0b"))
	// w0 b2: start=[1,"mmm"], end=[1,"mmm"] -> 上下界都有，返回空
	sourceMock.ExpectQuery("^SELECT `tenant_id`, `code`, `payload` FROM `"+fullSyncSrcSchema+"`.`items` WHERE \\(.*>.*\\) AND \\(.*<.*\\) ORDER BY `tenant_id`, `code` ASC LIMIT \\?$").
		WithArgs(int64(1), "mmm", int64(1), int64(1), "mmm", int64(1), "mmm", int64(1), rl).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "code", "payload"}))

	// w1 b1: start=[1,"mmm"], end=nil -> 退化为 ReadBatchByKeys，下界 OR 分支 (code > ?)
	sourceMock.ExpectQuery("^SELECT `tenant_id`, `code`, `payload` FROM `"+fullSyncSrcSchema+"`.`items` WHERE \\(\\`tenant_id\\` = \\? AND \\`code\\` > \\?\\) OR \\(\\`tenant_id\\` > \\?\\) ORDER BY `tenant_id`, `code` ASC LIMIT \\?$").
		WithArgs(int64(1), "mmm", int64(1), rl).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "code", "payload"}).
			AddRow(int64(2), "zzz", "p1"))
	// w1 b2: start=[2,"zzz"], end=nil -> 空
	sourceMock.ExpectQuery("^SELECT `tenant_id`, `code`, `payload` FROM `"+fullSyncSrcSchema+"`.`items` WHERE \\(\\`tenant_id\\` = \\? AND \\`code\\` > \\?\\) OR \\(\\`tenant_id\\` > \\?\\) ORDER BY `tenant_id`, `code` ASC LIMIT \\?$").
		WithArgs(int64(2), "zzz", int64(2), rl).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "code", "payload"}))

	// === 目标端 mock ===
	expectTargetTableAlreadyExists(targetMock, fullSyncTgtSchema, tableName)
	// 2 个 worker 各 1 个事务（多行 INSERT），共 2 次 Begin/Exec/Commit
	expectParallelTargetWriteSessionsN(targetMock, insertSQL, 2, 2)

	// === 运行 ===
	cfg := taskEntity.TaskConfig{
		ID:                       "sample_boundary_composite",
		Name:                     "Sample Boundary Composite",
		Mode:                     taskEntity.SyncModeFull,
		SourceSchema:             fullSyncSrcSchema,
		TargetSchema:             fullSyncTgtSchema,
		BatchSize:                100,
		WorkerCount:              1,
		IntraTableWorkerCount:    2,
		EnableDropTableBeforeDDL: false,
	}
	task := taskEntity.NewSyncTask(cfg)
	task.Start()

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
	assert.NoError(t, sourceMock.ExpectationsWereMet())
	assert.NoError(t, targetMock.ExpectationsWereMet())
}
