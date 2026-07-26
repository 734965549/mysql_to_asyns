package service

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"mysql-to-sync/internal/audit"
	"mysql-to-sync/internal/checkpoint"
	"mysql-to-sync/internal/config"
	"mysql-to-sync/internal/metadata/domain/entity"
	taskEntity "mysql-to-sync/internal/task/domain/entity"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockAnalyzer ?????? IdentityAnalyzer
type mockAnalyzer struct{}

type failingTaskStorage struct {
	err error
}

func (s *failingTaskStorage) Save(task *taskEntity.SyncTask) error {
	return s.err
}

func (s *failingTaskStorage) Delete(taskID string) error {
	return nil
}

func (s *failingTaskStorage) LoadAll() ([]*taskEntity.SyncTask, error) {
	return nil, nil
}

func (s *failingTaskStorage) QueryTasksPage(page, pageSize int, status, keyword, sortBy string) ([]*taskEntity.SyncTask, int, int, int, error) {
	return nil, 0, page, pageSize, s.err
}

func (m *mockAnalyzer) AnalyzeTable(schema, tableName string) (*entity.TableIdentity, error) {
	return &entity.TableIdentity{
		TableName:    tableName,
		Strategy:     entity.PKStrategy,
		IdentifyCols: []string{"id"},
	}, nil
}

func (m *mockAnalyzer) GetAllTables(schema string) ([]entity.TableInfo, error) {
	return []entity.TableInfo{
		{Schema: schema, TableName: "users"},
		{Schema: schema, TableName: "orders"},
	}, nil
}

func (m *mockAnalyzer) GetAllDatabases() ([]string, error) {
	return []string{"test", "test_target"}, nil
}

func TestStripNonPrimaryIndexesFromCreateSQL(t *testing.T) {
	createSQL := "CREATE TABLE `users` (\n" +
		"  `id` bigint NOT NULL,\n" +
		"  `email` varchar(255) NOT NULL,\n" +
		"  `name` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  UNIQUE KEY `uk_email` (`email`),\n" +
		"  KEY `idx_name` (`name`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

	stripped := stripNonPrimaryIndexesFromCreateSQL(createSQL)

	assert.Contains(t, stripped, "PRIMARY KEY (`id`)")
	assert.NotContains(t, stripped, "UNIQUE KEY `uk_email`")
	assert.NotContains(t, stripped, "KEY `idx_name`")
	assert.False(t, strings.Contains(stripped, ",\n) ENGINE"))
}

func TestStripNonPrimaryIndexesFromCreateSQL_KeepPrimaryOnlyDDLValid(t *testing.T) {
	createSQL := "CREATE TABLE `orders` (\n" +
		"  `id` bigint NOT NULL,\n" +
		"  `tenant_id` bigint NOT NULL,\n" +
		"  PRIMARY KEY (`id`,`tenant_id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

	stripped := stripNonPrimaryIndexesFromCreateSQL(createSQL)

	assert.Equal(t, createSQL, stripped)
	assert.Contains(t, stripped, "PRIMARY KEY (`id`,`tenant_id`)")
}

func TestStripNonPrimaryIndexesFromCreateSQL_KeepsAutoIncrementKey(t *testing.T) {
	createSQL := "CREATE TABLE `repair` (\n" +
		"  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
		"  `dealer_id` bigint DEFAULT NULL,\n" +
		"  `name` varchar(255) DEFAULT NULL,\n" +
		"  UNIQUE KEY `uk_id` (`id`),\n" +
		"  KEY `idx_dealer_id` (`dealer_id`),\n" +
		"  KEY `idx_name` (`name`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

	stripped := stripNonPrimaryIndexesFromCreateSQL(createSQL)

	assert.Contains(t, stripped, "`id` bigint NOT NULL AUTO_INCREMENT")
	assert.Contains(t, stripped, "UNIQUE KEY `uk_id` (`id`)")
	assert.NotContains(t, stripped, "KEY `idx_dealer_id` (`dealer_id`)")
	assert.NotContains(t, stripped, "KEY `idx_name` (`name`)")
	assert.False(t, strings.Contains(stripped, ",\n) ENGINE"))
}

func TestSelectDeferredIndexes_ALLKeepsIdentityUK(t *testing.T) {
	ukIdentity := &entity.TableIdentity{
		TableName:    "users",
		Strategy:     entity.UKStrategy,
		HasUK:        true,
		IdentifyCols: []string{"email"},
	}
	indexes := []map[string]interface{}{
		{"name": "uk_email", "non_unique": 0, "columns": "`email`"},
		{"name": "uk_phone", "non_unique": 0, "columns": "`phone`"},
		{"name": "idx_name", "non_unique": 1, "columns": "`name`"},
	}

	// optimize_index=true：保留 identity UK，延迟其他唯一+非唯一
	deferred := selectDeferredIndexes(indexes, ukIdentity, taskEntity.SyncModeAll, true)
	require.Len(t, deferred, 2)
	assert.Equal(t, "uk_phone", deferred[0]["name"])
	assert.Equal(t, "idx_name", deferred[1]["name"])

	// optimize_index=false：仍延迟非 identity 唯一索引，但保留普通二级索引
	deferred = selectDeferredIndexes(indexes, ukIdentity, taskEntity.SyncModeAll, false)
	require.Len(t, deferred, 1)
	assert.Equal(t, "uk_phone", deferred[0]["name"])

	// FULL + optimize：延迟全部非 PK（含 identity UK）
	deferred = selectDeferredIndexes(indexes, ukIdentity, taskEntity.SyncModeFull, true)
	require.Len(t, deferred, 3)
}

func TestDropDeferredIndexes_FailClosedRestoresDropped(t *testing.T) {
	targetDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	// SHOW INDEX: two unique indexes to defer
	indexRows := sqlmock.NewRows([]string{
		"Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name", "Collation", "Cardinality",
		"Sub_part", "Packed", "Null", "Index_type", "Comment", "Index_comment", "Visible", "Expression",
	}).
		AddRow("users", 0, "uk_phone", 1, "phone", "A", 1, nil, nil, "YES", "BTREE", "", "", "YES", nil).
		AddRow("users", 0, "uk_code", 1, "code", "A", 1, nil, nil, "YES", "BTREE", "", "", "YES", nil)
	mock.ExpectQuery("SHOW INDEX FROM").WillReturnRows(indexRows)
	mock.ExpectExec("ALTER TABLE `tgt`.`users` DROP INDEX `uk_code`").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("ALTER TABLE `tgt`.`users` DROP INDEX `uk_phone`").WillReturnError(fmt.Errorf("cannot drop"))
	// rollback already-dropped uk_code
	mock.ExpectQuery("SELECT NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, SEQ_IN_INDEX").
		WithArgs("tgt", "users", "uk_code").
		WillReturnRows(sqlmock.NewRows([]string{"NON_UNIQUE", "INDEX_TYPE", "COLUMN_NAME", "SUB_PART", "SEQ_IN_INDEX"}))
	mock.ExpectExec("ALTER TABLE `tgt`.`users` ADD UNIQUE INDEX `uk_code`").WillReturnResult(sqlmock.NewResult(0, 0))

	ts := &TaskService{}
	dropped, dropErr := ts.dropDeferredIndexes(
		context.Background(),
		&taskRuntime{targetDB: targetDB},
		"tgt",
		"users",
		&entity.TableIdentity{Strategy: entity.PKStrategy, IdentifyCols: []string{"id"}},
		taskEntity.SyncModeAll,
		false,
	)
	require.Error(t, dropErr)
	assert.Nil(t, dropped)
	assert.Contains(t, dropErr.Error(), "failed to drop deferred index")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStripIndexesByNameFromCreateSQL_KeepsIdentityUK(t *testing.T) {
	createSQL := "CREATE TABLE `users` (\n" +
		"  `id` bigint NOT NULL,\n" +
		"  `email` varchar(255) NOT NULL,\n" +
		"  `phone` varchar(32) DEFAULT NULL,\n" +
		"  UNIQUE KEY `uk_email` (`email`),\n" +
		"  UNIQUE KEY `uk_phone` (`phone`),\n" +
		"  KEY `idx_phone` (`phone`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

	stripped := stripIndexesByNameFromCreateSQL(createSQL, map[string]struct{}{
		"uk_phone":  {},
		"idx_phone": {},
	})
	assert.Contains(t, stripped, "UNIQUE KEY `uk_email` (`email`)")
	assert.NotContains(t, stripped, "UNIQUE KEY `uk_phone`")
	assert.NotContains(t, stripped, "KEY `idx_phone`")
}

func TestRestorePendingIndexes_ProcessesTablesConcurrently(t *testing.T) {
	targetDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	emptyIndexStatsRows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"NON_UNIQUE", "INDEX_TYPE", "COLUMN_NAME", "SUB_PART", "SEQ_IN_INDEX"})
	}

	// sqlmock checks expectations in order by default. Disable ordered matching
	// because the concurrent implementation may dispatch the two tables in any
	// order, and each table's ALTER TABLE may overlap with the other table's.
	// ?? ExpectQuery ?????? Rows?????????????????
	// *sqlmock.Rows ??? data race?
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("SELECT NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, SEQ_IN_INDEX").
		WithArgs("target_db", "users", "idx_users_name").
		WillReturnRows(emptyIndexStatsRows())
	mock.ExpectQuery("SELECT NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, SEQ_IN_INDEX").
		WithArgs("target_db", "orders", "uk_orders_no").
		WillReturnRows(emptyIndexStatsRows())
	mock.ExpectExec("ALTER TABLE `target_db`.`users` ADD INDEX `idx_users_name` \\(`name`\\)").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("ALTER TABLE `target_db`.`orders` ADD UNIQUE INDEX `uk_orders_no` \\(`order_no`\\)").
		WillReturnResult(sqlmock.NewResult(0, 0))

	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"task-index-order": {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusRunning}},
		},
	}
	runtime := &taskRuntime{targetDB: targetDB}
	pending := []pendingIndexRestore{
		{
			targetSchema: "target_db",
			targetTable:  "users",
			indexes: []map[string]interface{}{
				{"name": "idx_users_name", "non_unique": 1, "type": "BTREE", "columns": "`name`"},
			},
		},
		{
			targetSchema: "target_db",
			targetTable:  "orders",
			indexes: []map[string]interface{}{
				{"name": "uk_orders_no", "non_unique": 0, "type": "BTREE", "columns": "`order_no`"},
			},
		},
	}

	require.NoError(t, ts.restorePendingIndexes(context.Background(), runtime, "task-index-order", pending, 2))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRestorePendingIndexes_RespectsStopSignal(t *testing.T) {
	targetDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	// ????? ALTER TABLE ADD INDEX ???
	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"task-stop": {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusPaused}},
		},
	}
	runtime := &taskRuntime{targetDB: targetDB}
	pending := []pendingIndexRestore{
		{
			targetSchema: "db",
			targetTable:  "t",
			indexes: []map[string]interface{}{
				{"name": "idx", "non_unique": 1, "type": "BTREE", "columns": "`c`"},
			},
		},
	}

	err = ts.restorePendingIndexes(context.Background(), runtime, "task-stop", pending, 2)
	require.ErrorIs(t, err, errFullSyncStoppedByUser)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRestorePendingIndexes_FailsFastOnFirstError_Sequential(t *testing.T) {
	targetDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	emptyStatsRows := sqlmock.NewRows([]string{"NON_UNIQUE", "INDEX_TYPE", "COLUMN_NAME", "SUB_PART", "SEQ_IN_INDEX"})

	// workers=1 ????????? a ?????? break ?????? b ? Exec ????????
	mock.ExpectQuery("SELECT NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, SEQ_IN_INDEX").
		WithArgs("db", "a", "idx_a").
		WillReturnRows(emptyStatsRows)
	mock.ExpectExec("ALTER TABLE `db`.`a` ADD INDEX `idx_a` \\(`c`\\)").
		WillReturnError(fmt.Errorf("DDL failed"))

	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"task-fail": {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusRunning}},
		},
	}
	runtime := &taskRuntime{targetDB: targetDB}
	pending := []pendingIndexRestore{
		{
			targetSchema: "db",
			targetTable:  "a",
			indexes: []map[string]interface{}{
				{"name": "idx_a", "non_unique": 1, "type": "BTREE", "columns": "`c`"},
			},
		},
		{
			targetSchema: "db",
			targetTable:  "b",
			indexes: []map[string]interface{}{
				{"name": "idx_b", "non_unique": 1, "type": "BTREE", "columns": "`c`"},
			},
		},
	}

	err = ts.restorePendingIndexes(context.Background(), runtime, "task-fail", pending, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "restore indexes for db.a")
	require.NoError(t, mock.ExpectationsWereMet())
}

// fakeIndexRestoreDriver ??????????? fail-fast / context ?????
// ??? started / proceed channel ???????????? DDL ?????????
type fakeIndexRestoreDriver struct {
	mu      sync.Mutex
	started chan struct{}
	proceed chan struct{}
	calls   []string
}

func (d *fakeIndexRestoreDriver) Open(string) (driver.Conn, error) {
	return &fakeIndexRestoreConn{driver: d}, nil
}

type fakeIndexRestoreConn struct{ driver *fakeIndexRestoreDriver }

func (c *fakeIndexRestoreConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not supported")
}
func (c *fakeIndexRestoreConn) Close() error              { return nil }
func (c *fakeIndexRestoreConn) Begin() (driver.Tx, error) { return nil, errors.New("not supported") }

func (c *fakeIndexRestoreConn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.driver.mu.Lock()
	c.driver.calls = append(c.driver.calls, query)
	c.driver.mu.Unlock()

	select {
	case c.driver.started <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case <-c.driver.proceed:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if strings.Contains(query, "idx_a") {
		return nil, errors.New("DDL failed for a")
	}
	return driver.ResultNoRows, nil
}

func (c *fakeIndexRestoreConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return &fakeIndexRestoreRows{}, nil
}

// fakeIndexRestoreRows ????????? fakeIndexRestoreConn ? QueryContext?
type fakeIndexRestoreRows struct{}

func (r *fakeIndexRestoreRows) Columns() []string { return nil }
func (r *fakeIndexRestoreRows) Close() error      { return nil }
func (r *fakeIndexRestoreRows) Next(dest []driver.Value) error {
	return io.EOF
}

func TestRestorePendingIndexes_FailsFastOnFirstError_Concurrent(t *testing.T) {
	drv := &fakeIndexRestoreDriver{
		started: make(chan struct{}, 2),
		proceed: make(chan struct{}),
	}
	sql.Register("fake_index_restore_failfast", drv)

	targetDB, err := sql.Open("fake_index_restore_failfast", "")
	require.NoError(t, err)
	defer targetDB.Close()

	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"task-fail-concurrent": {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusRunning}},
		},
	}
	runtime := &taskRuntime{targetDB: targetDB}
	pending := []pendingIndexRestore{
		{
			targetSchema: "db",
			targetTable:  "a",
			indexes: []map[string]interface{}{
				{"name": "idx_a", "non_unique": 1, "type": "BTREE", "columns": "`c`"},
			},
		},
		{
			targetSchema: "db",
			targetTable:  "b",
			indexes: []map[string]interface{}{
				{"name": "idx_b", "non_unique": 1, "type": "BTREE", "columns": "`c`"},
			},
		},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- ts.restorePendingIndexes(context.Background(), runtime, "task-fail-concurrent", pending, 2)
	}()

	// ?????? DDL ?????????????
	<-drv.started
	<-drv.started
	close(drv.proceed)

	err = <-errCh
	require.Error(t, err)
	require.Contains(t, err.Error(), "restore indexes for db.a")

	drv.mu.Lock()
	require.Len(t, drv.calls, 2)
	drv.mu.Unlock()
}

func TestRestorePendingIndexes_ContextCanceled_SkipsAllWork(t *testing.T) {
	targetDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"task-ctx-cancel": {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusRunning}},
		},
	}
	runtime := &taskRuntime{targetDB: targetDB}
	pending := []pendingIndexRestore{
		{
			targetSchema: "db",
			targetTable:  "a",
			indexes: []map[string]interface{}{
				{"name": "idx_a", "non_unique": 1, "type": "BTREE", "columns": "`c`"},
			},
		},
		{
			targetSchema: "db",
			targetTable:  "b",
			indexes: []map[string]interface{}{
				{"name": "idx_b", "non_unique": 1, "type": "BTREE", "columns": "`c`"},
			},
		},
	}

	err = ts.restorePendingIndexes(ctx, runtime, "task-ctx-cancel", pending, 2)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRestoreIndexes_ContextCanceled_ReturnsEarly(t *testing.T) {
	targetDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ts := &TaskService{}
	runtime := &taskRuntime{targetDB: targetDB}
	indexes := []map[string]interface{}{
		{"name": "idx_a", "non_unique": 1, "type": "BTREE", "columns": "`c`"},
	}

	err = ts.restoreIndexes(ctx, runtime, "db", "a", indexes)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFilterIndexesUsingAutoIncrementColumns(t *testing.T) {
	indexes := []map[string]interface{}{
		{"name": "idx_id", "columns": "`id`"},
		{"name": "idx_name", "columns": "`name`"},
		{"name": "idx_id_name", "columns": "`id`, `name`"},
	}

	autoIncs := map[string]struct{}{"id": {}}
	filtered := filterIndexesUsingAutoIncrementColumns(indexes, autoIncs)

	require.Len(t, filtered, 1)
	assert.Equal(t, "idx_name", filtered[0]["name"])
}

func TestFilterIndexesUsingAutoIncrementColumns_EmptyAutoIncrements(t *testing.T) {
	indexes := []map[string]interface{}{
		{"name": "idx_id", "columns": "`id`"},
	}

	filtered := filterIndexesUsingAutoIncrementColumns(indexes, nil)
	assert.Equal(t, indexes, filtered)
}

func TestDropNonPrimaryKeyIndexes_FailClosedRestoresDropped(t *testing.T) {
	targetDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	rows := sqlmock.NewRows([]string{
		"Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name",
		"Collation", "Cardinality", "Sub_part", "Packed", "Null",
		"Index_type", "Comment", "Index_comment", "Visible", "Expression",
	}).AddRow("t", 1, "idx_a", 1, "a", "A", 0, nil, nil, "YES", "BTREE", "", "", "YES", nil).
		AddRow("t", 1, "idx_b", 1, "b", "A", 0, nil, nil, "YES", "BTREE", "", "", "YES", nil)

	mock.ExpectQuery("SHOW INDEX FROM `db`.`t`").WillReturnRows(rows)
	mock.ExpectExec("ALTER TABLE `db`.`t` DROP INDEX `idx_a`").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("ALTER TABLE `db`.`t` DROP INDEX `idx_b`").WillReturnError(fmt.Errorf("lock wait timeout"))
	mock.ExpectQuery("SELECT NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, SEQ_IN_INDEX").
		WithArgs("db", "t", "idx_a").
		WillReturnRows(sqlmock.NewRows([]string{"NON_UNIQUE", "INDEX_TYPE", "COLUMN_NAME", "SUB_PART", "SEQ_IN_INDEX"}))
	mock.ExpectExec("ALTER TABLE `db`.`t` ADD INDEX `idx_a`").WillReturnResult(sqlmock.NewResult(0, 0))

	ts := &TaskService{}
	runtime := &taskRuntime{targetDB: targetDB}
	dropped, err := ts.dropNonPrimaryKeyIndexes(context.Background(), runtime, "db", "t")

	require.Error(t, err)
	assert.Nil(t, dropped)
	assert.Contains(t, err.Error(), "failed to drop deferred index")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTargetIndexExists_MatchingIndex(t *testing.T) {
	targetDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	rows := sqlmock.NewRows([]string{"NON_UNIQUE", "INDEX_TYPE", "COLUMN_NAME", "SUB_PART", "SEQ_IN_INDEX"}).
		AddRow(1, "BTREE", "c", nil, 1)
	mock.ExpectQuery("SELECT NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, SEQ_IN_INDEX").
		WithArgs("db", "t", "idx_c").
		WillReturnRows(rows)

	exists, match, err := targetIndexExists(context.Background(), targetDB, "db", "t", "idx_c", 1, "BTREE", "`c`")

	require.NoError(t, err)
	assert.True(t, exists)
	assert.True(t, match)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTargetIndexExists_ConflictingIndex(t *testing.T) {
	targetDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	rows := sqlmock.NewRows([]string{"NON_UNIQUE", "INDEX_TYPE", "COLUMN_NAME", "SUB_PART", "SEQ_IN_INDEX"}).
		AddRow(1, "BTREE", "d", nil, 1)
	mock.ExpectQuery("SELECT NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, SEQ_IN_INDEX").
		WithArgs("db", "t", "idx_c").
		WillReturnRows(rows)

	exists, match, err := targetIndexExists(context.Background(), targetDB, "db", "t", "idx_c", 1, "BTREE", "`c`")

	require.NoError(t, err)
	assert.True(t, exists)
	assert.False(t, match)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRestoreIndexes_SkipsExistingMatchingIndex(t *testing.T) {
	targetDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	rows := sqlmock.NewRows([]string{"NON_UNIQUE", "INDEX_TYPE", "COLUMN_NAME", "SUB_PART", "SEQ_IN_INDEX"}).
		AddRow(1, "BTREE", "c", nil, 1)
	mock.ExpectQuery("SELECT NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, SEQ_IN_INDEX").
		WithArgs("db", "t", "idx_c").
		WillReturnRows(rows)
	// ????? ALTER TABLE ADD INDEX

	ts := &TaskService{}
	runtime := &taskRuntime{targetDB: targetDB}
	indexes := []map[string]interface{}{
		{"name": "idx_c", "non_unique": 1, "type": "BTREE", "columns": "`c`"},
	}

	err = ts.restoreIndexes(context.Background(), runtime, "db", "t", indexes)

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRestoreIndexes_FailsOnConflictingExistingIndex(t *testing.T) {
	targetDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	rows := sqlmock.NewRows([]string{"NON_UNIQUE", "INDEX_TYPE", "COLUMN_NAME", "SUB_PART", "SEQ_IN_INDEX"}).
		AddRow(1, "BTREE", "d", nil, 1)
	mock.ExpectQuery("SELECT NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, SEQ_IN_INDEX").
		WithArgs("db", "t", "idx_c").
		WillReturnRows(rows)

	ts := &TaskService{}
	runtime := &taskRuntime{targetDB: targetDB}
	indexes := []map[string]interface{}{
		{"name": "idx_c", "non_unique": 1, "type": "BTREE", "columns": "`c`"},
	}

	err = ts.restoreIndexes(context.Background(), runtime, "db", "t", indexes)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRestoreIndexes_BatchesMultipleIndexesIntoOneAlter(t *testing.T) {
	targetDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	empty := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"NON_UNIQUE", "INDEX_TYPE", "COLUMN_NAME", "SUB_PART", "SEQ_IN_INDEX"})
	}
	mock.ExpectQuery("SELECT NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, SEQ_IN_INDEX").
		WithArgs("db", "t", "idx_a").
		WillReturnRows(empty())
	mock.ExpectQuery("SELECT NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, SEQ_IN_INDEX").
		WithArgs("db", "t", "uk_b").
		WillReturnRows(empty())
	mock.ExpectQuery("SELECT NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, SEQ_IN_INDEX").
		WithArgs("db", "t", "ft_c").
		WillReturnRows(empty())
	mock.ExpectExec("ALTER TABLE `db`.`t` ADD INDEX `idx_a` \\(`a`\\), ADD UNIQUE INDEX `uk_b` \\(`b`\\)").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("ALTER TABLE `db`.`t` ADD FULLTEXT INDEX `ft_c` \\(`c`\\)").
		WillReturnResult(sqlmock.NewResult(0, 0))

	ts := &TaskService{}
	runtime := &taskRuntime{targetDB: targetDB}
	indexes := []map[string]interface{}{
		{"name": "idx_a", "non_unique": 1, "type": "BTREE", "columns": "`a`"},
		{"name": "uk_b", "non_unique": 0, "type": "BTREE", "columns": "`b`"},
		{"name": "ft_c", "non_unique": 1, "type": "FULLTEXT", "columns": "`c`"},
	}

	err = ts.restoreIndexes(context.Background(), runtime, "db", "t", indexes)

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBuildAlterAddIndexesSQL(t *testing.T) {
	sql := buildAlterAddIndexesSQL("db", "t", []string{
		buildAddIndexClause("idx_a", 1, "BTREE", "`a`"),
		buildAddIndexClause("uk_b", 0, "BTREE", "`b`"),
	})
	assert.Equal(t, "ALTER TABLE `db`.`t` ADD INDEX `idx_a` (`a`), ADD UNIQUE INDEX `uk_b` (`b`)", sql)
}

func TestGroupIndexRestoreBatches_SplitsByType(t *testing.T) {
	items := []indexRestoreItem{
		{"idx_a", 1, "BTREE", "`a`"},
		{"uk_b", 0, "BTREE", "`b`"},
		{"ft_c", 1, "FULLTEXT", "`c`"},
		{"sp_d", 1, "SPATIAL", "`d`"},
	}
	batches := groupIndexRestoreBatches(items)
	require.Len(t, batches, 3)
	assert.Equal(t, indexRestoreBatchBTREE, batches[0].kind)
	assert.Equal(t, []string{"idx_a", "uk_b"}, batches[0].names)
	assert.Equal(t, indexRestoreBatchFULLTEXT, batches[1].kind)
	assert.Equal(t, []string{"ft_c"}, batches[1].names)
	assert.Equal(t, indexRestoreBatchSPATIAL, batches[2].kind)
	assert.Equal(t, []string{"sp_d"}, batches[2].names)
}

func TestRestoreIndexes_RetriesInvalidConnection(t *testing.T) {
	targetDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	rows := sqlmock.NewRows([]string{"NON_UNIQUE", "INDEX_TYPE", "COLUMN_NAME", "SUB_PART", "SEQ_IN_INDEX"})
	mock.ExpectQuery("SELECT NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, SEQ_IN_INDEX").
		WithArgs("db", "t", "idx_c").
		WillReturnRows(rows)
	mock.ExpectExec("ALTER TABLE `db`.`t` ADD INDEX `idx_c` \\(`c`\\)").
		WillReturnError(fmt.Errorf("invalid connection"))
	mock.ExpectExec("ALTER TABLE `db`.`t` ADD INDEX `idx_c` \\(`c`\\)").
		WillReturnResult(sqlmock.NewResult(0, 0))

	ts := &TaskService{}
	runtime := &taskRuntime{targetDB: targetDB}
	indexes := []map[string]interface{}{
		{"name": "idx_c", "non_unique": 1, "type": "BTREE", "columns": "`c`"},
	}

	err = ts.restoreIndexes(context.Background(), runtime, "db", "t", indexes)

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRestoreIndexes_TreatsAlreadyAppliedBatchAsSuccessAfterRetryError(t *testing.T) {
	targetDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	mock.ExpectQuery("SELECT NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, SEQ_IN_INDEX").
		WithArgs("db", "t", "idx_c").
		WillReturnRows(sqlmock.NewRows([]string{"NON_UNIQUE", "INDEX_TYPE", "COLUMN_NAME", "SUB_PART", "SEQ_IN_INDEX"}))

	mock.ExpectExec("ALTER TABLE `db`.`t` ADD INDEX `idx_c` \\(`c`\\)").
		WillReturnError(fmt.Errorf("invalid connection"))
	mock.ExpectExec("ALTER TABLE `db`.`t` ADD INDEX `idx_c` \\(`c`\\)").
		WillReturnError(fmt.Errorf("Error 1061: Duplicate key name 'idx_c'"))

	mock.ExpectQuery("SELECT NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, SEQ_IN_INDEX").
		WithArgs("db", "t", "idx_c").
		WillReturnRows(sqlmock.NewRows([]string{"NON_UNIQUE", "INDEX_TYPE", "COLUMN_NAME", "SUB_PART", "SEQ_IN_INDEX"}).
			AddRow(1, "BTREE", "c", nil, 1))

	ts := &TaskService{}
	runtime := &taskRuntime{targetDB: targetDB}
	indexes := []map[string]interface{}{
		{"name": "idx_c", "non_unique": 1, "type": "BTREE", "columns": "`c`"},
	}

	err = ts.restoreIndexes(context.Background(), runtime, "db", "t", indexes)

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestIsConnRetryable(t *testing.T) {
	assert.False(t, isConnRetryable(nil))
	assert.True(t, isConnRetryable(driver.ErrBadConn))
	assert.True(t, isConnRetryable(fmt.Errorf("invalid connection")))
	assert.True(t, isConnRetryable(fmt.Errorf("dial tcp 10.0.0.1:3306: connect: connection refused")))
	assert.True(t, isConnRetryable(fmt.Errorf("write: broken pipe")))
	assert.True(t, isConnRetryable(fmt.Errorf("read: connection reset by peer")))
	assert.False(t, isConnRetryable(fmt.Errorf("Error 1062: Duplicate entry")))
}

func TestEffectiveIndexRestoreWorkers(t *testing.T) {
	cases := []struct{ configured, workerCount, hardMax, want int }{
		{0, 0, 0, 4},    // ??? -> 4
		{0, 8, 0, 4},    // ?? min(8,4)=4
		{0, 2, 0, 2},    // workerCount<4 -> 2
		{6, 8, 0, 6},    // ?? 6
		{32, 8, 16, 16}, // ? hardMax ??
		{0, 0, 2, 2},    // hardMax<defaultCap -> 2
		{-1, 0, 0, 4},   // ??? 0
	}
	for _, c := range cases {
		require.Equal(t, c.want, taskEntity.EffectiveIndexRestoreWorkers(c.configured, c.workerCount, c.hardMax))
	}
}

// newTestTaskService ????????????????????
func newTestTaskService(dataDir string) *TaskService {
	storage := NewFileTaskStorage(dataDir)
	return &TaskService{
		tasks:               make(map[string]*taskEntity.SyncTask),
		runtimes:            make(map[string]*taskRuntime),
		runningProgress:     make(map[string]*taskEntity.RunningProgress),
		lastProgressPersist: make(map[string]time.Time),
		storage:             storage,
	}
}

func newScheduledTestTaskService(dataDir string) *TaskService {
	ts := newTestTaskService(dataDir)
	ts.initRuntimeFn = func(task *taskEntity.SyncTask) (*taskRuntime, error) {
		return &taskRuntime{}, nil
	}
	ts.executeSyncFn = func(ctx context.Context, taskID string, runtime *taskRuntime) {}
	return ts
}

func newDefaultConfig() *config.Config {
	return &config.Config{
		Storage: config.StorageConfig{Mode: "file", DataDir: "data"},
	}
}

func TestResolveSourceSchema(t *testing.T) {
	t.Run("prefers task source db database", func(t *testing.T) {
		ts := NewTaskService(&config.Config{
			Storage:    config.StorageConfig{Mode: "file", DataDir: t.TempDir()},
			Datasource: config.DatasourceConfig{Database: "config_db"},
		})
		defer ts.Close()

		task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
			ID:           "task-source-db",
			SourceSchema: "task_schema",
			SourceDB: &taskEntity.DatabaseConfig{
				Database: "source_db_override",
			},
		})

		assert.Equal(t, "source_db_override", ts.resolveSourceSchema(task))
	})

	t.Run("falls back to task source schema", func(t *testing.T) {
		ts := NewTaskService(&config.Config{
			Storage:    config.StorageConfig{Mode: "file", DataDir: t.TempDir()},
			Datasource: config.DatasourceConfig{Database: "config_db"},
		})
		defer ts.Close()

		task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
			ID:           "task-schema",
			SourceSchema: "task_schema",
		})

		assert.Equal(t, "task_schema", ts.resolveSourceSchema(task))
	})

	t.Run("falls back to config datasource database", func(t *testing.T) {
		ts := NewTaskService(&config.Config{
			Storage:    config.StorageConfig{Mode: "file", DataDir: t.TempDir()},
			Datasource: config.DatasourceConfig{Database: "config_db"},
		})
		defer ts.Close()

		task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "config-fallback"})

		assert.Equal(t, "config_db", ts.resolveSourceSchema(task))
	})
}

func TestResolveTargetSchema(t *testing.T) {
	ts := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()}})
	defer ts.Close()

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "rename-target"})
	assert.Equal(t, "target_db", ts.resolveTargetSchema(task, "target_db"))

	task.Config.TargetSchema = "custom_target"
	assert.Equal(t, "custom_target", ts.resolveTargetSchema(task, "target_db"))

	task.Config.TargetSchema = ""
	task.Config.TargetDB = &taskEntity.DatabaseConfig{Database: "target_override"}
	assert.Equal(t, "target_override", ts.resolveTargetSchema(task, "source_db"))
}

func TestResolveTableTargetName(t *testing.T) {
	ts := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()}})
	defer ts.Close()

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "rename-table", SourceSchema: "source_db", Tables: []string{"users", "orders"}, TargetTables: []string{"users_bak", "orders_bak"}})
	assert.Equal(t, "users_bak", ts.resolveTableTargetName(task, "source_db", "users", 0))
	assert.Equal(t, "orders_bak", ts.resolveTableTargetName(task, "source_db", "orders", 1))
	assert.Equal(t, "fallback", ts.resolveTableTargetName(task, "source_db", "fallback", 5))

	task.Config.TargetTables = nil
	assert.Equal(t, "users", ts.resolveTableTargetName(task, "source_db", "users", 0))
}

func TestResolveTableTargetName_MultiDatabaseUsesQualifiedSourceKey(t *testing.T) {
	ts := &TaskService{}
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:              "rename-table-multi-db",
		SourceDatabases: []string{"db_a", "db_b"},
		TargetDatabases: []string{"backup", "backup"},
		Tables:          []string{"db_a.a", "db_a.a2", "db_b.b", "db_b.b2"},
		TargetTables:    []string{"a_target", "a2_target", "b_target", "b2_target"},
	})

	// The local index resets for each source database. db_b.b is local index 0,
	// but its target must come from global index 2, not TargetTables[0].
	assert.Equal(t, "a_target", ts.resolveTableTargetName(task, "db_a", "a", 0))
	assert.Equal(t, "b_target", ts.resolveTableTargetName(task, "db_b", "b", 0))
	assert.Equal(t, "b2_target", ts.resolveTableTargetName(task, "db_b", "b2", 1))
}

func TestResolveTableTargetName_MultiDatabaseDoesNotUseUnsafeLocalIndexFallback(t *testing.T) {
	ts := &TaskService{}
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:              "rename-table-multi-db-fallback",
		SourceDatabases: []string{"db_a", "db_b"},
		Tables:          []string{"db_a.a"},
		TargetTables:    []string{"a_target"},
	})

	assert.Equal(t, "b", ts.resolveTableTargetName(task, "db_b", "b", 0))
}

func TestResolveTableTargetName_MultiDatabaseRejectsAmbiguousUnqualifiedTable(t *testing.T) {
	ts := &TaskService{}
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:              "rename-table-multi-db-unqualified",
		SourceDatabases: []string{"db_a", "db_b"},
		Tables:          []string{"shared"},
		TargetTables:    []string{"wrong_for_db_b"},
	})

	assert.Equal(t, "shared", ts.resolveTableTargetName(task, "db_b", "shared", 0))
}

func TestNewTaskService(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)
	assert.NotNil(t, ts)
	assert.NotNil(t, ts.tasks)
	assert.NotNil(t, ts.storage)
}

func TestNewTaskServiceWithDB(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer sourceDB.Close()

	targetDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	analyzer := &mockAnalyzer{}

	ts := NewTaskServiceWithDB(sourceDB, targetDB, analyzer)
	assert.NotNil(t, ts)
	assert.NotNil(t, ts.sourceDB)
	assert.NotNil(t, ts.targetDB)
	assert.NotNil(t, ts.analyzer)
	assert.NotNil(t, ts.readOnlyManager)
	assert.NotNil(t, ts.checkpointManager)
	assert.True(t, ts.enableReadOnly)
}

func TestNewTaskServiceWithDBAndConfig(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer sourceDB.Close()

	targetDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	analyzer := &mockAnalyzer{}

	cfg := &config.Config{
		Redis: config.RedisConfig{
			Host:     "",
			Port:     0,
			Password: "",
			DB:       0,
		},
	}

	ts := NewTaskServiceWithDBAndConfig(sourceDB, targetDB, analyzer, cfg)
	assert.NotNil(t, ts)
	assert.NotNil(t, ts.sourceDB)
	assert.NotNil(t, ts.targetDB)
	assert.NotNil(t, ts.analyzer)
	assert.NotNil(t, ts.readOnlyManager)
	assert.NotNil(t, ts.checkpointManager)
	assert.NotNil(t, ts.config)
	assert.True(t, ts.enableReadOnly)
}

func TestSetEnableReadOnly(t *testing.T) {
	ts := NewTaskService(newDefaultConfig())

	// ????? false
	ts.SetEnableReadOnly(false)
	assert.False(t, ts.GetEnableReadOnly())

	// ????? true
	ts.SetEnableReadOnly(true)
	assert.True(t, ts.GetEnableReadOnly())
}

func TestReinitStorage_FileMode(t *testing.T) {
	dataDir := t.TempDir()
	newDataDir := t.TempDir()

	cfg := &config.Config{
		Storage: config.StorageConfig{Mode: "file", DataDir: dataDir},
	}

	ts := NewTaskService(cfg)
	require.NotNil(t, ts)

	fileStorage, ok := ts.storage.(*FileTaskStorage)
	require.True(t, ok)
	assert.Equal(t, dataDir, fileStorage.dataDir)

	newCfg := &config.Config{
		Storage: config.StorageConfig{Mode: "file", DataDir: newDataDir},
	}

	err := ts.ReinitStorage(newCfg)
	require.NoError(t, err)

	fileStorage, ok = ts.storage.(*FileTaskStorage)
	require.True(t, ok)
	assert.Equal(t, newDataDir, fileStorage.dataDir)
	assert.Equal(t, newCfg, ts.config)
	assert.DirExists(t, newDataDir)
	assert.NoError(t, ts.Close())
}

func TestReinitCheckpointManager_MemoryMode(t *testing.T) {
	dataDir := t.TempDir()

	ts := NewTaskService(&config.Config{
		Storage: config.StorageConfig{Mode: "file", DataDir: dataDir},
		Redis:   config.RedisConfig{},
	})
	require.NotNil(t, ts)

	_, ok := ts.checkpointManager.(*checkpoint.MemoryCheckpointManager)
	require.True(t, ok)

	newCfg := &config.Config{
		Storage: config.StorageConfig{Mode: "file", DataDir: dataDir},
		Redis:   config.RedisConfig{},
	}

	err := ts.ReinitCheckpointManager(newCfg)
	require.NoError(t, err)

	_, ok = ts.checkpointManager.(*checkpoint.MemoryCheckpointManager)
	require.True(t, ok)
	assert.Equal(t, newCfg, ts.config)
	assert.NoError(t, ts.Close())
}

func TestCreateTask(t *testing.T) {
	ts := NewTaskService(newDefaultConfig())

	taskConfig := taskEntity.TaskConfig{
		ID:                       "test_task_1",
		Name:                     "Test Task",
		SourceSchema:             "source_db",
		TargetSchema:             "target_db",
		Tables:                   []string{"users", "orders"},
		Mode:                     taskEntity.SyncModeFull,
		EnableDropTableBeforeDDL: true,
	}

	task, err := ts.CreateTask(taskConfig)
	assert.NoError(t, err)
	assert.NotNil(t, task)
	assert.Equal(t, "test_task_1", task.Config.ID)
	assert.True(t, task.Config.EnableDropTableBeforeDDL)

	// ???????????
	retrievedTask, exists := ts.GetTask("test_task_1")
	assert.True(t, exists)
	assert.Equal(t, task.Config.ID, retrievedTask.Config.ID)
	assert.True(t, retrievedTask.Config.EnableDropTableBeforeDDL)
}

// TestCreateTask_RetryRequiresStaging 验证 service 层入口契约：
// full_load_read_retry_times>0 且 full_load_enable_staging=false 时，
// CreateTask 必须在 ValidateFullLoadOptions 处被拒，且任务不得持久化。
func TestCreateTask_RetryRequiresStaging(t *testing.T) {
	ts := NewTaskService(newDefaultConfig())
	defer ts.Close()

	// Case A: retry>0 且 staging=false 必须在 CreateTask 入口被拒（fail-closed）。
	t.Run("rejects retry without staging", func(t *testing.T) {
		cfg := taskEntity.TaskConfig{
			ID:                     "retry-no-staging",
			Name:                   "retry no staging",
			SourceSchema:           "src",
			TargetSchema:           "tgt",
			Mode:                   taskEntity.SyncModeFull,
			FullLoadReadRetryTimes: 2,
			FullLoadEnableStaging:  false,
		}
		_, err := ts.CreateTask(cfg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "requires full_load_enable_staging")
		_, exists := ts.GetTask("retry-no-staging")
		assert.False(t, exists, "rejected task must not be persisted")
	})

	// Case B: retry>0 且 staging=true 应创建成功。
	t.Run("accepts retry with staging", func(t *testing.T) {
		cfg := taskEntity.TaskConfig{
			ID:                     "retry-with-staging",
			Name:                   "retry with staging",
			SourceSchema:           "src",
			TargetSchema:           "tgt",
			Mode:                   taskEntity.SyncModeFull,
			FullLoadReadRetryTimes: 2,
			FullLoadEnableStaging:  true,
		}
		task, err := ts.CreateTask(cfg)
		assert.NoError(t, err)
		assert.NotNil(t, task)
		_, exists := ts.GetTask("retry-with-staging")
		assert.True(t, exists)
	})
}

func TestGetTask(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// ??????????
	task, exists := ts.GetTask("non_existent")
	assert.False(t, exists)
	assert.Nil(t, task)

	// ????
	taskConfig := taskEntity.TaskConfig{
		ID:   "test_task_2",
		Name: "Test Task",
	}
	ts.CreateTask(taskConfig)

	// ?????????
	task, exists = ts.GetTask("test_task_2")
	assert.True(t, exists)
	assert.NotNil(t, task)
	assert.Equal(t, "test_task_2", task.Config.ID)
}

func TestGetAllTasks(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// ??????
	tasks := ts.GetAllTasks()
	assert.Empty(t, tasks)

	// ??????
	ts.CreateTask(taskEntity.TaskConfig{ID: "task_1_unique", Name: "Task 1"})
	ts.CreateTask(taskEntity.TaskConfig{ID: "task_2_unique", Name: "Task 2"})
	ts.CreateTask(taskEntity.TaskConfig{ID: "task_3_unique", Name: "Task 3"})

	// ??????
	tasks = ts.GetAllTasks()
	assert.Len(t, tasks, 3)

	taskIDs := make(map[string]bool)
	for _, task := range tasks {
		taskIDs[task.Config.ID] = true
	}

	assert.True(t, taskIDs["task_1_unique"])
	assert.True(t, taskIDs["task_2_unique"])
	assert.True(t, taskIDs["task_3_unique"])

	// ?????? live ??????????????
	live, ok := ts.GetTask("task_1_unique")
	require.True(t, ok)
	var snap *taskEntity.SyncTask
	for _, task := range tasks {
		if task.Config.ID == "task_1_unique" {
			snap = task
			break
		}
	}
	require.NotNil(t, snap)
	assert.NotSame(t, live, snap)
	snap.Context.Status = taskEntity.TaskStatusFailed
	assert.NotEqual(t, taskEntity.TaskStatusFailed, live.Context.Status)
}

func TestUpdateTask(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// ????
	taskConfig := taskEntity.TaskConfig{
		ID:   "test_task_update",
		Name: "Original Name",
	}
	task, _ := ts.CreateTask(taskConfig)

	// ?????????????
	task.Config.Name = "Updated Name"

	// ????
	err := ts.UpdateTask(task)
	assert.NoError(t, err)

	// ?????Config ????Context ??? live ??
	retrievedTask, _ := ts.GetTask("test_task_update")
	assert.Equal(t, "Updated Name", retrievedTask.Config.Name)
	assert.Equal(t, taskEntity.TaskStatusPending, retrievedTask.Context.Status)
}

func TestUpdateTask_RejectsRunning(t *testing.T) {
	dataDir := t.TempDir()
	ts := newTestTaskService(dataDir)

	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "upd_running", Name: "run"})
	require.NoError(t, err)
	task.Start()

	snap, ok := ts.GetTaskSnapshot("upd_running")
	require.True(t, ok)
	snap.Config.Name = "should-not-apply"

	err = ts.UpdateTask(snap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot update running")

	live, _ := ts.GetTask("upd_running")
	assert.Equal(t, "run", live.Config.Name)
	assert.Equal(t, taskEntity.TaskStatusRunning, live.Context.Status)
}

func TestUpdateTask_PreservesLiveContextFromSnapshot(t *testing.T) {
	dataDir := t.TempDir()
	ts := newTestTaskService(dataDir)

	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "upd_ctx", Name: "orig"})
	require.NoError(t, err)
	task.Context.TotalRows = 42
	task.Context.ProgressPercent = 12.5

	snap, ok := ts.GetTaskSnapshot("upd_ctx")
	require.True(t, ok)
	snap.Config.Name = "renamed"
	snap.Context.TotalRows = 0 // ?????? Context???????

	require.NoError(t, ts.UpdateTask(snap))

	live, _ := ts.GetTask("upd_ctx")
	assert.Equal(t, "renamed", live.Config.Name)
	assert.Equal(t, int64(42), live.Context.TotalRows)
	assert.InDelta(t, 12.5, live.Context.ProgressPercent, 0.01)
}

func TestUpdateTask_PersistsNopkAllRiskAcknowledgement(t *testing.T) {
	dataDir := t.TempDir()
	ts := newTestTaskService(dataDir)

	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "nopk_ack_upd", Name: "orig", Mode: taskEntity.SyncModeAll})
	require.NoError(t, err)
	assert.False(t, task.HasNopkAllRiskAcknowledgement())

	snap, ok := ts.GetTaskSnapshot("nopk_ack_upd")
	require.True(t, ok)
	snap.Config.AllowNopkAll = true
	// 模拟 handler：只改 Config，不碰 live Context。
	require.NoError(t, ts.UpdateTask(snap))

	live, _ := ts.GetTask("nopk_ack_upd")
	require.True(t, live.HasNopkAllRiskAcknowledgement())
	assert.True(t, live.Config.AllowNopkAll)
	firstAck := *live.Context.NopkAllRiskAcknowledgedAt

	// 再次勾选不应覆盖已有确认时间。
	snap2, ok := ts.GetTaskSnapshot("nopk_ack_upd")
	require.True(t, ok)
	snap2.Config.AllowNopkAll = true
	require.NoError(t, ts.UpdateTask(snap2))
	live2, _ := ts.GetTask("nopk_ack_upd")
	assert.Equal(t, firstAck, *live2.Context.NopkAllRiskAcknowledgedAt)

	// 取消勾选必须清除服务端时间戳。
	snap3, ok := ts.GetTaskSnapshot("nopk_ack_upd")
	require.True(t, ok)
	snap3.Config.AllowNopkAll = false
	require.NoError(t, ts.UpdateTask(snap3))
	live3, _ := ts.GetTask("nopk_ack_upd")
	assert.False(t, live3.HasNopkAllRiskAcknowledgement())
	assert.False(t, live3.Config.AllowNopkAll)
}

func TestDeleteTask_RejectsRunning(t *testing.T) {
	dataDir := t.TempDir()
	ts := newTestTaskService(dataDir)

	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "del_running", Name: "run"})
	require.NoError(t, err)
	task.Start()

	err = ts.DeleteTask("del_running")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot delete running")

	_, exists := ts.GetTask("del_running")
	assert.True(t, exists)
}

func TestUpdateTask_NotFound(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:   "non_existent",
		Name: "Test",
	})

	err := ts.UpdateTask(task)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task not found")
}

func TestDeleteTask(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// ????
	taskConfig := taskEntity.TaskConfig{
		ID:   "test_task_delete",
		Name: "Test Task",
	}
	ts.CreateTask(taskConfig)

	// ??????
	_, exists := ts.GetTask("test_task_delete")
	assert.True(t, exists)

	// ????
	err := ts.DeleteTask("test_task_delete")
	assert.NoError(t, err)

	// ???????
	_, exists = ts.GetTask("test_task_delete")
	assert.False(t, exists)
}

func TestDeleteTask_NotFound(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	err := ts.DeleteTask("non_existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task not found")
}

func TestStartTask(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer sourceDB.Close()

	targetDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	analyzer := &mockAnalyzer{}

	ts := NewTaskServiceWithDB(sourceDB, targetDB, analyzer)

	// ????
	taskConfig := taskEntity.TaskConfig{
		ID:           "test_task_start",
		Name:         "Test Task",
		SourceSchema: "source_db",
		TargetSchema: "target_db",
		Tables:       []string{"users"},
		Mode:         taskEntity.SyncModeFull,
		SourceDB: &taskEntity.DatabaseConfig{
			Host:     "127.0.0.1",
			Port:     3306,
			Database: "source_db",
			Username: "root",
			Password: "pwd",
		},
		TargetDB: &taskEntity.DatabaseConfig{
			Host:     "127.0.0.1",
			Port:     3306,
			Database: "target_db",
			Username: "root",
			Password: "pwd",
		},
	}
	ts.CreateTask(taskConfig)

	// ????????????????????????????????????
	ctx := context.Background()
	err = ts.StartTask(ctx, "test_task_start")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to initialize database connections")

	// ???????????
	task, _ := ts.GetTask("test_task_start")
	assert.Equal(t, taskEntity.TaskStatusPending, task.Context.Status)
}

func TestStartTask_NotFound(t *testing.T) {
	ts := NewTaskService(newDefaultConfig())

	ctx := context.Background()
	err := ts.StartTask(ctx, "non_existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task not found")
}

func TestStartTask_AlreadyRunning(t *testing.T) {
	ts := NewTaskService(newDefaultConfig())

	// ???????
	taskConfig := taskEntity.TaskConfig{
		ID:   "test_task_running",
		Name: "Test Task",
	}
	task, _ := ts.CreateTask(taskConfig)
	task.Start()

	ctx := context.Background()
	err := ts.StartTask(ctx, "test_task_running")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already running")
}

func TestStartTask_ConcurrentRuntimeIsolation(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)
	ts.runtimes = make(map[string]*taskRuntime)

	_, err := ts.CreateTask(taskEntity.TaskConfig{ID: "task_concurrent_1", Name: "Task Concurrent 1"})
	require.NoError(t, err)
	_, err = ts.CreateTask(taskEntity.TaskConfig{ID: "task_concurrent_2", Name: "Task Concurrent 2"})
	require.NoError(t, err)

	createdRuntimes := make(map[string]*taskRuntime)
	var createdMu sync.Mutex
	ts.initRuntimeFn = func(task *taskEntity.SyncTask) (*taskRuntime, error) {
		r := &taskRuntime{}
		createdMu.Lock()
		createdRuntimes[task.Config.ID] = r
		createdMu.Unlock()
		return r, nil
	}

	execStarted := make(chan string, 2)
	ts.executeSyncFn = func(_ context.Context, taskID string, _ *taskRuntime) {
		execStarted <- taskID
	}

	startErrCh := make(chan error, 2)
	var wg sync.WaitGroup
	for _, taskID := range []string{"task_concurrent_1", "task_concurrent_2"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			startErrCh <- ts.StartTask(context.Background(), id)
		}(taskID)
	}
	wg.Wait()
	close(startErrCh)

	for startErr := range startErrCh {
		assert.NoError(t, startErr)
	}

	received := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case taskID := <-execStarted:
			received[taskID] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting executeSync for task %d", i+1)
		}
	}
	assert.True(t, received["task_concurrent_1"])
	assert.True(t, received["task_concurrent_2"])

	ts.mu.RLock()
	runtime1 := ts.runtimes["task_concurrent_1"]
	runtime2 := ts.runtimes["task_concurrent_2"]
	ts.mu.RUnlock()

	require.NotNil(t, runtime1)
	require.NotNil(t, runtime2)
	assert.NotSame(t, runtime1, runtime2)

	createdMu.Lock()
	assert.Same(t, createdRuntimes["task_concurrent_1"], runtime1)
	assert.Same(t, createdRuntimes["task_concurrent_2"], runtime2)
	createdMu.Unlock()

	task1, ok1 := ts.GetTask("task_concurrent_1")
	task2, ok2 := ts.GetTask("task_concurrent_2")
	require.True(t, ok1)
	require.True(t, ok2)
	assert.Equal(t, taskEntity.TaskStatusRunning, task1.Context.Status)
	assert.Equal(t, taskEntity.TaskStatusRunning, task2.Context.Status)
}

func TestStartTask_SuccessPath(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)
	ts.runtimes = make(map[string]*taskRuntime)

	_, err := ts.CreateTask(taskEntity.TaskConfig{
		ID:   "task_success",
		Name: "Success Path Task",
		Mode: taskEntity.SyncModeFull,
	})
	require.NoError(t, err)

	// Inject fake runtime init
	fakeRuntime := &taskRuntime{}
	ts.initRuntimeFn = func(task *taskEntity.SyncTask) (*taskRuntime, error) {
		return fakeRuntime, nil
	}

	// Capture executeSync call
	type execCall struct {
		taskID  string
		runtime *taskRuntime
	}
	execCh := make(chan execCall, 1)
	ts.executeSyncFn = func(ctx context.Context, taskID string, rt *taskRuntime) {
		execCh <- execCall{taskID: taskID, runtime: rt}
	}

	// Act
	beforeStart := time.Now().Add(-time.Millisecond)
	err = ts.StartTask(context.Background(), "task_success")
	require.NoError(t, err)

	// Assert: runtime is stored
	ts.mu.RLock()
	storedRT := ts.runtimes["task_success"]
	ts.mu.RUnlock()
	assert.Same(t, fakeRuntime, storedRT, "runtime should be stored in runtimes map")

	// Assert: task status changed to Running with StartTime set
	task, ok := ts.GetTask("task_success")
	require.True(t, ok)
	assert.Equal(t, taskEntity.TaskStatusRunning, task.Context.Status)
	assert.False(t, task.Context.StartTime.IsZero(), "StartTime should be set")
	assert.True(t, task.Context.StartTime.After(beforeStart), "StartTime should be recent")

	// Assert: cancel function is wired on runtime
	assert.NotNil(t, fakeRuntime.cancel, "runtime.cancel should be set by StartTask")

	// Assert: executeSync was triggered asynchronously with correct args
	select {
	case call := <-execCh:
		assert.Equal(t, "task_success", call.taskID)
		assert.Same(t, fakeRuntime, call.runtime)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for executeSync to be called")
	}
}

func TestStartTask_SuccessPath_ExecuteSyncGetsIndependentContext(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)
	ts.runtimes = make(map[string]*taskRuntime)

	_, err := ts.CreateTask(taskEntity.TaskConfig{ID: "task_ctx", Name: "Ctx Task"})
	require.NoError(t, err)

	ts.initRuntimeFn = func(_ *taskEntity.SyncTask) (*taskRuntime, error) {
		return &taskRuntime{}, nil
	}

	// Capture the context passed to executeSync
	ctxCh := make(chan context.Context, 1)
	ts.executeSyncFn = func(ctx context.Context, _ string, _ *taskRuntime) {
		ctxCh <- ctx
	}

	// Use a cancellable "HTTP request" context
	httpCtx, httpCancel := context.WithCancel(context.Background())
	err = ts.StartTask(httpCtx, "task_ctx")
	require.NoError(t, err)

	// Cancel the HTTP context ? executeSync's context should remain alive
	httpCancel()

	select {
	case syncCtx := <-ctxCh:
		assert.NoError(t, syncCtx.Err(), "executeSync context should NOT be cancelled when HTTP ctx is cancelled")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for executeSync")
	}
}

func TestStartTask_SuccessPath_OldRuntimeClosed(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)
	ts.runtimes = make(map[string]*taskRuntime)

	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "task_restart", Name: "Restart Task"})
	require.NoError(t, err)

	// Simulate a pre-existing runtime with a cancel func to verify Close() is called
	oldCancelled := false
	oldRuntime := &taskRuntime{
		cancel: func() { oldCancelled = true },
	}
	ts.runtimes["task_restart"] = oldRuntime

	newRuntime := &taskRuntime{}
	ts.initRuntimeFn = func(_ *taskEntity.SyncTask) (*taskRuntime, error) {
		return newRuntime, nil
	}
	ts.executeSyncFn = func(_ context.Context, _ string, _ *taskRuntime) {}

	// Task must not be Running to pass the guard
	task.Context.Status = taskEntity.TaskStatusPaused

	err = ts.StartTask(context.Background(), "task_restart")
	require.NoError(t, err)

	// Old runtime should have been closed (cancel called)
	assert.True(t, oldCancelled, "old runtime cancel should be called during Close()")

	// New runtime replaces old
	ts.mu.RLock()
	assert.Same(t, newRuntime, ts.runtimes["task_restart"])
	ts.mu.RUnlock()
}

func TestStartTask_SuccessPath_AuditLoggerCalled(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)
	ts.runtimes = make(map[string]*taskRuntime)

	_, err := ts.CreateTask(taskEntity.TaskConfig{ID: "task_audit", Name: "Audit Task"})
	require.NoError(t, err)

	ts.initRuntimeFn = func(_ *taskEntity.SyncTask) (*taskRuntime, error) {
		return &taskRuntime{}, nil
	}
	ts.executeSyncFn = func(_ context.Context, _ string, _ *taskRuntime) {}

	// Wire up a real audit logger to a temp dir, then query it
	auditDir := t.TempDir()
	ts.auditLogger = audit.NewAuditLogger(auditDir)
	defer ts.auditLogger.Close()

	err = ts.StartTask(context.Background(), "task_audit")
	require.NoError(t, err)

	events, err := ts.auditLogger.Query(audit.QueryOptions{TaskID: "task_audit"})
	require.NoError(t, err)
	require.NotEmpty(t, events, "audit logger should have recorded a task-resumed event")
}

func TestStartTask_SuccessPath_StorageSaveCalled(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)
	ts.runtimes = make(map[string]*taskRuntime)

	_, err := ts.CreateTask(taskEntity.TaskConfig{ID: "task_save", Name: "Save Task"})
	require.NoError(t, err)

	ts.initRuntimeFn = func(_ *taskEntity.SyncTask) (*taskRuntime, error) {
		return &taskRuntime{}, nil
	}
	ts.executeSyncFn = func(_ context.Context, _ string, _ *taskRuntime) {}

	err = ts.StartTask(context.Background(), "task_save")
	require.NoError(t, err)

	// Reload from storage to confirm the Running state was persisted
	loaded, err := ts.storage.LoadAll()
	require.NoError(t, err)

	var found bool
	for _, lt := range loaded {
		if lt.Config.ID == "task_save" {
			found = true
			assert.Equal(t, taskEntity.TaskStatusRunning, lt.Context.Status,
				"persisted task should have Running status")
		}
	}
	assert.True(t, found, "task_save should exist in storage")
}

func TestPauseTask(t *testing.T) {
	ts := NewTaskService(newDefaultConfig())

	// ???????
	taskConfig := taskEntity.TaskConfig{
		ID:   "test_task_pause",
		Name: "Test Task",
	}
	task, _ := ts.CreateTask(taskConfig)
	task.Start()

	// ????
	err := ts.PauseTask("test_task_pause")
	assert.NoError(t, err)

	// ??????
	retrievedTask, _ := ts.GetTask("test_task_pause")
	assert.Equal(t, taskEntity.TaskStatusPaused, retrievedTask.Context.Status)
}

func TestPauseTask_NotFound(t *testing.T) {
	ts := NewTaskService(newDefaultConfig())

	err := ts.PauseTask("non_existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task not found")
}

func TestSkipError(t *testing.T) {
	ts := NewTaskService(newDefaultConfig())

	// ???????????
	taskConfig := taskEntity.TaskConfig{
		ID:   "test_task_skip",
		Name: "Test Task",
	}
	task, _ := ts.CreateTask(taskConfig)
	task.Context.Status = taskEntity.TaskStatusFailed
	task.Context.ErrorStack = "some error"

	// ????
	err := ts.SkipError("test_task_skip")
	assert.NoError(t, err)

	// ???????
	retrievedTask, _ := ts.GetTask("test_task_skip")
	assert.Equal(t, taskEntity.TaskStatusPaused, retrievedTask.Context.Status)
	assert.Empty(t, retrievedTask.Context.ErrorStack)
}

func TestSkipError_NotFound(t *testing.T) {
	ts := NewTaskService(newDefaultConfig())

	err := ts.SkipError("non_existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task not found")
}

func TestGetTaskMetrics(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// ????
	taskConfig := taskEntity.TaskConfig{
		ID:     "test_task_metrics_unique",
		Name:   "Test Task",
		Tables: []string{"users", "orders"},
	}
	task, _ := ts.CreateTask(taskConfig)
	task.Context.ProcessedRows = 1000
	task.Context.TotalRows = 2000
	task.Context.CurrentPosition = "position_1"
	// ?????????
	task.Context.ProgressPercent = 50.0

	// ????
	metrics, err := ts.GetTaskMetrics("test_task_metrics_unique")
	assert.NoError(t, err)
	assert.NotNil(t, metrics)
	assert.Equal(t, int64(1000), metrics["processed_rows"])
	assert.Equal(t, int64(2000), metrics["total_rows"])
	assert.Equal(t, 50.0, metrics["progress_percent"])
	assert.Equal(t, 0, metrics["tables_completed"])
	assert.Equal(t, 2, metrics["tables_total"])
	assert.Equal(t, "position_1", metrics["current_position"])
}

func TestGetTaskMetrics_NotFound(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	_, err := ts.GetTaskMetrics("non_existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task not found")
}

func TestGetRunningTaskCount(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// ????? 0
	count := ts.GetRunningTaskCount()
	assert.Equal(t, 0, count)

	// ?????????
	task1, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1_unique", Name: "Task 1"})
	task1.Start()

	task2, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_2_unique", Name: "Task 2"})
	task2.Start()

	_, _ = ts.CreateTask(taskEntity.TaskConfig{ID: "task_3_unique", Name: "Task 3"})
	// task3 ??????

	count = ts.GetRunningTaskCount()
	assert.Equal(t, 2, count)
}

func TestClose(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer sourceDB.Close()

	targetDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	analyzer := &mockAnalyzer{}

	ts := NewTaskServiceWithDB(sourceDB, targetDB, analyzer)

	// ??????
	task1, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})
	task1.Start()

	task2, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_2", Name: "Task 2"})
	task2.Start()

	// ????
	err = ts.Close()
	assert.NoError(t, err)

	// ???????????
	retrievedTask1, _ := ts.GetTask("task_1")
	assert.Equal(t, taskEntity.TaskStatusPaused, retrievedTask1.Context.Status)

	retrievedTask2, _ := ts.GetTask("task_2")
	assert.Equal(t, taskEntity.TaskStatusPaused, retrievedTask2.Context.Status)
}

func TestGenerateServerID(t *testing.T) {
	tests := []struct {
		name     string
		taskID   string
		expected uint32
	}{
		{"simple", "task_1", 0},
		{"empty", "", 1},
		{"complex", "task_with_long_name_12345", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serverID := generateServerID(tt.taskID)
			assert.NotZero(t, serverID)
			if tt.taskID == "" {
				assert.Equal(t, tt.expected, serverID)
			}
		})
	}
}

func TestIsTaskStopped(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// ????????
	assert.True(t, ts.isTaskStopped("non_existent"))

	// ????
	task, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})

	// ????????
	assert.True(t, ts.isTaskStopped("task_1"))

	// ????
	task.Start()

	// ????????
	assert.False(t, ts.isTaskStopped("task_1"))
}

func TestUpdateTaskProgress(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// ????
	_, _ = ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})

	// ????
	ts.updateTaskProgress("task_1", 100, "position_1")

	// ????
	retrievedTask, _ := ts.GetTask("task_1")
	assert.Equal(t, int64(100), retrievedTask.Context.ProcessedRows)
	assert.Equal(t, "position_1", retrievedTask.Context.CurrentPosition)
}

func TestIncrementTaskProgress(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// ????
	task, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})
	task.Context.TotalRows = 1000

	// ????
	ts.incrementTaskProgress("task_1", 100, "position_1")
	ts.incrementTaskProgress("task_1", 200, "position_2")

	// ????
	retrievedTask, _ := ts.GetTask("task_1")
	assert.Equal(t, int64(300), retrievedTask.Context.ProcessedRows)
	assert.Equal(t, "position_2", retrievedTask.Context.CurrentPosition)
	assert.Equal(t, 30.0, retrievedTask.Context.ProgressPercent)
}

// TestIncrementTaskProgress_ReturnsEstimatedTotal ???? TotalRows ?????
// incrementTaskProgress ?? EstimatedTotalRows ???? ETA ???
func TestIncrementTaskProgress_ReturnsEstimatedTotal(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	task, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_eta", Name: "Task ETA"})
	// ?? COUNT(*) ??????????
	task.Context.TotalRows = 0
	task.Context.EstimatedTotalRows = 5000

	total := ts.incrementTaskProgress("task_eta", 100, "pos_1")

	assert.Equal(t, int64(5000), total, "should return EstimatedTotalRows when TotalRows is 0")

	// ?? COUNT(*) ?????????
	task.Context.TotalRows = 6000
	total = ts.incrementTaskProgress("task_eta", 200, "pos_2")
	assert.Equal(t, int64(6000), total, "should return TotalRows when it is set")
}

// spyTaskStorage ???? FileTaskStorage??? Save ?????????????????
type spyTaskStorage struct {
	*FileTaskStorage
	saveCount int
	mu        sync.Mutex
}

func (s *spyTaskStorage) Save(task *taskEntity.SyncTask) error {
	s.mu.Lock()
	s.saveCount++
	s.mu.Unlock()
	return s.FileTaskStorage.Save(task)
}

func (s *spyTaskStorage) SaveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveCount
}

func (s *spyTaskStorage) ResetCount() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveCount = 0
}

// newTestTaskServiceWithSpy ??? spy storage ?????????? Save ?????
func newTestTaskServiceWithSpy(dataDir string) (*TaskService, *spyTaskStorage) {
	real := NewFileTaskStorage(dataDir)
	spy := &spyTaskStorage{FileTaskStorage: real}
	return &TaskService{
		tasks:               make(map[string]*taskEntity.SyncTask),
		runtimes:            make(map[string]*taskRuntime),
		runningProgress:     make(map[string]*taskEntity.RunningProgress),
		lastProgressPersist: make(map[string]time.Time),
		storage:             spy,
	}, spy
}

// TestIncrementTaskProgress_Throttle ??????????
// 1. ?????? incrementTaskProgress ????? Save
// 2. ?? 1 ??????????? Save
func TestIncrementTaskProgress_Throttle(t *testing.T) {
	dataDir := t.TempDir()

	ts, spy := newTestTaskServiceWithSpy(dataDir)

	// ?????CreateTask ??????? Save???????
	task, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})
	task.Context.TotalRows = 1000
	spy.ResetCount()

	// ? 1 ?????????? Save
	ts.incrementTaskProgress("task_1", 100, "pos_1")
	assert.Equal(t, 1, spy.SaveCount(), "??????? Save")

	// ? 2 ?????????????? Save
	ts.incrementTaskProgress("task_1", 200, "pos_2")
	assert.Equal(t, 1, spy.SaveCount(), "1 ??????????? Save")

	// ? 3 ?????????????? Save
	ts.incrementTaskProgress("task_1", 50, "pos_3")
	assert.Equal(t, 1, spy.SaveCount(), "1 ??????????? Save")

	// ??????????????????
	retrievedTask, _ := ts.GetTask("task_1")
	assert.Equal(t, int64(350), retrievedTask.Context.ProcessedRows)
	assert.Equal(t, "pos_3", retrievedTask.Context.CurrentPosition)
	assert.Equal(t, 35.0, retrievedTask.Context.ProgressPercent)

	// ???? 1 ??????????? Save
	time.Sleep(1100 * time.Millisecond)
	ts.incrementTaskProgress("task_1", 150, "pos_4")
	assert.Equal(t, 2, spy.SaveCount(), "?? 1 ??????? Save")

	// ?????????
	retrievedTask, _ = ts.GetTask("task_1")
	assert.Equal(t, int64(500), retrievedTask.Context.ProcessedRows)
	assert.Equal(t, "pos_4", retrievedTask.Context.CurrentPosition)
	assert.Equal(t, 50.0, retrievedTask.Context.ProgressPercent)
}

// TestIncrementTaskProgress_ThrottleReset ???????????????
// ???? taskID ???????????????
func TestIncrementTaskProgress_ThrottleReset(t *testing.T) {
	dataDir := t.TempDir()

	ts, spy := newTestTaskServiceWithSpy(dataDir)

	// ????
	task, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})
	task.Context.TotalRows = 1000
	spy.ResetCount()

	// ?????????? progress
	ts.incrementTaskProgress("task_1", 100, "pos_1")
	assert.Equal(t, 1, spy.SaveCount())

	// ?????????????s.mu ????????progressMu ?????????
	ts.mu.Lock()
	ts.clearLastProgressPersistLocked("task_1")
	ts.mu.Unlock()
	ts.clearRunningProgress("task_1")

	// ?? lastProgressPersist ???
	_, exists := ts.lastProgressPersist["task_1"]
	assert.False(t, exists, "???????? lastProgressPersist ??")

	// ??????????? taskID ???????
	task2, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})
	task2.Context.TotalRows = 1000
	spy.ResetCount()

	// ?????????? Save????????????
	ts.incrementTaskProgress("task_1", 200, "pos_2")
	assert.Equal(t, 1, spy.SaveCount(), "?????????????????? Save")
}

// TestIncrementTaskProgress_SkipsStaleSnapshotAfterLifecycleSave ?? Pause ?????????
// ??? RUNNING ??????????????? PAUSED/STOPPED ??? RUNNING?
func TestIncrementTaskProgress_SkipsStaleSnapshotAfterLifecycleSave(t *testing.T) {
	dataDir := t.TempDir()

	ts, spy := newTestTaskServiceWithSpy(dataDir)

	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})
	require.NoError(t, err)
	task.Start()
	task.Context.TotalRows = 1000
	spy.ResetCount()

	// ?? incrementTaskProgress ?????? Save ??? RUNNING ???
	ts.mu.Lock()
	task.Context.ProcessedRows = 100
	task.Context.CurrentPosition = "pos_stale"
	staleTime := time.Now().Add(-2 * time.Second)
	task.Context.LastUpdateTime = staleTime
	staleJSON, marshalErr := json.Marshal(task)
	require.NoError(t, marshalErr)
	var staleSnapshot taskEntity.SyncTask
	require.NoError(t, json.Unmarshal(staleJSON, &staleSnapshot))
	task.Pause()
	ts.mu.Unlock()

	require.NoError(t, ts.storage.Save(task))
	spy.ResetCount()

	assert.False(t, ts.shouldPersistAsyncProgressSnapshot("task_1", &staleSnapshot))
	if ts.shouldPersistAsyncProgressSnapshot("task_1", &staleSnapshot) {
		_ = ts.storage.Save(&staleSnapshot)
	}
	assert.Equal(t, 0, spy.SaveCount(), "stale snapshot must not be persisted")

	loadedTasks, loadErr := ts.storage.LoadAll()
	require.NoError(t, loadErr)
	require.Len(t, loadedTasks, 1)
	loaded := loadedTasks[0]
	assert.Equal(t, taskEntity.TaskStatusPaused, loaded.Context.Status)
	assert.Equal(t, int64(100), loaded.Context.ProcessedRows)
}

// TestDeleteTask_CleansThrottleRecord ?? DeleteTask ??? lastProgressPersist?
func TestDeleteTask_CleansThrottleRecord(t *testing.T) {
	dataDir := t.TempDir()

	ts, spy := newTestTaskServiceWithSpy(dataDir)

	// ????
	_, _ = ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})
	spy.ResetCount()

	// ???? progress ?? lastProgressPersist ?????
	ts.incrementTaskProgress("task_1", 100, "pos_1")
	assert.Equal(t, 1, spy.SaveCount())

	// ??????
	_, exists := ts.lastProgressPersist["task_1"]
	assert.True(t, exists, "?? incrementTaskProgress ????????")

	// ????
	err := ts.DeleteTask("task_1")
	assert.NoError(t, err)

	// ??????????
	_, exists = ts.lastProgressPersist["task_1"]
	assert.False(t, exists, "DeleteTask ??? lastProgressPersist ??")
}

func TestUpdateTaskTotalRows(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// ????
	_, _ = ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})

	// ??????
	ts.updateTaskEstimatedRows("task_1", 5000)

	// ????????
	retrievedTask, _ := ts.GetTask("task_1")
	assert.Equal(t, int64(5000), retrievedTask.Context.EstimatedTotalRows)
}

func TestUpdateTaskStatus(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// ????
	_, _ = ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})

	// ????
	ts.updateTaskStatus("task_1", taskEntity.TaskStatusFailed, "test error")

	// ????
	retrievedTask, _ := ts.GetTask("task_1")
	assert.Equal(t, taskEntity.TaskStatusFailed, retrievedTask.Context.Status)
	assert.Equal(t, "test error", retrievedTask.Context.ErrorStack)
	assert.NotNil(t, retrievedTask.Context.EndTime)
}

func TestCompleteTask(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// ???????
	task, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})
	task.Start()

	// ????
	ts.completeTask("task_1")

	// ????
	retrievedTask, _ := ts.GetTask("task_1")
	assert.Equal(t, taskEntity.TaskStatusCompleted, retrievedTask.Context.Status)
	assert.NotNil(t, retrievedTask.Context.EndTime)
}

func TestTaskStorage_Save_Error(t *testing.T) {
	// ???????????????????
	storage := NewFileTaskStorage("invalid:dir")

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:   "test_task",
		Name: "Test Task",
	})

	err := storage.Save(task)
	assert.Error(t, err)
}

func TestTaskStorage_LoadAll_InvalidJSON(t *testing.T) {
	dataDir := t.TempDir()

	storage := NewFileTaskStorage(dataDir)

	// ??????? JSON ??
	invalidJSON := `{"invalid": json}`
	filePath := dataDir + "/invalid.json"
	os.WriteFile(filePath, []byte(invalidJSON), 0644)

	// ??????????
	tasks, err := storage.LoadAll()
	assert.NoError(t, err)
	assert.Empty(t, tasks)
}

func TestTaskStorage_LoadAll_ReadError(t *testing.T) {
	dataDir := t.TempDir()

	storage := NewFileTaskStorage(dataDir)

	// ???????????
	dirPath := dataDir + "/subdir"
	os.MkdirAll(dirPath, 0755)

	// ????????
	tasks, err := storage.LoadAll()
	assert.NoError(t, err)
	assert.Empty(t, tasks)
}

func TestTaskStorage_NewTaskStorage_Error(t *testing.T) {
	// ???????????
	// ?? os.MkdirAll ?????????????????????? panic
	storage := NewFileTaskStorage("data")
	assert.NotNil(t, storage)
}

func TestTaskService_ConcurrentOperations(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// ??????
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			taskID := "concurrent_task_unique_" + string(rune('0'+id))
			ts.CreateTask(taskEntity.TaskConfig{
				ID:   taskID,
				Name: "Concurrent Task",
			})
			done <- true
		}(i)
	}

	// ????????
	for i := 0; i < 10; i++ {
		<-done
	}

	// ??????????
	tasks := ts.GetAllTasks()
	assert.Equal(t, 10, len(tasks))
}

// ==================== syncReadBatchLimit ====================

func TestSyncReadBatchLimit(t *testing.T) {
	tests := []struct {
		name      string
		batchSize int
		expected  int64
	}{
		{"zero returns default 1000", 0, 1000},
		{"negative returns default 1000", -1, 1000},
		{"small positive returns as-is", 500, 500},
		{"default boundary", 1000, 1000},
		{"large value capped at 100000", 200000, 100000},
		{"exact hard max", 100000, 100000},
		{"one", 1, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := syncReadBatchLimit(tt.batchSize)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// ==================== adjustReadLimitForWideColumns ====================

func TestAdjustReadLimitForWideColumns(t *testing.T) {
	t.Run("nil identity returns base", func(t *testing.T) {
		assert.Equal(t, int64(1000), adjustReadLimitForWideColumns(1000, nil))
	})

	t.Run("empty columns returns base", func(t *testing.T) {
		identity := &entity.TableIdentity{Columns: []entity.ColumnMeta{}}
		assert.Equal(t, int64(1000), adjustReadLimitForWideColumns(1000, identity))
	})

	t.Run("no heavy columns returns base", func(t *testing.T) {
		identity := &entity.TableIdentity{
			Columns: []entity.ColumnMeta{
				{Name: "id", DataType: "bigint"},
				{Name: "name", DataType: "varchar(255)"},
			},
		}
		assert.Equal(t, int64(1000), adjustReadLimitForWideColumns(1000, identity))
	})

	t.Run("one json column reduces limit", func(t *testing.T) {
		identity := &entity.TableIdentity{
			Columns: []entity.ColumnMeta{
				{Name: "id", DataType: "bigint"},
				{Name: "data", DataType: "json"},
			},
		}
		result := adjustReadLimitForWideColumns(1000, identity)
		assert.True(t, result < 1000)
		assert.True(t, result >= 25)
	})

	t.Run("multiple heavy columns reduce more", func(t *testing.T) {
		identity := &entity.TableIdentity{
			Columns: []entity.ColumnMeta{
				{Name: "id", DataType: "bigint"},
				{Name: "data", DataType: "json"},
				{Name: "content", DataType: "longtext"},
				{Name: "avatar", DataType: "blob"},
			},
		}
		result := adjustReadLimitForWideColumns(1000, identity)
		assert.True(t, result < 500)
		assert.True(t, result >= 25)
	})

	t.Run("small base does not go below 25", func(t *testing.T) {
		identity := &entity.TableIdentity{
			Columns: []entity.ColumnMeta{
				{Name: "data", DataType: "longblob"},
				{Name: "content", DataType: "mediumtext"},
				{Name: "extra", DataType: "tinyblob"},
			},
		}
		result := adjustReadLimitForWideColumns(50, identity)
		assert.Equal(t, int64(25), result)
	})

	t.Run("various blob/text types recognized", func(t *testing.T) {
		for _, dt := range []string{"json", "blob", "tinyblob", "mediumblob", "longblob", "text", "tinytext", "mediumtext", "longtext"} {
			identity := &entity.TableIdentity{
				Columns: []entity.ColumnMeta{
					{Name: "id", DataType: "int"},
					{Name: "col", DataType: dt},
				},
			}
			result := adjustReadLimitForWideColumns(1000, identity)
			assert.True(t, result < 1000, "data type %s should be recognized as heavy", dt)
		}
	})
}

// ==================== extractTableDefinition ====================

func TestExtractTableDefinition(t *testing.T) {
	t.Run("normal create table", func(t *testing.T) {
		sql := "CREATE TABLE `users` (\n  `id` bigint NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB"
		result := extractTableDefinition(sql)
		assert.True(t, strings.HasPrefix(result, "("))
		assert.Contains(t, result, "PRIMARY KEY")
		assert.Contains(t, result, "ENGINE=InnoDB")
	})

	t.Run("no parentheses returns original", func(t *testing.T) {
		sql := "SOMETHING WEIRD"
		assert.Equal(t, sql, extractTableDefinition(sql))
	})

	t.Run("only opening paren", func(t *testing.T) {
		sql := "CREATE TABLE `t` (incomplete"
		result := extractTableDefinition(sql)
		assert.Equal(t, sql, result)
	})
}

// ==================== isSecondaryIndexDefinitionLine ====================

func TestIsSecondaryIndexDefinitionLine(t *testing.T) {
	tests := []struct {
		line     string
		expected bool
	}{
		{"  UNIQUE KEY `uk_email` (`email`),", true},
		{"  UNIQUE INDEX `ui_email` (`email`),", true},
		{"  KEY `idx_name` (`name`),", true},
		{"  INDEX `idx_name` (`name`),", true},
		{"  FULLTEXT KEY `ft_content` (`content`),", true},
		{"  FULLTEXT INDEX `ft_content` (`content`),", true},
		{"  SPATIAL KEY `sp_geo` (`geo`),", true},
		{"  SPATIAL INDEX `sp_geo` (`geo`),", true},
		{"  PRIMARY KEY (`id`),", false},
		{"  `id` bigint NOT NULL,", false},
		{"  ) ENGINE=InnoDB", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.line, func(t *testing.T) {
			assert.Equal(t, tt.expected, isSecondaryIndexDefinitionLine(tt.line))
		})
	}
}

// ==================== trimTrailingComma ====================

func TestTrimTrailingComma(t *testing.T) {
	assert.Equal(t, "  PRIMARY KEY (`id`)", trimTrailingComma("  PRIMARY KEY (`id`),"))
	assert.Equal(t, "  PRIMARY KEY (`id`)", trimTrailingComma("  PRIMARY KEY (`id`)"))
	assert.Equal(t, "", trimTrailingComma(","))
	assert.Equal(t, "", trimTrailingComma(""))
	assert.Equal(t, "abc", trimTrailingComma("abc,  "))
}

// ==================== toInt64PK ====================

func TestToInt64PK(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int64
		ok       bool
	}{
		{"int", int(42), 42, true},
		{"int8", int8(8), 8, true},
		{"int16", int16(16), 16, true},
		{"int32", int32(32), 32, true},
		{"int64", int64(64), 64, true},
		{"uint", uint(10), 10, true},
		{"uint8", uint8(8), 8, true},
		{"uint16", uint16(16), 16, true},
		{"uint32", uint32(32), 32, true},
		{"uint64 small", uint64(100), 100, true},
		{"uint64 overflow", uint64(^uint64(0)), 0, false},
		{"string numeric", "12345", 12345, true},
		{"string non-numeric", "abc", 0, false},
		{"[]byte numeric", []byte("999"), 999, true},
		{"[]byte non-numeric", []byte("xyz"), 0, false},
		{"nil", nil, 0, false},
		{"float64", float64(3.14), 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := toInt64PK(tt.input)
			assert.Equal(t, tt.ok, ok)
			if ok {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// ==================== comparePKValues ====================

func TestComparePKValues(t *testing.T) {
	// int64 comparison
	assert.Equal(t, -1, comparePKValues(int64(1), int64(2)))
	assert.Equal(t, 0, comparePKValues(int64(5), int64(5)))
	assert.Equal(t, 1, comparePKValues(int64(10), int64(3)))

	// string comparison
	assert.Equal(t, -1, comparePKValues("abc", "def"))
	assert.Equal(t, 0, comparePKValues("same", "same"))
	assert.Equal(t, 1, comparePKValues("z", "a"))
	assert.Equal(t, 0, comparePKValues("556273a8b5467ad9b287b74fab12b346", []byte("556273a8b5467ad9b287b74fab12b346")))
	assert.Equal(t, -1, comparePKValues("556273a8b5467ad9b287b74fab12b346", []byte("5580fe5946606ff3a02b8a99baea9efc")))

	// mixed: both convertible to int64
	assert.Equal(t, 0, comparePKValues(int64(42), "42"))

	// fallback to string comparison
	assert.Equal(t, -1, comparePKValues(nil, "something"))
}

// ==================== comparePKWithBoundary ====================

func TestComparePKWithBoundary(t *testing.T) {
	t.Run("nil boundary returns -1", func(t *testing.T) {
		row := map[string]interface{}{"id": int64(1)}
		assert.Equal(t, -1, comparePKWithBoundary([]string{"id"}, row, nil))
	})

	t.Run("single column boundary less (string comparison)", func(t *testing.T) {
		row := map[string]interface{}{"id": "10"}
		assert.Equal(t, -1, comparePKWithBoundary([]string{"id"}, row, "20"))
	})

	t.Run("single column byte boundary compares as string", func(t *testing.T) {
		row := map[string]interface{}{"id": "556273a8b5467ad9b287b74fab12b346"}
		assert.Equal(t, -1, comparePKWithBoundary([]string{"id"}, row, []byte("5580fe5946606ff3a02b8a99baea9efc")))
		assert.Equal(t, 0, comparePKWithBoundary([]string{"id"}, row, []byte("556273a8b5467ad9b287b74fab12b346")))
	})

	t.Run("single column boundary equal", func(t *testing.T) {
		row := map[string]interface{}{"id": "10"}
		assert.Equal(t, 0, comparePKWithBoundary([]string{"id"}, row, "10"))
	})

	t.Run("single column boundary greater", func(t *testing.T) {
		row := map[string]interface{}{"id": "30"}
		assert.Equal(t, 1, comparePKWithBoundary([]string{"id"}, row, "20"))
	})

	t.Run("single column uses string repr of int64", func(t *testing.T) {
		// fmt.Sprintf("%v", int64(5)) = "5", fmt.Sprintf("%v", int64(10)) = "10"
		// "5" > "10" lexicographically, so result is 1 (not -1)
		row := map[string]interface{}{"id": int64(5)}
		assert.Equal(t, 1, comparePKWithBoundary([]string{"id"}, row, int64(10)))
	})

	t.Run("composite boundary less on first col", func(t *testing.T) {
		row := map[string]interface{}{"a": "1", "b": "5"}
		boundary := []interface{}{"2", "3"}
		assert.Equal(t, -1, comparePKWithBoundary([]string{"a", "b"}, row, boundary))
	})

	t.Run("composite boundary equal", func(t *testing.T) {
		row := map[string]interface{}{"a": "2", "b": "3"}
		boundary := []interface{}{"2", "3"}
		assert.Equal(t, 0, comparePKWithBoundary([]string{"a", "b"}, row, boundary))
	})

	t.Run("composite boundary greater on second col", func(t *testing.T) {
		row := map[string]interface{}{"a": "2", "b": "9"}
		boundary := []interface{}{"2", "3"}
		assert.Equal(t, 1, comparePKWithBoundary([]string{"a", "b"}, row, boundary))
	})
}

// ==================== boundaryToString ====================

func TestBoundaryToString(t *testing.T) {
	assert.Equal(t, "", boundaryToString(nil))
	assert.Equal(t, "42", boundaryToString(42))
	assert.Equal(t, "hello", boundaryToString("hello"))
	assert.Equal(t, "hello", boundaryToString([]byte("hello")))
	// composite
	result := boundaryToString([]interface{}{"a", []byte("b"), "c"})
	assert.Equal(t, "a\x00b\x00c", result)
}

// TestSamplePKBoundariesImproved_KeysetStepsForAllWorkers ?? keyset ?????
// ????? OFFSET????? (n-1) ? WHERE pk > ? ... LIMIT ? ?????????????
func TestSamplePKBoundariesImproved_KeysetStepsForAllWorkers(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	const workers = 4
	const estimatedRows int64 = 40 // step = 40/4 = 10
	const step int64 = 10

	// Batch 1??????
	firstQuery := "SELECT `id` FROM `src`.`events` ORDER BY `id` ASC LIMIT ?"
	rows1 := sqlmock.NewRows([]string{"id"})
	for i := int64(1); i <= step; i++ {
		rows1.AddRow(fmt.Sprintf("%03d", i))
	}
	mock.ExpectQuery(firstQuery).WithArgs(step).WillReturnRows(rows1)

	// Batch 2..n-1????????????????? n-1 ???????? mock n-2 ?
	for b := 1; b < workers-1; b++ {
		lastID := fmt.Sprintf("%03d", int64(b)*step)
		query := "SELECT `id` FROM `src`.`events` WHERE `id` > ? ORDER BY `id` ASC LIMIT ?"
		rows := sqlmock.NewRows([]string{"id"})
		for i := int64(1); i <= step; i++ {
			rows.AddRow(fmt.Sprintf("%03d", int64(b)*step+i))
		}
		mock.ExpectQuery(query).WithArgs(lastID, step).WillReturnRows(rows)
	}

	var ts TaskService
	boundaries, err := ts.samplePKBoundariesImproved(context.Background(), db, "src", "events", []string{"id"}, estimatedRows, workers)
	require.NoError(t, err)
	require.Len(t, boundaries, workers-1)
	for i := 1; i < len(boundaries); i++ {
		assert.Less(t, compareBoundaryValues(boundaries[i-1], boundaries[i]), 0)
	}
	// ????? "030"????? step*3 ??
	assert.Equal(t, "030", boundaries[workers-2])
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestSamplePKBoundariesImproved_EstimatedRowsTooSmall ?????? n*2 ????????
// ?????????????????? keyset ???
func TestSamplePKBoundariesImproved_EstimatedRowsTooSmall(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	var ts TaskService
	// estimatedRows=5, n=4??? >= n*2=8????????????
	_, err = ts.samplePKBoundariesImproved(context.Background(), db, "src", "events", []string{"id"}, 5, 4)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient rows")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestSamplePKBoundariesImproved_EstimatedTooLargeConvergesAtTableEnd ???????
// ???? rowsRead < step ?????? worker ???????????? intraWorkers?
func TestSamplePKBoundariesImproved_EstimatedTooLargeConvergesAtTableEnd(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	const workers = 4
	const estimatedRows int64 = 400 // step = 100????????
	const step int64 = 100

	// Batch 1: ? batch
	rows1 := sqlmock.NewRows([]string{"id"})
	for i := int64(1); i <= step; i++ {
		rows1.AddRow(fmt.Sprintf("%03d", i))
	}
	mock.ExpectQuery("SELECT `id` FROM `src`.`events` ORDER BY `id` ASC LIMIT ?").
		WithArgs(step).
		WillReturnRows(rows1)

	// Batch 2: ? batch
	rows2 := sqlmock.NewRows([]string{"id"})
	for i := int64(1); i <= step; i++ {
		rows2.AddRow(fmt.Sprintf("%03d", step+i))
	}
	mock.ExpectQuery("SELECT `id` FROM `src`.`events` WHERE `id` > ? ORDER BY `id` ASC LIMIT ?").
		WithArgs("100", step).
		WillReturnRows(rows2)

	// Batch 3: ?? 30 ??rowsRead < step?????
	rows3 := sqlmock.NewRows([]string{"id"})
	for i := int64(1); i <= 30; i++ {
		rows3.AddRow(fmt.Sprintf("%03d", 2*step+i))
	}
	mock.ExpectQuery("SELECT `id` FROM `src`.`events` WHERE `id` > ? ORDER BY `id` ASC LIMIT ?").
		WithArgs("200", step).
		WillReturnRows(rows3)

	// Batch 4 ???????????

	var ts TaskService
	boundaries, err := ts.samplePKBoundariesImproved(context.Background(), db, "src", "events", []string{"id"}, estimatedRows, workers)
	require.NoError(t, err)
	// ???? = 230, step=100???? 2 ????100, 200???? worker=3
	require.Len(t, boundaries, 2)
	assert.Equal(t, "100", boundaries[0])
	assert.Equal(t, "200", boundaries[1])
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestSamplePKBoundariesImproved_CompositeKeysetSteps ???? keyset ???
// ?? buildKeysetCompositeWhere ???? (pk1,pk2) > (v1,v2) ? OR ????
// ???? []interface{} ???? comparePKWithBoundary ???????
func TestSamplePKBoundariesImproved_CompositeKeysetSteps(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	const workers = 3
	const estimatedRows int64 = 30
	const step int64 = 10

	// Batch 1????????? = ["002", "b"]
	batch1 := [][]driver.Value{
		{"001", "a"}, {"001", "b"}, {"001", "c"}, {"001", "d"}, {"001", "e"},
		{"001", "f"}, {"001", "g"}, {"001", "h"}, {"001", "i"}, {"002", "b"},
	}
	rows1 := sqlmock.NewRows([]string{"id1", "id2"})
	for _, r := range batch1 {
		rows1.AddRow(r...)
	}
	mock.ExpectQuery("SELECT `id1`, `id2` FROM `src`.`events` ORDER BY `id1`, `id2` ASC LIMIT ?").
		WithArgs(step).
		WillReturnRows(rows1)

	// Batch 2: WHERE (id1=id1_v AND id2>id2_v) OR (id1>id1_v)
	// args: "002", "b", "002"
	batch2 := [][]driver.Value{
		{"002", "c"}, {"002", "d"}, {"002", "e"}, {"002", "f"}, {"002", "g"},
		{"002", "h"}, {"002", "i"}, {"002", "j"}, {"003", "a"}, {"003", "b"},
	}
	rows2 := sqlmock.NewRows([]string{"id1", "id2"})
	for _, r := range batch2 {
		rows2.AddRow(r...)
	}
	mock.ExpectQuery("SELECT `id1`, `id2` FROM `src`.`events` WHERE (`id1` = ? AND `id2` > ?) OR (`id1` > ?) ORDER BY `id1`, `id2` ASC LIMIT ?").
		WithArgs("002", "b", "002", step).
		WillReturnRows(rows2)

	var ts TaskService
	boundaries, err := ts.samplePKBoundariesImproved(context.Background(), db, "src", "events", []string{"id1", "id2"}, estimatedRows, workers)
	require.NoError(t, err)
	require.Len(t, boundaries, workers-1)

	// ??? ["002","b"] ? ["003","b"]??? []interface{} ??
	expected := []interface{}{
		[]interface{}{"002", "b"},
		[]interface{}{"003", "b"},
	}
	assert.Equal(t, expected, boundaries)
	require.NoError(t, mock.ExpectationsWereMet())
}

// ==================== isRetryableLockError ====================

func TestIsRetryableLockError(t *testing.T) {
	assert.False(t, isRetryableLockError(nil))
	assert.False(t, isRetryableLockError(fmt.Errorf("some random error")))
	assert.True(t, isRetryableLockError(fmt.Errorf("Error 1205: Lock wait timeout exceeded")))
	assert.True(t, isRetryableLockError(fmt.Errorf("Error 1213: Deadlock found when trying to get lock")))
	assert.True(t, isRetryableLockError(fmt.Errorf("Lock wait timeout")))
	assert.True(t, isRetryableLockError(fmt.Errorf("Deadlock found")))
}

// ==================== isNumericPKColumn ====================

func TestIsNumericPKColumn(t *testing.T) {
	identity := &entity.TableIdentity{
		Columns: []entity.ColumnMeta{
			{Name: "id", DataType: "bigint"},
			{Name: "name", DataType: "varchar"},
			{Name: "age", DataType: "int"},
			{Name: "code", DataType: "smallint"},
			{Name: "tiny", DataType: "tinyint"},
			{Name: "mid", DataType: "mediumint"},
		},
	}

	assert.True(t, isNumericPKColumn(identity, "id"))
	assert.False(t, isNumericPKColumn(identity, "name"))
	assert.True(t, isNumericPKColumn(identity, "age"))
	assert.True(t, isNumericPKColumn(identity, "code"))
	assert.True(t, isNumericPKColumn(identity, "tiny"))
	assert.True(t, isNumericPKColumn(identity, "mid"))
	assert.False(t, isNumericPKColumn(identity, "nonexistent"))
}

// ==================== dbScanToString ====================

func TestDbScanToString(t *testing.T) {
	assert.Equal(t, "", dbScanToString(nil))
	assert.Equal(t, "hello", dbScanToString([]byte("hello")))
	assert.Equal(t, "world", dbScanToString("world"))
	assert.Equal(t, "42", dbScanToString(int64(42)))
	assert.Equal(t, "3.14", dbScanToString(3.14))
}

// ==================== dbScanToInt ====================

func TestDbScanToInt(t *testing.T) {
	assert.Equal(t, 0, dbScanToInt(nil))
	assert.Equal(t, 42, dbScanToInt(int64(42)))
	assert.Equal(t, 99, dbScanToInt([]byte("99")))
	assert.Equal(t, 7, dbScanToInt("7"))
	assert.Equal(t, 0, dbScanToInt("abc"))
	assert.Equal(t, 0, dbScanToInt(3.14))
}

// ==================== syncTuneFrom ====================

func TestSyncTuneFrom(t *testing.T) {
	t.Run("nil config returns nil", func(t *testing.T) {
		assert.Nil(t, syncTuneFrom(nil))
	})

	t.Run("non-nil config returns sync config pointer", func(t *testing.T) {
		cfg := &config.Config{
			Sync: config.SyncTuneConfig{
				IntraTableLegacyCap: 8,
				IntraTableHardMax:   32,
			},
		}
		result := syncTuneFrom(cfg)
		require.NotNil(t, result)
		assert.Equal(t, 8, result.IntraTableLegacyCap)
		assert.Equal(t, 32, result.IntraTableHardMax)
	})
}

// ==================== intraTableConcurrencyCaps ====================

func TestIntraTableConcurrencyCaps(t *testing.T) {
	t.Run("nil config returns defaults", func(t *testing.T) {
		ts := &TaskService{config: nil}
		legacy, hard := ts.intraTableConcurrencyCaps()
		assert.Equal(t, 16, legacy)
		assert.Equal(t, 64, hard)
	})

	t.Run("zero sync config returns defaults", func(t *testing.T) {
		ts := &TaskService{config: &config.Config{}}
		legacy, hard := ts.intraTableConcurrencyCaps()
		assert.Equal(t, 16, legacy)
		assert.Equal(t, 64, hard)
	})

	t.Run("custom values override defaults", func(t *testing.T) {
		ts := &TaskService{config: &config.Config{
			Sync: config.SyncTuneConfig{
				IntraTableLegacyCap: 8,
				IntraTableHardMax:   32,
			},
		}}
		legacy, hard := ts.intraTableConcurrencyCaps()
		assert.Equal(t, 8, legacy)
		assert.Equal(t, 32, hard)
	})

	t.Run("only legacy cap set", func(t *testing.T) {
		ts := &TaskService{config: &config.Config{
			Sync: config.SyncTuneConfig{
				IntraTableLegacyCap: 4,
			},
		}}
		legacy, hard := ts.intraTableConcurrencyCaps()
		assert.Equal(t, 4, legacy)
		assert.Equal(t, 64, hard)
	})
}

// ==================== failTaskUnlessCancelled ====================

func TestFailTaskUnlessCancelled(t *testing.T) {
	t.Run("marks task failed when context active and task running", func(t *testing.T) {
		ts := newTestTaskService(t.TempDir())
		task, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "ft1", Name: "T1"})
		task.Start() // must be running, otherwise isTaskStopped returns true

		ctx := context.Background()
		ts.failTaskUnlessCancelled(ctx, "ft1", "some error")

		task, _ = ts.GetTask("ft1")
		assert.Equal(t, taskEntity.TaskStatusFailed, task.Context.Status)
		assert.Equal(t, "some error", task.Context.ErrorStack)
	})

	t.Run("ignores error when context cancelled", func(t *testing.T) {
		ts := newTestTaskService(t.TempDir())
		task, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "ft2", Name: "T2"})
		task.Start()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		ts.failTaskUnlessCancelled(ctx, "ft2", "should be ignored")

		task, _ = ts.GetTask("ft2")
		assert.Equal(t, taskEntity.TaskStatusRunning, task.Context.Status)
		assert.Empty(t, task.Context.ErrorStack)
	})

	t.Run("ignores error when task already stopped", func(t *testing.T) {
		ts := newTestTaskService(t.TempDir())
		ts.CreateTask(taskEntity.TaskConfig{ID: "ft3", Name: "T3"})
		// task is in Pending status (not running), so isTaskStopped returns true

		ctx := context.Background()
		ts.failTaskUnlessCancelled(ctx, "ft3", "should be ignored")

		task, _ := ts.GetTask("ft3")
		assert.Equal(t, taskEntity.TaskStatusPending, task.Context.Status)
	})
}

// ==================== taskRuntime.Close ====================

func TestTaskRuntimeClose(t *testing.T) {
	t.Run("nil runtime does not panic", func(t *testing.T) {
		var r *taskRuntime
		assert.NotPanics(t, func() { r.Close() })
	})

	t.Run("calls cancel and closes DBs", func(t *testing.T) {
		cancelCalled := false

		sourceDB, _, err := sqlmock.New()
		require.NoError(t, err)

		targetDB, _, err := sqlmock.New()
		require.NoError(t, err)

		r := &taskRuntime{
			sourceDB: sourceDB,
			targetDB: targetDB,
			cancel:   func() { cancelCalled = true },
		}

		r.Close()
		assert.True(t, cancelCalled)
	})

	t.Run("does not double-close when source equals target", func(t *testing.T) {
		db, _, err := sqlmock.New()
		require.NoError(t, err)

		r := &taskRuntime{
			sourceDB: db,
			targetDB: db,
		}
		assert.NotPanics(t, func() { r.Close() })
	})

	t.Run("nil cancel does not panic", func(t *testing.T) {
		r := &taskRuntime{}
		assert.NotPanics(t, func() { r.Close() })
	})
}

// ==================== closeResource ====================

func TestCloseResource(t *testing.T) {
	t.Run("nil closer does not panic", func(t *testing.T) {
		assert.NotPanics(t, func() { closeResource(nil, "test") })
	})

	t.Run("successful close", func(t *testing.T) {
		called := false
		closer := func() error {
			called = true
			return nil
		}
		closeResource(closer, "test")
		assert.True(t, called)
	})

	t.Run("error close does not panic", func(t *testing.T) {
		closer := func() error {
			return fmt.Errorf("close error")
		}
		assert.NotPanics(t, func() { closeResource(closer, "test") })
	})
}

// ==================== withDDL ====================

func TestWithDDL(t *testing.T) {
	t.Run("nil runtime executes fn directly", func(t *testing.T) {
		ts := &TaskService{}
		called := false
		err := ts.withDDL(nil, func() error {
			called = true
			return nil
		})
		assert.NoError(t, err)
		assert.True(t, called)
	})

	t.Run("nil readOnlyManager executes fn directly", func(t *testing.T) {
		ts := &TaskService{}
		rt := &taskRuntime{readOnlyManager: nil}
		called := false
		err := ts.withDDL(rt, func() error {
			called = true
			return nil
		})
		assert.NoError(t, err)
		assert.True(t, called)
	})

	t.Run("fn error propagated", func(t *testing.T) {
		ts := &TaskService{}
		err := ts.withDDL(nil, func() error {
			return fmt.Errorf("ddl error")
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ddl error")
	})
}

func TestDropTargetTableIfNeeded(t *testing.T) {
	ts := &TaskService{}
	err := ts.dropTargetTableIfNeeded(context.Background(), nil, "test", "users", true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "target connection is nil")

	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDB.Close()

	conn, err := mockDB.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	t.Run("disabled returns nil", func(t *testing.T) {
		err := ts.dropTargetTableIfNeeded(context.Background(), conn, "test", "users", false)
		assert.NoError(t, err)
	})

	t.Run("drops target table when enabled", func(t *testing.T) {
		mock.ExpectExec("DROP TABLE IF EXISTS `test`.`users`").WillReturnResult(sqlmock.NewResult(0, 0))
		err := ts.dropTargetTableIfNeeded(context.Background(), conn, "test", "users", true)
		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("uses custom target table name when dropping", func(t *testing.T) {
		customMockDB, customMock, err := sqlmock.New()
		require.NoError(t, err)
		defer customMockDB.Close()

		customConn, err := customMockDB.Conn(context.Background())
		require.NoError(t, err)
		defer customConn.Close()

		customMock.ExpectExec("DROP TABLE IF EXISTS `prod_backup`.`users_archive`").WillReturnResult(sqlmock.NewResult(0, 0))
		err = ts.dropTargetTableIfNeeded(context.Background(), customConn, "prod_backup", "users_archive", true)
		require.NoError(t, err)
		require.NoError(t, customMock.ExpectationsWereMet())
	})
}

func TestEnsureTargetTable_DropBeforeDDLRecreatesExistingTable(t *testing.T) {
	sourceDB, sourceMock, err := sqlmock.New()
	require.NoError(t, err)
	defer sourceDB.Close()

	targetDB, targetMock, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	ts := &TaskService{tasks: make(map[string]*taskEntity.SyncTask)}
	runtime := &taskRuntime{sourceDB: sourceDB, targetDB: targetDB}

	targetMock.ExpectQuery("SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?").WithArgs("target_db").WillReturnError(sql.ErrNoRows)
	targetMock.ExpectExec("CREATE DATABASE IF NOT EXISTS `target_db` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci").WillReturnResult(sqlmock.NewResult(0, 0))
	targetMock.ExpectQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = \\? AND table_name = \\?").
		WithArgs("target_db", "users").
		WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("users"))
	targetMock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	targetMock.ExpectExec("DROP TABLE IF EXISTS `target_db`.`users`").WillReturnResult(sqlmock.NewResult(0, 0))
	targetMock.ExpectExec("CREATE TABLE `target_db`.`users` LIKE `source_db`.`users`").WillReturnResult(sqlmock.NewResult(0, 0))
	targetMock.ExpectExec("SET SESSION FOREIGN_KEY_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))

	t.Run("existing table is recreated when enabled", func(t *testing.T) {
		targetConn, err := targetDB.Conn(context.Background())
		require.NoError(t, err)
		defer targetConn.Close()
		sourceConn, err := sourceDB.Conn(context.Background())
		require.NoError(t, err)
		defer sourceConn.Close()

		runtime.sourceDB = sourceDB
		runtime.targetDB = targetDB

		// ensureTargetTable ??? CREATE TABLE LIKE ???????? DROP????????????
		_, err = ts.ensureTargetTable(context.Background(), runtime, "source_db", "target_db", "users", "users", false, true, pkUsersIdentity(), taskEntity.SyncModeFull)
		require.NoError(t, err)
	})

	require.NoError(t, targetMock.ExpectationsWereMet())
	require.NoError(t, sourceMock.ExpectationsWereMet())
}

// ==================== min ====================

func TestMin(t *testing.T) {
	assert.Equal(t, 3, min(3, 5))
	assert.Equal(t, 3, min(5, 3))
	assert.Equal(t, 4, min(4, 4))
	assert.Equal(t, -1, min(-1, 0))
}

// ==================== captureFullSyncStartPosition??????? ====================

// captureFullSyncStartPosition ????"?????"???
// ??? FTWRL?SHOW MASTER STATUS ??????? UNLOCK?
func TestCaptureFullSyncStartPosition_UnlockedMasterStatus(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// 新语义：无 FTWRL / UNLOCK，仅 SHOW MASTER STATUS。
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).AddRow("mysql-bin.000123", 456, "", "", ""))

	ts := newTestTaskService(t.TempDir())
	pos, err := ts.captureFullSyncStartPosition(context.Background(), &taskRuntime{sourceDB: db})

	require.NoError(t, err)
	assert.Equal(t, "mysql-bin.000123", pos.Name)
	assert.Equal(t, uint32(456), pos.Pos)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCaptureFullSyncStartPosition_NilRuntime(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	_, err := ts.captureFullSyncStartPosition(context.Background(), nil)
	assert.Error(t, err)
}

// ==================== resolveSourceSchema edge cases ====================

func TestResolveSourceSchema_Empty(t *testing.T) {
	t.Run("nil task returns empty from nil config", func(t *testing.T) {
		ts := &TaskService{}
		assert.Equal(t, "", ts.resolveSourceSchema(nil))
	})

	t.Run("empty everything returns empty", func(t *testing.T) {
		ts := &TaskService{}
		task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "empty"})
		assert.Equal(t, "", ts.resolveSourceSchema(task))
	})
}

// ==================== FileTaskStorage.Delete edge cases ====================

func TestFileTaskStorage_Delete_NonExistent(t *testing.T) {
	dataDir := t.TempDir()
	storage := NewFileTaskStorage(dataDir)

	err := storage.Delete("non_existent_task")
	assert.NoError(t, err)
}

func TestFileTaskStorage_Delete_Existing(t *testing.T) {
	dataDir := t.TempDir()
	storage := NewFileTaskStorage(dataDir)

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "delete_me", Name: "Delete Me"})
	err := storage.Save(task)
	require.NoError(t, err)

	err = storage.Delete("delete_me")
	assert.NoError(t, err)

	tasks, err := storage.LoadAll()
	require.NoError(t, err)
	assert.Empty(t, tasks)
}

// ==================== FileTaskStorage LoadAll empty dir ====================

func TestFileTaskStorage_LoadAll_EmptyDir(t *testing.T) {
	dataDir := t.TempDir()
	storage := NewFileTaskStorage(dataDir)

	tasks, err := storage.LoadAll()
	assert.NoError(t, err)
	assert.Empty(t, tasks)
}

// ==================== FileTaskStorage Save and LoadAll round-trip ====================

func TestFileTaskStorage_SaveAndLoad_RoundTrip(t *testing.T) {
	dataDir := t.TempDir()
	storage := NewFileTaskStorage(dataDir)

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:           "round_trip",
		Name:         "Round Trip Task",
		SourceSchema: "src",
		TargetSchema: "tgt",
		Tables:       []string{"t1", "t2"},
	})

	err := storage.Save(task)
	require.NoError(t, err)

	tasks, err := storage.LoadAll()
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, "round_trip", tasks[0].Config.ID)
	assert.Equal(t, "Round Trip Task", tasks[0].Config.Name)
	assert.Equal(t, "src", tasks[0].Config.SourceSchema)
	assert.Equal(t, "tgt", tasks[0].Config.TargetSchema)
	assert.Equal(t, []string{"t1", "t2"}, tasks[0].Config.Tables)
}

func TestCancelScheduleRestoresPreviousStatus(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "schedule_restore", Name: "Schedule Restore"})
	require.NoError(t, err)

	task.Pause()
	require.NoError(t, ts.ScheduleTask(task.Config.ID, time.Now().Add(time.Minute)))
	assert.Equal(t, taskEntity.TaskStatusScheduled, task.Context.Status)
	require.NotNil(t, task.Context.ScheduledFromStatus)
	assert.Equal(t, taskEntity.TaskStatusPaused, *task.Context.ScheduledFromStatus)

	require.NoError(t, ts.CancelSchedule(task.Config.ID))
	assert.Equal(t, taskEntity.TaskStatusPaused, task.Context.Status)
	assert.Nil(t, task.Context.ScheduledAt)
	assert.Nil(t, task.Context.ScheduledFromStatus)
	assert.NoError(t, ts.Close())
}

func TestCancelScheduleRestoresFailedStatus(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "schedule_restore_failed", Name: "Schedule Restore Failed"})
	require.NoError(t, err)

	task.Fail(assert.AnError)
	require.NoError(t, ts.ScheduleTask(task.Config.ID, time.Now().Add(time.Minute)))
	require.NoError(t, ts.CancelSchedule(task.Config.ID))

	assert.Equal(t, taskEntity.TaskStatusFailed, task.Context.Status)
	assert.Nil(t, task.Context.ScheduledAt)
	assert.Nil(t, task.Context.ScheduledFromStatus)
	assert.NoError(t, ts.Close())
}

func TestScheduleTaskWithRepeatConfiguresRepeatFields(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "schedule_repeat", Name: "Schedule Repeat"})
	require.NoError(t, err)

	when := time.Now().Add(time.Minute)
	require.NoError(t, ts.ScheduleTaskWithRepeat(task.Config.ID, when, 3, 30))

	assert.Equal(t, taskEntity.TaskStatusScheduled, task.Context.Status)
	require.NotNil(t, task.Context.ScheduledAt)
	assert.WithinDuration(t, when, *task.Context.ScheduledAt, time.Second)
	assert.Equal(t, 3, task.Context.RepeatCount)
	assert.Equal(t, 3, task.Context.RepeatRemaining)
	assert.Equal(t, 30, task.Context.RepeatIntervalSec)
	assert.NoError(t, ts.Close())
}

func TestScheduleTaskWithRepeatRejectsInvalidTask(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	err := ts.ScheduleTaskWithRepeat("missing", time.Now().Add(time.Minute), 2, 10)
	assert.Error(t, err)
}

func TestCancelScheduleClearsRepeatFields(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "schedule_cancel_repeat", Name: "Schedule Cancel Repeat"})
	require.NoError(t, err)

	require.NoError(t, ts.ScheduleTaskWithRepeat(task.Config.ID, time.Now().Add(time.Minute), 3, 30))
	require.NoError(t, ts.CancelSchedule(task.Config.ID))

	assert.Equal(t, taskEntity.TaskStatusPending, task.Context.Status)
	assert.Nil(t, task.Context.ScheduledAt)
	assert.Nil(t, task.Context.ScheduledFromStatus)
	assert.Zero(t, task.Context.RepeatCount)
	assert.Zero(t, task.Context.RepeatRemaining)
	assert.Zero(t, task.Context.RepeatIntervalSec)
	assert.Empty(t, task.Context.ScheduleMode)
	assert.Empty(t, task.Context.CronExpression)
	assert.Empty(t, task.Context.CronTimezone)
	assert.NoError(t, ts.Close())
}

func TestStartTaskImmediatelyClearsStaleRepeatFields(t *testing.T) {
	ts := newScheduledTestTaskService(t.TempDir())
	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "start_clear_repeat", Name: "Start Clear Repeat"})
	require.NoError(t, err)
	task.ConfigureRepeat(3, 30)

	require.NoError(t, ts.StartTask(context.Background(), task.Config.ID))

	assert.Equal(t, taskEntity.TaskStatusRunning, task.Context.Status)
	assert.Zero(t, task.Context.RepeatCount)
	assert.Zero(t, task.Context.RepeatRemaining)
	assert.Zero(t, task.Context.RepeatIntervalSec)
	assert.NoError(t, ts.Close())
}

func TestCompleteTaskWithRepeatReschedulesUntilExhausted(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "repeat_complete", Name: "Repeat Complete"})
	require.NoError(t, err)

	require.NoError(t, ts.ScheduleTaskWithRepeat(task.Config.ID, time.Now().Add(time.Minute), 2, 1))
	task.Start()
	ts.completeTask(task.Config.ID)

	assert.Equal(t, taskEntity.TaskStatusScheduled, task.Context.Status)
	require.NotNil(t, task.Context.ScheduledAt)
	assert.Equal(t, 2, task.Context.RepeatCount)
	assert.Equal(t, 1, task.Context.RepeatRemaining)
	assert.Equal(t, 1, task.Context.RepeatIntervalSec)

	task.Start()
	ts.completeTask(task.Config.ID)

	assert.Equal(t, taskEntity.TaskStatusCompleted, task.Context.Status)
	assert.Nil(t, task.Context.ScheduledAt)
	assert.Zero(t, task.Context.RepeatCount)
	assert.Zero(t, task.Context.RepeatRemaining)
	assert.Zero(t, task.Context.RepeatIntervalSec)
	assert.NoError(t, ts.Close())
}

// TestScheduleCronTaskPreservesCronConfig ?? ScheduleCronTask ??? cron ?????
// ????????? ResetRepeat ??? ConfigureCronSchedule ???? cron ???
// ?? nextCronRun ? CronExpression ??????
func TestScheduleCronTaskPreservesCronConfig(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "cron_set", Name: "Cron Set"})
	require.NoError(t, err)

	require.NoError(t, ts.ScheduleCronTask(task.Config.ID, time.Now().Add(time.Hour), "0 9 * * 1-5", "Asia/Shanghai"))

	assert.Equal(t, taskEntity.TaskStatusScheduled, task.Context.Status)
	assert.Equal(t, "cron", task.Context.ScheduleMode)
	assert.Equal(t, "0 9 * * 1-5", task.Context.CronExpression)
	assert.Equal(t, "Asia/Shanghai", task.Context.CronTimezone)
	require.NotNil(t, task.Context.ScheduledAt)
	assert.NoError(t, ts.Close())
}

// TestCompleteTaskCronReschedules ?? cron ???????????????????
// ? cron ????? ClearScheduleConfig ???
func TestCompleteTaskCronReschedules(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "cron_reschedule", Name: "Cron Reschedule"})
	require.NoError(t, err)

	require.NoError(t, ts.ScheduleCronTask(task.Config.ID, time.Now().Add(time.Hour), "0 9 * * 1-5", "Asia/Shanghai"))
	task.Start()
	ts.completeTask(task.Config.ID)

	assert.Equal(t, taskEntity.TaskStatusScheduled, task.Context.Status)
	assert.Equal(t, "cron", task.Context.ScheduleMode)
	assert.Equal(t, "0 9 * * 1-5", task.Context.CronExpression)
	require.NotNil(t, task.Context.ScheduledAt)
	assert.NoError(t, ts.Close())
}

// TestCompleteTaskClearsStaleScheduleConfig ??? cron / repeat ??????????
// ???????? ClearScheduleConfig ???????? COMPLETED ???????????
func TestCompleteTaskClearsStaleScheduleConfig(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "complete_clear", Name: "Complete Clear"})
	require.NoError(t, err)

	// ??????????repeat ???????? 0
	task.Context.ScheduleMode = "repeat"
	task.Context.RepeatRemaining = 0
	task.Context.CronExpression = "0 9 * * 1-5"
	task.Start()
	ts.completeTask(task.Config.ID)

	assert.Equal(t, taskEntity.TaskStatusCompleted, task.Context.Status)
	assert.Empty(t, task.Context.ScheduleMode)
	assert.Empty(t, task.Context.CronExpression)
	assert.Empty(t, task.Context.CronTimezone)
	assert.Nil(t, task.Context.ScheduledAt)
	assert.Nil(t, task.Context.ScheduledFromStatus)
	assert.NoError(t, ts.Close())
}

// TestStartTaskImmediatelyClearsStaleCronFields ?????????????? cron ?????
// ?? RUNNING ????????????????
func TestStartTaskImmediatelyClearsStaleCronFields(t *testing.T) {
	ts := newScheduledTestTaskService(t.TempDir())
	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "start_clear_cron", Name: "Start Clear Cron"})
	require.NoError(t, err)
	task.ConfigureCronSchedule("0 9 * * 1-5", "Asia/Shanghai")
	require.Equal(t, "cron", task.Context.ScheduleMode)

	require.NoError(t, ts.StartTask(context.Background(), task.Config.ID))

	assert.Equal(t, taskEntity.TaskStatusRunning, task.Context.Status)
	assert.Empty(t, task.Context.ScheduleMode)
	assert.Empty(t, task.Context.CronExpression)
	assert.Empty(t, task.Context.CronTimezone)
	assert.Nil(t, task.Context.ScheduledAt)
	assert.Nil(t, task.Context.ScheduledFromStatus)
	assert.NoError(t, ts.Close())
}

// TestCancelScheduleAfterCronRestoresStatus ??? cron ????????????????
// ??? ScheduledFromStatus ??????? PENDING?
func TestCancelScheduleAfterCronRestoresStatus(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "cron_cancel_restore", Name: "Cron Cancel Restore"})
	require.NoError(t, err)
	task.Fail(assert.AnError)
	require.Equal(t, taskEntity.TaskStatusFailed, task.Context.Status)

	require.NoError(t, ts.ScheduleCronTask(task.Config.ID, time.Now().Add(time.Hour), "0 9 * * 1-5", "Asia/Shanghai"))
	require.NoError(t, ts.CancelSchedule(task.Config.ID))

	assert.Equal(t, taskEntity.TaskStatusFailed, task.Context.Status)
	assert.Empty(t, task.Context.ScheduleMode)
	assert.Empty(t, task.Context.CronExpression)
	assert.Nil(t, task.Context.ScheduledAt)
	assert.Nil(t, task.Context.ScheduledFromStatus)
	assert.NoError(t, ts.Close())
}

func TestScheduleOperationsReturnStorageErrors(t *testing.T) {
	storageErr := fmt.Errorf("storage unavailable")
	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"schedule_save_error": taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "schedule_save_error", Name: "Schedule Save Error"}),
		},
		runtimes: make(map[string]*taskRuntime),
		storage:  &failingTaskStorage{err: storageErr},
	}

	err := ts.ScheduleTask("schedule_save_error", time.Now().Add(time.Minute))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to save scheduled task state")

	ts.tasks["schedule_save_error"] = taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "schedule_save_error", Name: "Schedule Save Error"})
	err = ts.ScheduleTaskWithRepeat("schedule_save_error", time.Now().Add(time.Minute), 2, 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to save")

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "cancel_save_error", Name: "Cancel Save Error"})
	task.Schedule(time.Now().Add(time.Minute))
	ts.tasks["cancel_save_error"] = task
	err = ts.CancelSchedule("cancel_save_error")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to save cancelled schedule state")
}

// ==================== ??????entity ???? ====================

func TestSyncPhase_HelpersOnFreshTask(t *testing.T) {
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "phase_fresh"})
	assert.Equal(t, taskEntity.SyncPhaseInit, task.Context.SyncPhase)
	assert.False(t, task.HasFullSyncEverCompleted(), "fresh task must not be considered completed")
	assert.False(t, task.FullSyncIncomplete(), "fresh task is neither completed nor incomplete")
}

func TestSyncPhase_MarkFullSyncStartedThenCompleted(t *testing.T) {
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "phase_full"})

	task.MarkFullSyncStarted("mysql-bin.000001:120")
	assert.Equal(t, taskEntity.SyncPhaseFullStarted, task.Context.SyncPhase)
	assert.Equal(t, "mysql-bin.000001:120", task.Context.FullSyncStartPosition)
	require.NotNil(t, task.Context.FullSyncStartedAt)
	assert.Nil(t, task.Context.FullSyncCompletedAt)
	assert.True(t, task.FullSyncIncomplete(), "FULL_STARTED is incomplete")
	assert.False(t, task.HasFullSyncEverCompleted(), "FULL_STARTED is not completed")

	task.MarkFullSyncCompleted()
	assert.Equal(t, taskEntity.SyncPhaseFullCompleted, task.Context.SyncPhase)
	require.NotNil(t, task.Context.FullSyncCompletedAt)
	assert.True(t, task.HasFullSyncEverCompleted(), "after FULL_COMPLETED, must be ever-completed")
	assert.False(t, task.FullSyncIncomplete(), "FULL_COMPLETED is not incomplete anymore")
}

func TestSyncPhase_MarkFullSyncFailedKeepsIncompleteFlag(t *testing.T) {
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "phase_fail"})
	task.MarkFullSyncStarted("mysql-bin.000002:999")
	task.MarkFullSyncFailed("simulated DB error")

	assert.Equal(t, taskEntity.SyncPhaseFullFailed, task.Context.SyncPhase)
	assert.Equal(t, "simulated DB error", task.Context.FullSyncFailedReason)
	assert.True(t, task.FullSyncIncomplete(), "FULL_FAILED still counts as incomplete (requires target rebuild before a new full sync)")
	assert.False(t, task.HasFullSyncEverCompleted(), "FULL_FAILED must not pretend to be completed")
}

func TestSyncPhase_MarkIncrementalStartedPreservesCompletion(t *testing.T) {
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "phase_incr"})
	task.MarkFullSyncStarted("mysql-bin.000003:1")
	task.MarkFullSyncCompleted()
	task.MarkIncrementalStarted()

	assert.Equal(t, taskEntity.SyncPhaseIncrementalStarted, task.Context.SyncPhase)
	assert.True(t, task.HasFullSyncEverCompleted(), "INCREMENTAL_STARTED also satisfies HasFullSyncEverCompleted")
	assert.False(t, task.FullSyncIncomplete())
}

func TestSyncPhase_UpdateIncrementalPosition(t *testing.T) {
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "phase_pos"})
	task.MarkFullSyncCompleted()
	task.MarkIncrementalStarted()

	task.UpdateIncrementalPosition("mysql-bin.000004:777")
	assert.Equal(t, "mysql-bin.000004:777", task.Context.LastIncrementalPosition)
	assert.Equal(t, taskEntity.SyncPhaseIncrementalStarted, task.Context.SyncPhase, "updating position should not change phase")
}

func TestSyncPhase_ResetWipesAllPhaseFields(t *testing.T) {
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "phase_reset"})
	task.MarkFullSyncStarted("mysql-bin.000005:1")
	task.MarkFullSyncCompleted()
	task.MarkIncrementalStarted()
	task.UpdateIncrementalPosition("mysql-bin.000005:888")
	task.SetTableBinlogHWM("db.nopk", "mysql-bin.000005:100")

	task.ResetSyncPhase()
	assert.Equal(t, taskEntity.SyncPhaseInit, task.Context.SyncPhase)
	assert.Empty(t, task.Context.FullSyncStartPosition)
	assert.Empty(t, task.Context.LastIncrementalPosition)
	assert.Empty(t, task.Context.FullSyncFailedReason)
	assert.Nil(t, task.Context.FullSyncStartedAt)
	assert.Nil(t, task.Context.FullSyncCompletedAt)
	assert.Nil(t, task.Context.TableBinlogHWMs)
	assert.False(t, task.HasFullSyncEverCompleted())
	assert.False(t, task.FullSyncIncomplete())
}

// ==================== ??????service ??? helper ====================

func TestUpdateSyncPhase_PersistsAndIsNoopForUnknownTask(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "persist_phase", Name: "Persist Phase"})
	ts.tasks[task.Config.ID] = task

	ts.updateSyncPhase("persist_phase", func(tk *taskEntity.SyncTask) {
		tk.MarkFullSyncStarted("mysql-bin.000010:5")
	})

	assert.Equal(t, taskEntity.SyncPhaseFullStarted, task.Context.SyncPhase)
	assert.Equal(t, "mysql-bin.000010:5", task.Context.FullSyncStartPosition)

	assert.NotPanics(t, func() {
		ts.updateSyncPhase("does_not_exist", func(_ *taskEntity.SyncTask) {
			t.Fatal("mutator should not run for missing task")
		})
	})
}

// ==================== executeSync ???? + ?? bug ====================

// TestExecuteSync_IncrementalRequiresFullSync ???? 5?
// ? INCREMENTAL ???????????????????? FAILED ????? binlog?
func TestExecuteSync_IncrementalRequiresFullSync(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:   "incr_without_full",
		Name: "Incremental Without Full",
		Mode: taskEntity.SyncModeIncremental,
	})
	task.Start() // ?? RUNNING??? failTaskUnlessCancelled ???
	ts.tasks[task.Config.ID] = task

	ts.executeSync(context.Background(), task.Config.ID, &taskRuntime{})

	assert.Equal(t, taskEntity.TaskStatusFailed, task.Context.Status,
		"INCREMENTAL without prior FULL must fail-fast, not silently start a stale subscriber")
	assert.Contains(t, task.Context.ErrorStack, "requires a previously completed full sync")
}

// TestExecuteSync_IncrementalAllowedAfterFullCompleted ???? 5 ??????
// ????? FULL_COMPLETED?INCREMENTAL ?????? executeIncrementalSync?
// ??? nil runtime ?? executeIncrementalSync ??????????????????
func TestExecuteSync_IncrementalAllowedAfterFullCompleted(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:   "incr_after_full",
		Name: "Incremental After Full",
		Mode: taskEntity.SyncModeIncremental,
	})
	task.Start()
	task.MarkFullSyncStarted("mysql-bin.000020:1")
	task.MarkFullSyncCompleted()
	ts.tasks[task.Config.ID] = task

	ts.executeSync(context.Background(), task.Config.ID, nil)

	// ??????? executeIncrementalSync??? runtime ? nil ????? FAILED?
	// ????????"task runtime is nil"???????????
	assert.Equal(t, taskEntity.TaskStatusFailed, task.Context.Status)
	assert.Contains(t, task.Context.ErrorStack, "runtime")
	assert.NotContains(t, task.Context.ErrorStack, "requires a previously completed full sync")
}

// TestExecuteSync_FullPauseDoesNotCompleteTask ??????? COMPLETED ? P0 bug?
// ????????? ? executeFullSync ???"??????"?? errFullSyncStoppedByUser
// ? executeSync ?? sentinel ?????? completeTask ??????? FAILED?
// ????????? FULL_COMPLETED?
//
// ???? executeFullSync ??????????? mock SQL??? runtime ???? nil
// ???????????? isTaskStopped ???
func TestExecuteSync_FullPauseDoesNotCompleteTask(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ts := newTestTaskService(t.TempDir())

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:           "full_paused",
		Name:         "Full Paused",
		Mode:         taskEntity.SyncModeFull,
		SourceSchema: "dummy",
	})
	task.Start()
	ts.tasks[task.Config.ID] = task

	// ??"????????????"?? TaskService ???????????
	require.NoError(t, ts.PauseTask(task.Config.ID))

	ts.executeSync(context.Background(), task.Config.ID, &taskRuntime{
		sourceDB: db,
		targetDB: db,
		analyzer: &mockAnalyzer{},
	})

	assert.Equal(t, taskEntity.TaskStatusPaused, task.Context.Status,
		"paused full sync must NOT be marked COMPLETED (regression of pause-as-success bug)")
	assert.NotEqual(t, taskEntity.SyncPhaseFullCompleted, task.Context.SyncPhase,
		"paused full sync must NOT advance phase to FULL_COMPLETED")
	assert.NotEqual(t, taskEntity.SyncPhaseFullFailed, task.Context.SyncPhase,
		"paused full sync must NOT be flipped to FULL_FAILED either")
}

// TestExecuteSync_DatabaseRebuildPauseKeepsPhase ????????????
// executeFullSync ??? errFullSyncStoppedByUser ????????? MarkFullSyncFailed?
// ?? SyncPhase ?? FULL_STARTED ?????? FULL_FAILED?
func TestExecuteSync_DatabaseRebuildPauseKeepsPhase(t *testing.T) {
	sourceDB, sourceMock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer sourceDB.Close()
	targetDB, targetMock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer targetDB.Close()

	sourceMock.MatchExpectationsInOrder(false)
	targetMock.MatchExpectationsInOrder(false)

	// FULL ?????? binlog ????? mock FTWRL/SHOW MASTER STATUS/UNLOCK?

	// ????????????? DROP ??????????????????????
	// ???????????? isTaskStopped ??????? DROP/CREATE?
	targetMock.ExpectExec(regexp.QuoteMeta("DROP DATABASE IF EXISTS `tgt_a`")).
		WillDelayFor(500 * time.Millisecond).
		WillReturnResult(sqlmock.NewResult(0, 0))
	targetMock.ExpectExec(regexp.QuoteMeta("CREATE DATABASE IF NOT EXISTS `tgt_a` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	ts := newTestTaskService(t.TempDir())
	ts.sourceDB = sourceDB
	ts.targetDB = targetDB
	ts.analyzer = &fixedIdentityAnalyzer{identity: pkUsersIdentity()}
	ts.checkpointManager = checkpoint.NewMemoryCheckpointManager()

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:                       "db_rebuild_pause",
		Name:                     "DB Rebuild Pause",
		Mode:                     taskEntity.SyncModeFull,
		SyncLevel:                taskEntity.SyncLevelDatabase,
		SourceDatabases:          []string{"src_a", "src_b"},
		TargetDatabases:          []string{"tgt_a", "tgt_b"},
		EnableDropTableBeforeDDL: true,
	})
	task.Start()
	ts.tasks[task.Config.ID] = task

	runtime := &taskRuntime{
		sourceDB: sourceDB,
		targetDB: targetDB,
		analyzer: ts.analyzer,
	}
	ts.runtimes[task.Config.ID] = runtime

	// ????????????? 100ms ?? executeFullSync ?????????? rebuild?
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = ts.PauseTask(task.Config.ID)
	}()

	ts.executeSync(context.Background(), task.Config.ID, runtime)

	assert.Equal(t, taskEntity.TaskStatusPaused, task.Context.Status,
		"paused database rebuild must NOT be marked COMPLETED")
	assert.Equal(t, taskEntity.SyncPhaseFullStarted, task.Context.SyncPhase,
		"paused database rebuild must keep FULL_STARTED phase, not flip to FULL_FAILED")

	require.NoError(t, sourceMock.ExpectationsWereMet())
	require.NoError(t, targetMock.ExpectationsWereMet())
}

// TestErrFullSyncStoppedByUser_IsSentinel ?? sentinel ??????
// errors.Is ??????????? true????? switch ??????????
func TestErrFullSyncStoppedByUser_IsSentinel(t *testing.T) {
	wrapped := fmt.Errorf("syncDatabasePair: %w", errFullSyncStoppedByUser)
	assert.True(t, errors.Is(wrapped, errFullSyncStoppedByUser))

	other := fmt.Errorf("something else")
	assert.False(t, errors.Is(other, errFullSyncStoppedByUser))
}

func TestErrFullSyncPreflight_IsSentinel(t *testing.T) {
	wrapped := fmt.Errorf("%w: need allow_nopk_all", errFullSyncPreflight)
	assert.True(t, errors.Is(wrapped, errFullSyncPreflight))
	assert.False(t, errors.Is(fmt.Errorf("other"), errFullSyncPreflight))
}

type nopkIdentityAnalyzer struct{}

func (a *nopkIdentityAnalyzer) AnalyzeTable(_, tableName string) (*entity.TableIdentity, error) {
	return &entity.TableIdentity{
		TableName:    tableName,
		Strategy:     entity.FullColumnsStrategy,
		IdentifyCols: []string{"a", "b"},
	}, nil
}
func (a *nopkIdentityAnalyzer) GetAllTables(string) ([]entity.TableInfo, error) {
	return nil, nil
}
func (a *nopkIdentityAnalyzer) GetAllDatabases() ([]string, error) { return nil, nil }

type failingAnalyzeAnalyzer struct{ err error }

func (a *failingAnalyzeAnalyzer) AnalyzeTable(_, _ string) (*entity.TableIdentity, error) {
	return nil, a.err
}
func (a *failingAnalyzeAnalyzer) GetAllTables(string) ([]entity.TableInfo, error) {
	return nil, nil
}
func (a *failingAnalyzeAnalyzer) GetAllDatabases() ([]string, error) { return nil, nil }

func TestExecuteSync_ALLMissingNopkAckDoesNotMarkFullFailed(t *testing.T) {
	sourceDB, sourceMock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer sourceDB.Close()
	targetDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()
	sourceMock.MatchExpectationsInOrder(false)
	// 估算行数允许失败；确认门禁只依赖 AnalyzeTable。
	sourceMock.ExpectQuery("SELECT TABLE_ROWS").WillReturnError(fmt.Errorf("skip estimate"))

	ts := newTestTaskService(t.TempDir())
	ts.checkpointManager = checkpoint.NewMemoryCheckpointManager()
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:           "all_nopk_no_ack",
		Name:         "ALL No Ack",
		Mode:         taskEntity.SyncModeAll,
		SourceSchema: "src",
		Tables:       []string{"audit"},
	})
	task.Start()
	ts.tasks[task.Config.ID] = task
	phaseBefore := task.Context.SyncPhase

	runtime := &taskRuntime{
		sourceDB: sourceDB,
		targetDB: targetDB,
		analyzer: &nopkIdentityAnalyzer{},
	}
	ts.runtimes[task.Config.ID] = runtime
	ts.executeSync(context.Background(), task.Config.ID, runtime)

	assert.Equal(t, taskEntity.TaskStatusFailed, task.Context.Status)
	assert.Equal(t, phaseBefore, task.Context.SyncPhase, "missing ack must not pollute SyncPhase to FULL_FAILED")
	assert.NotEqual(t, taskEntity.SyncPhaseFullFailed, task.Context.SyncPhase)
	assert.Contains(t, task.Context.ErrorStack, "allow_nopk_all")
}

func TestExecuteFullSync_ALLAnalyzeFailureIsFailClosedPreflight(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer sourceDB.Close()
	targetDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	ts := newTestTaskService(t.TempDir())
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:           "all_meta_fail",
		Name:         "ALL Meta Fail",
		Mode:         taskEntity.SyncModeAll,
		SourceSchema: "src",
		Tables:       []string{"t1"},
		AllowNopkAll: true,
	})
	task.AcknowledgeNopkAllRisk(time.Now())
	task.Start()
	ts.tasks[task.Config.ID] = task

	runtime := &taskRuntime{
		sourceDB: sourceDB,
		targetDB: targetDB,
		analyzer: &failingAnalyzeAnalyzer{err: fmt.Errorf("metadata unavailable")},
	}
	err = ts.executeFullSync(context.Background(), task, runtime)
	require.Error(t, err)
	assert.ErrorIs(t, err, errFullSyncPreflight)
	assert.Contains(t, err.Error(), "analyze table")
	assert.NotEqual(t, taskEntity.SyncPhaseFullFailed, task.Context.SyncPhase)
}


// TestFormatBinlogPosition ??????????????????
func TestFormatBinlogPosition(t *testing.T) {
	cases := []struct {
		name string
		in   mysqlPositionLike
		want string
	}{
		{"empty file returns empty", mysqlPositionLike{Name: "", Pos: 0}, ""},
		{"file+pos formatted", mysqlPositionLike{Name: "mysql-bin.000001", Pos: 4}, "mysql-bin.000001:4"},
		{"file with zero pos still formats", mysqlPositionLike{Name: "mysql-bin.000099", Pos: 0}, "mysql-bin.000099:0"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := formatBinlogPosition(c.in.toMysql())
			assert.Equal(t, c.want, got)
		})
	}
}

// mysqlPositionLike ?????????????? mysql.Position?????? case ??????
type mysqlPositionLike struct {
	Name string
	Pos  uint32
}

func (p mysqlPositionLike) toMysql() mysql.Position {
	return mysql.Position{Name: p.Name, Pos: p.Pos}
}

// ==================== ??????? ====================

func TestThrottledPositionPersister_OnlyWritesAfterMinInterval(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "throttle"})
	ts.tasks[task.Config.ID] = task

	persist := ts.makeThrottledIncrementalPositionPersister(50 * time.Millisecond)

	persist("throttle", mysqlPositionLike{Name: "mysql-bin.000100", Pos: 1}.toMysql())
	assert.Equal(t, "mysql-bin.000100:1", task.Context.LastIncrementalPosition)

	// ???????? throttle ??????????????
	persist("throttle", mysqlPositionLike{Name: "mysql-bin.000100", Pos: 2}.toMysql())
	assert.Equal(t, "mysql-bin.000100:1", task.Context.LastIncrementalPosition,
		"second call within throttle window must be dropped")

	// ????????????
	time.Sleep(80 * time.Millisecond)
	persist("throttle", mysqlPositionLike{Name: "mysql-bin.000100", Pos: 3}.toMysql())
	assert.Equal(t, "mysql-bin.000100:3", task.Context.LastIncrementalPosition)
}

func TestThrottledPositionPersister_IgnoresEmptyPosition(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "throttle_empty"})
	ts.tasks[task.Config.ID] = task

	persist := ts.makeThrottledIncrementalPositionPersister(time.Millisecond)
	persist("throttle_empty", mysqlPositionLike{Name: "", Pos: 0}.toMysql())
	assert.Empty(t, task.Context.LastIncrementalPosition, "empty position must not be written")
}

func TestStopSchedulerIsIdempotent(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	ts.StartScheduler()
	assert.NotPanics(t, func() {
		ts.StopScheduler()
		ts.StopScheduler()
	})
}

// ============================================================
// ???????????
// ============================================================

func TestInitRunningProgress(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	taskID := "test_init_progress"
	entries := []tableEntry{
		{schema: "db1", table: "users", identity: nil},
		{schema: "db1", table: "orders", identity: nil},
		{schema: "db2", table: "products", identity: nil},
	}

	ts.initRunningProgress(taskID, entries, "full")

	rp, err := ts.GetTaskProgress(taskID)
	assert.NoError(t, err)
	assert.Equal(t, "full", rp.Phase)
	assert.Len(t, rp.Tables, 3)

	// ?????????? pending
	for _, ti := range rp.Tables {
		assert.Equal(t, "pending", ti.Status)
		assert.Equal(t, int64(0), ti.ProcessedRows)
		assert.Equal(t, float64(0), ti.ProgressPct)
	}
}

func TestStartTableProgress(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	taskID := "test_start_table"
	entries := []tableEntry{
		{schema: "db1", table: "users", identity: nil},
	}
	ts.initRunningProgress(taskID, entries, "full")

	ts.startTableProgress(taskID, "db1", "users", 10000)

	rp, err := ts.GetTaskProgress(taskID)
	assert.NoError(t, err)
	assert.Equal(t, "db1.users", rp.CurrentTable)

	var found bool
	for _, ti := range rp.Tables {
		if ti.Schema == "db1" && ti.Table == "users" {
			found = true
			assert.Equal(t, "running", ti.Status)
			assert.Equal(t, int64(10000), ti.TotalRows)
			assert.NotNil(t, ti.StartedAt)
		}
	}
	assert.True(t, found, "should find the users table")
}

func TestStartTableProgress_NonExistentTable(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	taskID := "test_start_nonexist"
	entries := []tableEntry{
		{schema: "db1", table: "users", identity: nil},
	}
	ts.initRunningProgress(taskID, entries, "full")

	// ???????????? panic
	assert.NotPanics(t, func() {
		ts.startTableProgress(taskID, "db1", "nonexistent", 0)
	})
}

func TestUpdateTableProgress(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	taskID := "test_update_progress"
	entries := []tableEntry{
		{schema: "db1", table: "users", identity: nil},
	}
	ts.initRunningProgress(taskID, entries, "full")
	ts.startTableProgress(taskID, "db1", "users", 10000)

	// ????? 2000 ???? 2 ?
	ts.updateTableProgress(taskID, "db1", "users", 2000, 2.0, time.Now().Add(-2*time.Second), 10000)

	rp, err := ts.GetTaskProgress(taskID)
	assert.NoError(t, err)

	for _, ti := range rp.Tables {
		if ti.Schema == "db1" && ti.Table == "users" {
			assert.Equal(t, int64(2000), ti.ProcessedRows)
			assert.Equal(t, 20.0, ti.ProgressPct)
			assert.Equal(t, 1000.0, ti.SpeedRowsSec)
		}
	}
}

func TestUpdateTableProgress_ProgressCappedAt100(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	taskID := "test_progress_capped"
	entries := []tableEntry{
		{schema: "db1", table: "users", identity: nil},
	}
	ts.initRunningProgress(taskID, entries, "full")
	ts.startTableProgress(taskID, "db1", "users", 100)

	// ???????
	ts.updateTableProgress(taskID, "db1", "users", 150, 1.0, time.Now().Add(-time.Second), 100)

	rp, err := ts.GetTaskProgress(taskID)
	assert.NoError(t, err)

	for _, ti := range rp.Tables {
		if ti.Schema == "db1" && ti.Table == "users" {
			assert.Equal(t, int64(150), ti.ProcessedRows)
			assert.Equal(t, 100.0, ti.ProgressPct) // ?? 100
		}
	}
}

func TestUpdateTableProgress_ZeroElapsed(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	taskID := "test_zero_elapsed"
	entries := []tableEntry{
		{schema: "db1", table: "users", identity: nil},
	}
	ts.initRunningProgress(taskID, entries, "full")
	ts.startTableProgress(taskID, "db1", "users", 10000)

	// elapsed=0 ?????????????
	assert.NotPanics(t, func() {
		ts.updateTableProgress(taskID, "db1", "users", 100, 0, time.Now().Add(-time.Second), 10000)
	})

	rp, _ := ts.GetTaskProgress(taskID)
	for _, ti := range rp.Tables {
		if ti.Schema == "db1" && ti.Table == "users" {
			assert.Equal(t, float64(0), ti.SpeedRowsSec)
		}
	}
}

func TestCompleteTableProgress(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	taskID := "test_complete_table"
	entries := []tableEntry{
		{schema: "db1", table: "users", identity: nil},
	}
	ts.initRunningProgress(taskID, entries, "full")
	ts.startTableProgress(taskID, "db1", "users", 10000)
	ts.completeTableProgress(taskID, "db1", "users")

	rp, err := ts.GetTaskProgress(taskID)
	assert.NoError(t, err)

	for _, ti := range rp.Tables {
		if ti.Schema == "db1" && ti.Table == "users" {
			assert.Equal(t, "completed", ti.Status)
			assert.Equal(t, 100.0, ti.ProgressPct)
			assert.NotNil(t, ti.CompletedAt)
		}
	}
}

func TestFailTableProgress(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	taskID := "test_fail_table"
	entries := []tableEntry{
		{schema: "db1", table: "users", identity: nil},
	}
	ts.initRunningProgress(taskID, entries, "full")
	ts.startTableProgress(taskID, "db1", "users", 10000)
	ts.failTableProgress(taskID, "db1", "users")

	rp, err := ts.GetTaskProgress(taskID)
	assert.NoError(t, err)

	for _, ti := range rp.Tables {
		if ti.Schema == "db1" && ti.Table == "users" {
			assert.Equal(t, "failed", ti.Status)
		}
	}
}

func TestRefreshOverallProgress(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	taskID := "test_refresh_overall"
	entries := []tableEntry{
		{schema: "db1", table: "users", identity: nil},
		{schema: "db1", table: "orders", identity: nil},
	}
	ts.initRunningProgress(taskID, entries, "full")

	// ??????
	ts.startTableProgress(taskID, "db1", "users", 1000)
	ts.updateTableProgress(taskID, "db1", "users", 1000, 1.0, time.Now().Add(-10*time.Second), 3000)
	ts.completeTableProgress(taskID, "db1", "users")

	// ???????
	ts.startTableProgress(taskID, "db1", "orders", 2000)
	ts.updateTableProgress(taskID, "db1", "orders", 500, 2.0, time.Now().Add(-10*time.Second), 3000)

	startTime := time.Now().Add(-10 * time.Second)
	ts.refreshOverallProgress(taskID, startTime, 3000)

	rp, err := ts.GetTaskProgress(taskID)
	assert.NoError(t, err)

	assert.Equal(t, "db1.orders", rp.CurrentTable)
	assert.GreaterOrEqual(t, rp.ElapsedSeconds, 10.0)
	assert.Greater(t, rp.OverallSpeed, 0.0)
	assert.Greater(t, rp.EstimatedRemain, 0.0)
}

func TestRefreshOverallProgress_UsesTaskTotalRowsWhenTableTotalsMissing(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	taskID := "test_refresh_task_total"
	entries := []tableEntry{
		{schema: "db1", table: "users", identity: nil},
	}
	ts.initRunningProgress(taskID, entries, "full")
	ts.startTableProgress(taskID, "db1", "users", 0)

	startTime := time.Now().Add(-10 * time.Second)
	ts.updateTableProgress(taskID, "db1", "users", 1000, 10.0, startTime, 3000)

	rp, err := ts.GetTaskProgress(taskID)
	assert.NoError(t, err)
	assert.Greater(t, rp.OverallSpeed, 0.0)
	assert.Greater(t, rp.EstimatedRemain, 0.0)
	assert.InDelta(t, 20.0, rp.EstimatedRemain, 2.0)
}

func TestRefreshOverallProgress_NoRemaining(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	taskID := "test_no_remaining"
	entries := []tableEntry{
		{schema: "db1", table: "users", identity: nil},
	}
	ts.initRunningProgress(taskID, entries, "full")
	ts.startTableProgress(taskID, "db1", "users", 1000)
	ts.updateTableProgress(taskID, "db1", "users", 1000, 1.0, time.Now().Add(-5*time.Second), 1000)
	ts.completeTableProgress(taskID, "db1", "users")

	startTime := time.Now().Add(-5 * time.Second)
	ts.refreshOverallProgress(taskID, startTime, 1000)

	rp, err := ts.GetTaskProgress(taskID)
	assert.NoError(t, err)
	// ???????????? -1
	assert.Equal(t, float64(-1), rp.EstimatedRemain)
}

func TestGetTaskProgress_NotFound(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	_, err := ts.GetTaskProgress("non_existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task progress not found")
}

func TestClearRunningProgress(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	taskID := "test_clear"
	entries := []tableEntry{
		{schema: "db1", table: "users", identity: nil},
	}
	ts.initRunningProgress(taskID, entries, "full")

	// ????
	_, err := ts.GetTaskProgress(taskID)
	assert.NoError(t, err)

	// ??
	ts.clearRunningProgress(taskID)

	// ?????
	_, err = ts.GetTaskProgress(taskID)
	assert.Error(t, err)
}

func TestProgressMethods_NoRunningProgress(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	taskID := "test_no_rp"

	// ?????? RunningProgress ??? panic
	assert.NotPanics(t, func() {
		ts.startTableProgress(taskID, "db1", "users", 100)
		ts.updateTableProgress(taskID, "db1", "users", 100, 1.0, time.Now().Add(-time.Second), 100)
		ts.completeTableProgress(taskID, "db1", "users")
		ts.failTableProgress(taskID, "db1", "users")
		ts.refreshOverallProgress(taskID, time.Now(), 100)
		ts.clearRunningProgress(taskID)
	})
}

func TestCompleteTask_ClearsRunningProgress(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	taskID := "test_complete_clears"
	_, _ = ts.CreateTask(taskEntity.TaskConfig{ID: taskID, Name: "Test"})

	entries := []tableEntry{
		{schema: "db1", table: "users", identity: nil},
	}
	ts.initRunningProgress(taskID, entries, "full")

	// ????
	ts.completeTask(taskID)

	// ??????
	_, err := ts.GetTaskProgress(taskID)
	assert.Error(t, err)
}

func TestPauseTask_ClearsRunningProgress(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	taskID := "test_pause_clears"
	_, _ = ts.CreateTask(taskEntity.TaskConfig{ID: taskID, Name: "Test"})

	entries := []tableEntry{
		{schema: "db1", table: "users", identity: nil},
	}
	ts.initRunningProgress(taskID, entries, "full")

	// ????
	_ = ts.PauseTask(taskID)

	// ??????
	_, err := ts.GetTaskProgress(taskID)
	assert.Error(t, err)
}

func TestRunningProgress_ConcurrentAccess(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	taskID := "test_concurrent"
	entries := []tableEntry{
		{schema: "db1", table: "users", identity: nil},
		{schema: "db1", table: "orders", identity: nil},
	}
	ts.initRunningProgress(taskID, entries, "full")
	ts.startTableProgress(taskID, "db1", "users", 10000)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ts.updateTableProgress(taskID, "db1", "users", 100, 1.0, time.Now().Add(-time.Second), 10000)
		}()
	}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ts.GetTaskProgress(taskID)
		}()
	}
	wg.Wait()

	rp, err := ts.GetTaskProgress(taskID)
	assert.NoError(t, err)
	for _, ti := range rp.Tables {
		if ti.Schema == "db1" && ti.Table == "users" {
			assert.Equal(t, int64(1000), ti.ProcessedRows)
		}
	}
}
