package service

import (
	"context"
	"database/sql"
	"database/sql/driver"
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

// mockAnalyzer 是一个模拟的 IdentityAnalyzer
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

func TestRestorePendingIndexes_ProcessesTablesConcurrently(t *testing.T) {
	targetDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	emptyStatsRows := sqlmock.NewRows([]string{"NON_UNIQUE", "INDEX_TYPE", "COLUMN_NAME", "SUB_PART", "SEQ_IN_INDEX"})

	// sqlmock checks expectations in order by default. Disable ordered matching
	// because the concurrent implementation may dispatch the two tables in any
	// order, and each table's CREATE INDEX may overlap with the other table's.
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("SELECT NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, SEQ_IN_INDEX").
		WithArgs("target_db", "users", "idx_users_name").
		WillReturnRows(emptyStatsRows)
	mock.ExpectQuery("SELECT NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, SEQ_IN_INDEX").
		WithArgs("target_db", "orders", "uk_orders_no").
		WillReturnRows(emptyStatsRows)
	mock.ExpectExec("CREATE INDEX `idx_users_name` ON `target_db`.`users` \\(`name`\\)").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE UNIQUE INDEX `uk_orders_no` ON `target_db`.`orders` \\(`order_no`\\)").
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

	// 不期望任何 CREATE INDEX 被执行
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

	// workers=1 让派发顺序确定：表 a 报错后主循环 break 出去，保证表 b 的 Exec 永远不会被发出。
	mock.ExpectQuery("SELECT NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, SEQ_IN_INDEX").
		WithArgs("db", "a", "idx_a").
		WillReturnRows(emptyStatsRows)
	mock.ExpectExec("CREATE INDEX `idx_a` ON `db`.`a` \\(`c`\\)").
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

// fakeIndexRestoreDriver 用于验证并发索引回放的 fail-fast / context 取消行为。
// 它通过 started / proceed channel 与测试协同，确保两张表的 DDL 都被派发后再继续。
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

// fakeIndexRestoreRows 返回空结果集，用于 fakeIndexRestoreConn 的 QueryContext。
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

	// 等待两张表的 DDL 都被派发，证明是并发执行。
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

func TestDropNonPrimaryKeyIndexes_SkipsFailedDrops(t *testing.T) {
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

	ts := &TaskService{}
	runtime := &taskRuntime{targetDB: targetDB}
	dropped, err := ts.dropNonPrimaryKeyIndexes(runtime, "db", "t")

	require.NoError(t, err)
	require.Len(t, dropped, 1)
	assert.Equal(t, "idx_a", dropped[0]["name"])
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
	// 不应再执行 CREATE INDEX

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

func TestEffectiveIndexRestoreWorkers(t *testing.T) {
	cases := []struct{ configured, workerCount, hardMax, want int }{
		{0, 0, 0, 4},    // 全默认 -> 4
		{0, 8, 0, 4},    // 回退 min(8,4)=4
		{0, 2, 0, 2},    // workerCount<4 -> 2
		{6, 8, 0, 6},    // 显式 6
		{32, 8, 16, 16}, // 受 hardMax 封顶
		{0, 0, 2, 2},    // hardMax<defaultCap -> 2
		{-1, 0, 0, 4},   // 负数当 0
	}
	for _, c := range cases {
		require.Equal(t, c.want, taskEntity.EffectiveIndexRestoreWorkers(c.configured, c.workerCount, c.hardMax))
	}
}

// newTestTaskService 创建一个使用自定义数据目录的测试任务服务
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

	// 测试设置为 false
	ts.SetEnableReadOnly(false)
	assert.False(t, ts.GetEnableReadOnly())

	// 测试设置为 true
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

	// 验证任务已添加到服务中
	retrievedTask, exists := ts.GetTask("test_task_1")
	assert.True(t, exists)
	assert.Equal(t, task.Config.ID, retrievedTask.Config.ID)
	assert.True(t, retrievedTask.Config.EnableDropTableBeforeDDL)
}

func TestGetTask(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// 测试获取不存在的任务
	task, exists := ts.GetTask("non_existent")
	assert.False(t, exists)
	assert.Nil(t, task)

	// 创建任务
	taskConfig := taskEntity.TaskConfig{
		ID:   "test_task_2",
		Name: "Test Task",
	}
	ts.CreateTask(taskConfig)

	// 测试获取存在的任务
	task, exists = ts.GetTask("test_task_2")
	assert.True(t, exists)
	assert.NotNil(t, task)
	assert.Equal(t, "test_task_2", task.Config.ID)
}

func TestGetAllTasks(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// 初始应该为空
	tasks := ts.GetAllTasks()
	assert.Empty(t, tasks)

	// 创建多个任务
	ts.CreateTask(taskEntity.TaskConfig{ID: "task_1_unique", Name: "Task 1"})
	ts.CreateTask(taskEntity.TaskConfig{ID: "task_2_unique", Name: "Task 2"})
	ts.CreateTask(taskEntity.TaskConfig{ID: "task_3_unique", Name: "Task 3"})

	// 获取所有任务
	tasks = ts.GetAllTasks()
	assert.Len(t, tasks, 3)

	taskIDs := make(map[string]bool)
	for _, task := range tasks {
		taskIDs[task.Config.ID] = true
	}

	assert.True(t, taskIDs["task_1_unique"])
	assert.True(t, taskIDs["task_2_unique"])
	assert.True(t, taskIDs["task_3_unique"])
}

func TestUpdateTask(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// 创建任务
	taskConfig := taskEntity.TaskConfig{
		ID:   "test_task_update",
		Name: "Original Name",
	}
	task, _ := ts.CreateTask(taskConfig)

	// 修改任务
	task.Config.Name = "Updated Name"
	task.Start()

	// 更新任务
	err := ts.UpdateTask(task)
	assert.NoError(t, err)

	// 验证更新
	retrievedTask, _ := ts.GetTask("test_task_update")
	assert.Equal(t, "Updated Name", retrievedTask.Config.Name)
	assert.Equal(t, taskEntity.TaskStatusRunning, retrievedTask.Context.Status)
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

	// 创建任务
	taskConfig := taskEntity.TaskConfig{
		ID:   "test_task_delete",
		Name: "Test Task",
	}
	ts.CreateTask(taskConfig)

	// 验证任务存在
	_, exists := ts.GetTask("test_task_delete")
	assert.True(t, exists)

	// 删除任务
	err := ts.DeleteTask("test_task_delete")
	assert.NoError(t, err)

	// 验证任务已删除
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

	// 创建任务
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

	// 启动任务（当前实现会在启动时重建真实数据库连接，单元测试环境下预期失败）
	ctx := context.Background()
	err = ts.StartTask(ctx, "test_task_start")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to initialize database connections")

	// 验证任务状态保持未启动
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

	// 创建并启动任务
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

	// Cancel the HTTP context — executeSync's context should remain alive
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

	// 创建并启动任务
	taskConfig := taskEntity.TaskConfig{
		ID:   "test_task_pause",
		Name: "Test Task",
	}
	task, _ := ts.CreateTask(taskConfig)
	task.Start()

	// 暂停任务
	err := ts.PauseTask("test_task_pause")
	assert.NoError(t, err)

	// 验证任务状态
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

	// 创建任务并设置错误状态
	taskConfig := taskEntity.TaskConfig{
		ID:   "test_task_skip",
		Name: "Test Task",
	}
	task, _ := ts.CreateTask(taskConfig)
	task.Context.Status = taskEntity.TaskStatusFailed
	task.Context.ErrorStack = "some error"

	// 跳过错误
	err := ts.SkipError("test_task_skip")
	assert.NoError(t, err)

	// 验证错误已清除
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

	// 创建任务
	taskConfig := taskEntity.TaskConfig{
		ID:     "test_task_metrics_unique",
		Name:   "Test Task",
		Tables: []string{"users", "orders"},
	}
	task, _ := ts.CreateTask(taskConfig)
	task.Context.ProcessedRows = 1000
	task.Context.TotalRows = 2000
	task.Context.CurrentPosition = "position_1"
	// 手动计算进度百分比
	task.Context.ProgressPercent = 50.0

	// 获取指标
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

	// 初始应该为 0
	count := ts.GetRunningTaskCount()
	assert.Equal(t, 0, count)

	// 创建并启动一些任务
	task1, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1_unique", Name: "Task 1"})
	task1.Start()

	task2, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_2_unique", Name: "Task 2"})
	task2.Start()

	_, _ = ts.CreateTask(taskEntity.TaskConfig{ID: "task_3_unique", Name: "Task 3"})
	// task3 保持暂停状态

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

	// 创建一些任务
	task1, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})
	task1.Start()

	task2, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_2", Name: "Task 2"})
	task2.Start()

	// 关闭服务
	err = ts.Close()
	assert.NoError(t, err)

	// 验证运行中的任务已暂停
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

	// 测试不存在的任务
	assert.True(t, ts.isTaskStopped("non_existent"))

	// 创建任务
	task, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})

	// 测试未运行的任务
	assert.True(t, ts.isTaskStopped("task_1"))

	// 启动任务
	task.Start()

	// 测试运行中的任务
	assert.False(t, ts.isTaskStopped("task_1"))
}

func TestUpdateTaskProgress(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// 创建任务
	_, _ = ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})

	// 更新进度
	ts.updateTaskProgress("task_1", 100, "position_1")

	// 验证更新
	retrievedTask, _ := ts.GetTask("task_1")
	assert.Equal(t, int64(100), retrievedTask.Context.ProcessedRows)
	assert.Equal(t, "position_1", retrievedTask.Context.CurrentPosition)
}

func TestIncrementTaskProgress(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// 创建任务
	task, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})
	task.Context.TotalRows = 1000

	// 增加进度
	ts.incrementTaskProgress("task_1", 100, "position_1")
	ts.incrementTaskProgress("task_1", 200, "position_2")

	// 验证更新
	retrievedTask, _ := ts.GetTask("task_1")
	assert.Equal(t, int64(300), retrievedTask.Context.ProcessedRows)
	assert.Equal(t, "position_2", retrievedTask.Context.CurrentPosition)
	assert.Equal(t, 30.0, retrievedTask.Context.ProgressPercent)
}

// spyTaskStorage 包装真实 FileTaskStorage，统计 Save 调用次数，用于测试持久化节流行为。
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

// newTestTaskServiceWithSpy 创建带 spy storage 的测试服务，用于观测 Save 调用次数。
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

// TestIncrementTaskProgress_Throttle 验证进度持久化节流：
// 1. 同一秒内多次 incrementTaskProgress 只触发一次 Save
// 2. 超过 1 秒后再次调用会触发新的 Save
func TestIncrementTaskProgress_Throttle(t *testing.T) {
	dataDir := t.TempDir()

	ts, spy := newTestTaskServiceWithSpy(dataDir)

	// 创建任务（CreateTask 内部会调用一次 Save，重置计数器）
	task, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})
	task.Context.TotalRows = 1000
	spy.ResetCount()

	// 第 1 次调用：首次，应触发 Save
	ts.incrementTaskProgress("task_1", 100, "pos_1")
	assert.Equal(t, 1, spy.SaveCount(), "首次调用应触发 Save")

	// 第 2 次调用：同一秒内，不应再触发 Save
	ts.incrementTaskProgress("task_1", 200, "pos_2")
	assert.Equal(t, 1, spy.SaveCount(), "1 秒内第二次调用不应触发 Save")

	// 第 3 次调用：同一秒内，仍不应触发 Save
	ts.incrementTaskProgress("task_1", 50, "pos_3")
	assert.Equal(t, 1, spy.SaveCount(), "1 秒内第三次调用不应触发 Save")

	// 验证内存值已正确累加（不受节流影响）
	retrievedTask, _ := ts.GetTask("task_1")
	assert.Equal(t, int64(350), retrievedTask.Context.ProcessedRows)
	assert.Equal(t, "pos_3", retrievedTask.Context.CurrentPosition)
	assert.Equal(t, 35.0, retrievedTask.Context.ProgressPercent)

	// 等待超过 1 秒，再次调用应触发新的 Save
	time.Sleep(1100 * time.Millisecond)
	ts.incrementTaskProgress("task_1", 150, "pos_4")
	assert.Equal(t, 2, spy.SaveCount(), "超过 1 秒后应触发新的 Save")

	// 验证内存值继续累加
	retrievedTask, _ = ts.GetTask("task_1")
	assert.Equal(t, int64(500), retrievedTask.Context.ProcessedRows)
	assert.Equal(t, "pos_4", retrievedTask.Context.CurrentPosition)
	assert.Equal(t, 50.0, retrievedTask.Context.ProgressPercent)
}

// TestIncrementTaskProgress_ThrottleReset 验证任务完成时会清理节流记录，
// 确保同一 taskID 快速重复运行时首秒能正常落盘。
func TestIncrementTaskProgress_ThrottleReset(t *testing.T) {
	dataDir := t.TempDir()

	ts, spy := newTestTaskServiceWithSpy(dataDir)

	// 创建任务
	task, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})
	task.Context.TotalRows = 1000
	spy.ResetCount()

	// 第一次运行：调用一次 progress
	ts.incrementTaskProgress("task_1", 100, "pos_1")
	assert.Equal(t, 1, spy.SaveCount())

	// 模拟任务完成时的清理路径（s.mu 下清理节流记录，progressMu 下清理运行时进度）
	ts.mu.Lock()
	ts.clearLastProgressPersistLocked("task_1")
	ts.mu.Unlock()
	ts.clearRunningProgress("task_1")

	// 验证 lastProgressPersist 已清理
	_, exists := ts.lastProgressPersist["task_1"]
	assert.False(t, exists, "任务完成时应清理 lastProgressPersist 记录")

	// 重新创建任务（模拟同一 taskID 快速重复运行）
	task2, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})
	task2.Context.TotalRows = 1000
	spy.ResetCount()

	// 新运行首次调用应触发 Save（因为节流记录已被清理）
	ts.incrementTaskProgress("task_1", 200, "pos_2")
	assert.Equal(t, 1, spy.SaveCount(), "清理节流记录后，新运行首次调用应触发 Save")
}

// TestDeleteTask_CleansThrottleRecord 验证 DeleteTask 会清理 lastProgressPersist。
func TestDeleteTask_CleansThrottleRecord(t *testing.T) {
	dataDir := t.TempDir()

	ts, spy := newTestTaskServiceWithSpy(dataDir)

	// 创建任务
	_, _ = ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})
	spy.ResetCount()

	// 调用一次 progress 以在 lastProgressPersist 中留下记录
	ts.incrementTaskProgress("task_1", 100, "pos_1")
	assert.Equal(t, 1, spy.SaveCount())

	// 验证记录存在
	_, exists := ts.lastProgressPersist["task_1"]
	assert.True(t, exists, "调用 incrementTaskProgress 后应存在节流记录")

	// 删除任务
	err := ts.DeleteTask("task_1")
	assert.NoError(t, err)

	// 验证节流记录已被清理
	_, exists = ts.lastProgressPersist["task_1"]
	assert.False(t, exists, "DeleteTask 应清理 lastProgressPersist 记录")
}

func TestUpdateTaskTotalRows(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// 创建任务
	_, _ = ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})

	// 更新总行数
	ts.updateTaskTotalRows("task_1", 5000)

	// 验证更新
	retrievedTask, _ := ts.GetTask("task_1")
	assert.Equal(t, int64(5000), retrievedTask.Context.TotalRows)
}

func TestUpdateTaskStatus(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// 创建任务
	_, _ = ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})

	// 更新状态
	ts.updateTaskStatus("task_1", taskEntity.TaskStatusFailed, "test error")

	// 验证更新
	retrievedTask, _ := ts.GetTask("task_1")
	assert.Equal(t, taskEntity.TaskStatusFailed, retrievedTask.Context.Status)
	assert.Equal(t, "test error", retrievedTask.Context.ErrorStack)
	assert.NotNil(t, retrievedTask.Context.EndTime)
}

func TestCompleteTask(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// 创建并启动任务
	task, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})
	task.Start()

	// 完成任务
	ts.completeTask("task_1")

	// 验证完成
	retrievedTask, _ := ts.GetTask("task_1")
	assert.Equal(t, taskEntity.TaskStatusCompleted, retrievedTask.Context.Status)
	assert.NotNil(t, retrievedTask.Context.EndTime)
}

func TestTaskStorage_Save_Error(t *testing.T) {
	// 创建一个无效的目录路径（使用保留字符）
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

	// 创建一个无效的 JSON 文件
	invalidJSON := `{"invalid": json}`
	filePath := dataDir + "/invalid.json"
	os.WriteFile(filePath, []byte(invalidJSON), 0644)

	// 加载应该跳过无效文件
	tasks, err := storage.LoadAll()
	assert.NoError(t, err)
	assert.Empty(t, tasks)
}

func TestTaskStorage_LoadAll_ReadError(t *testing.T) {
	dataDir := t.TempDir()

	storage := NewFileTaskStorage(dataDir)

	// 创建一个目录而不是文件
	dirPath := dataDir + "/subdir"
	os.MkdirAll(dirPath, 0755)

	// 加载应该跳过目录
	tasks, err := storage.LoadAll()
	assert.NoError(t, err)
	assert.Empty(t, tasks)
}

func TestTaskStorage_NewTaskStorage_Error(t *testing.T) {
	// 测试创建目录失败的情况
	// 由于 os.MkdirAll 在大多数情况下不会失败，这里只是测试函数不会 panic
	storage := NewFileTaskStorage("data")
	assert.NotNil(t, storage)
}

func TestTaskService_ConcurrentOperations(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)

	// 并发创建任务
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

	// 等待所有操作完成
	for i := 0; i < 10; i++ {
		<-done
	}

	// 验证所有任务都已创建
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

// TestSamplePKBoundariesImproved_KeysetStepsForAllWorkers 验证 keyset 步进算法：
// 取代串行深 OFFSET，预期发出 (n-1) 条 WHERE pk > ? ... LIMIT ? 查询，产出的边界单调递增。
func TestSamplePKBoundariesImproved_KeysetStepsForAllWorkers(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	const workers = 4
	const estimatedRows int64 = 40 // step = 40/4 = 10
	const step int64 = 10

	// Batch 1：从表头开始
	firstQuery := "SELECT `id` FROM `src`.`events` ORDER BY `id` ASC LIMIT ?"
	rows1 := sqlmock.NewRows([]string{"id"})
	for i := int64(1); i <= step; i++ {
		rows1.AddRow(fmt.Sprintf("%03d", i))
	}
	mock.ExpectQuery(firstQuery).WithArgs(step).WillReturnRows(rows1)

	// Batch 2..n-1：每批用上批的末位作为下界，算法共 n-1 轮，故除首轮外再 mock n-2 批
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
	// 校验末位为 "030"（第三批的 step*3 处）
	assert.Equal(t, "030", boundaries[workers-2])
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestSamplePKBoundariesImproved_EstimatedRowsTooSmall 估算行数小于 n*2 时直接返回错误，
// 表明不值得并行，调用方应回退到单线程 keyset 读取。
func TestSamplePKBoundariesImproved_EstimatedRowsTooSmall(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	var ts TaskService
	// estimatedRows=5, n=4，要求 >= n*2=8，期望失败且不发任何查询
	_, err = ts.samplePKBoundariesImproved(context.Background(), db, "src", "events", []string{"id"}, 5, 4)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient rows")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestSamplePKBoundariesImproved_EstimatedTooLargeConvergesAtTableEnd 估算行数偏大：
// 最后一批 rowsRead < step 即收敛，有效 worker 自动减少，调用方据此下调 intraWorkers。
func TestSamplePKBoundariesImproved_EstimatedTooLargeConvergesAtTableEnd(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	const workers = 4
	const estimatedRows int64 = 400 // step = 100，估算远大于实际
	const step int64 = 100

	// Batch 1: 满 batch
	rows1 := sqlmock.NewRows([]string{"id"})
	for i := int64(1); i <= step; i++ {
		rows1.AddRow(fmt.Sprintf("%03d", i))
	}
	mock.ExpectQuery("SELECT `id` FROM `src`.`events` ORDER BY `id` ASC LIMIT ?").
		WithArgs(step).
		WillReturnRows(rows1)

	// Batch 2: 满 batch
	rows2 := sqlmock.NewRows([]string{"id"})
	for i := int64(1); i <= step; i++ {
		rows2.AddRow(fmt.Sprintf("%03d", step+i))
	}
	mock.ExpectQuery("SELECT `id` FROM `src`.`events` WHERE `id` > ? ORDER BY `id` ASC LIMIT ?").
		WithArgs("100", step).
		WillReturnRows(rows2)

	// Batch 3: 只剩 30 行，rowsRead < step，触发收敛
	rows3 := sqlmock.NewRows([]string{"id"})
	for i := int64(1); i <= 30; i++ {
		rows3.AddRow(fmt.Sprintf("%03d", 2*step+i))
	}
	mock.ExpectQuery("SELECT `id` FROM `src`.`events` WHERE `id` > ? ORDER BY `id` ASC LIMIT ?").
		WithArgs("200", step).
		WillReturnRows(rows3)

	// Batch 4 不应发出（循环已收敛）

	var ts TaskService
	boundaries, err := ts.samplePKBoundariesImproved(context.Background(), db, "src", "events", []string{"id"}, estimatedRows, workers)
	require.NoError(t, err)
	// 实际行数 = 230, step=100，应产出 2 个边界（100, 200），有效 worker=3
	require.Len(t, boundaries, 2)
	assert.Equal(t, "100", boundaries[0])
	assert.Equal(t, "200", boundaries[1])
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestSamplePKBoundariesImproved_CompositeKeysetSteps 复合主键 keyset 步进：
// 验证 buildKeysetCompositeWhere 正确展开 (pk1,pk2) > (v1,v2) 为 OR 表达式，
// 边界产出 []interface{} 类型，与 comparePKWithBoundary 续传逻辑兼容。
func TestSamplePKBoundariesImproved_CompositeKeysetSteps(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	const workers = 3
	const estimatedRows int64 = 30
	const step int64 = 10

	// Batch 1：从表头开始，末位 = ["002", "b"]
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

	// 末位为 ["002","b"] 和 ["003","b"]，均为 []interface{} 类型
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
	err := ts.dropTargetTableIfNeeded(nil, "test", "users", true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "target connection is nil")

	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDB.Close()

	conn, err := mockDB.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	t.Run("disabled returns nil", func(t *testing.T) {
		err := ts.dropTargetTableIfNeeded(conn, "test", "users", false)
		assert.NoError(t, err)
	})

	t.Run("drops target table when enabled", func(t *testing.T) {
		mock.ExpectExec("DROP TABLE IF EXISTS `test`.`users`").WillReturnResult(sqlmock.NewResult(0, 0))
		err := ts.dropTargetTableIfNeeded(conn, "test", "users", true)
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
		err = ts.dropTargetTableIfNeeded(customConn, "prod_backup", "users_archive", true)
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
	targetMock.ExpectQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'target_db' AND table_name = 'users'").WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("users"))
	targetMock.ExpectExec("DROP TABLE IF EXISTS `target_db`.`users`").WillReturnResult(sqlmock.NewResult(0, 0))
	targetMock.ExpectExec("CREATE TABLE `target_db`.`users` LIKE `source_db`.`users`").WillReturnResult(sqlmock.NewResult(0, 0))

	t.Run("existing table is recreated when enabled", func(t *testing.T) {
		targetConn, err := targetDB.Conn(context.Background())
		require.NoError(t, err)
		defer targetConn.Close()
		sourceConn, err := sourceDB.Conn(context.Background())
		require.NoError(t, err)
		defer sourceConn.Close()

		runtime.sourceDB = sourceDB
		runtime.targetDB = targetDB

		// ensureTargetTable 只会在 CREATE TABLE LIKE 前走目标库连接的 DROP，因此这里仅验证执行路径
		_, err = ts.ensureTargetTable(runtime, "source_db", "target_db", "users", "users", false, true)
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

// ==================== captureFullSyncStartPosition（短锁取位点） ====================

// captureFullSyncStartPosition 已固定为"短锁取位点"模式：
// 永远先 FTWRL，SHOW MASTER STATUS 拿到位点后立即 UNLOCK。
func TestCaptureFullSyncStartPosition_ShortReadLock(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("FLUSH TABLES WITH READ LOCK").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).AddRow("mysql-bin.000123", 456, "", "", ""))
	mock.ExpectExec("UNLOCK TABLES").WillReturnResult(sqlmock.NewResult(0, 0))

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

// TestScheduleCronTaskPreservesCronConfig 验证 ScheduleCronTask 设置后 cron 字段保留，
// 这是回归测试：此前 ResetRepeat 误清了 ConfigureCronSchedule 刚写入的 cron 字段，
// 导致 nextCronRun 因 CronExpression 为空而失败。
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

// TestCompleteTaskCronReschedules 验证 cron 任务完成后会重新调度到下一次触发时间，
// 且 cron 配置不会被 ClearScheduleConfig 误清。
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

// TestCompleteTaskClearsStaleScheduleConfig 验证非 cron / repeat 已耗尽的任务完成时，
// 残留的调度字段被 ClearScheduleConfig 清空，避免前端在 COMPLETED 状态下误展示定时调度。
func TestCompleteTaskClearsStaleScheduleConfig(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "complete_clear", Name: "Complete Clear"})
	require.NoError(t, err)

	// 模拟残留的调度配置：repeat 模式但剩余次数为 0
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

// TestStartTaskImmediatelyClearsStaleCronFields 验证立即启动任务时清除残留的 cron 调度配置，
// 避免 RUNNING 期间残留调度字段造成状态不一致。
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

// TestCancelScheduleAfterCronRestoresStatus 验证对 cron 定时任务取消后能恢复到原始状态，
// 而非因 ScheduledFromStatus 被误清而退化为 PENDING。
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

// ==================== 阶段状态机：entity 助手方法 ====================

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

	task.ResetSyncPhase()
	assert.Equal(t, taskEntity.SyncPhaseInit, task.Context.SyncPhase)
	assert.Empty(t, task.Context.FullSyncStartPosition)
	assert.Empty(t, task.Context.LastIncrementalPosition)
	assert.Empty(t, task.Context.FullSyncFailedReason)
	assert.Nil(t, task.Context.FullSyncStartedAt)
	assert.Nil(t, task.Context.FullSyncCompletedAt)
	assert.False(t, task.HasFullSyncEverCompleted())
	assert.False(t, task.FullSyncIncomplete())
}

// ==================== 阶段状态机：service 持久化 helper ====================

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

// ==================== executeSync 入口门禁 + 暂停 bug ====================

// TestExecuteSync_IncrementalRequiresFullSync 覆盖修复 5：
// 纯 INCREMENTAL 模式下，若任务从未完成过全量，必须直接标 FAILED 而不是订阅 binlog。
func TestExecuteSync_IncrementalRequiresFullSync(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:   "incr_without_full",
		Name: "Incremental Without Full",
		Mode: taskEntity.SyncModeIncremental,
	})
	task.Start() // 状态 RUNNING，否则 failTaskUnlessCancelled 会忽略
	ts.tasks[task.Config.ID] = task

	ts.executeSync(context.Background(), task.Config.ID, &taskRuntime{})

	assert.Equal(t, taskEntity.TaskStatusFailed, task.Context.Status,
		"INCREMENTAL without prior FULL must fail-fast, not silently start a stale subscriber")
	assert.Contains(t, task.Context.ErrorStack, "requires a previously completed full sync")
}

// TestExecuteSync_IncrementalAllowedAfterFullCompleted 覆盖修复 5 的肯定路径：
// 一旦阶段为 FULL_COMPLETED，INCREMENTAL 必须放行进入 executeIncrementalSync。
// 这里用 nil runtime 触发 executeIncrementalSync 内部的早退保护，确认门禁本身已通过。
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

	// 门禁通过后进入 executeIncrementalSync，由于 runtime 为 nil 会把任务标 FAILED，
	// 但错误信息应来自"task runtime is nil"，证明门禁本身已放行。
	assert.Equal(t, taskEntity.TaskStatusFailed, task.Context.Status)
	assert.Contains(t, task.Context.ErrorStack, "runtime")
	assert.NotContains(t, task.Context.ErrorStack, "requires a previously completed full sync")
}

// TestExecuteSync_FullPauseDoesNotCompleteTask 覆盖暂停被错标 COMPLETED 的 P0 bug：
// 全量过程中用户暂停 → executeFullSync 入口的"早期停止检查"返回 errFullSyncStoppedByUser
// → executeSync 识别 sentinel 后既不能调用 completeTask 也不能把任务标 FAILED，
// 阶段也不能被推进到 FULL_COMPLETED。
//
// 这里复用 executeFullSync 入口的早停短路：不需要 mock SQL，只要 runtime 三件套非 nil
// 即可，校验通过后就会撞到 isTaskStopped 检查。
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

	// 模拟"用户在全量过程中按了暂停"
	task.Pause()

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

// TestExecuteSync_DatabaseRebuildPauseKeepsPhase 覆盖库级别重建被暂停时，
// executeFullSync 必须把 errFullSyncStoppedByUser 原样返回，不能调用 MarkFullSyncFailed，
// 因此 SyncPhase 保持 FULL_STARTED 而不被翻转为 FULL_FAILED。
func TestExecuteSync_DatabaseRebuildPauseKeepsPhase(t *testing.T) {
	sourceDB, sourceMock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer sourceDB.Close()
	targetDB, targetMock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer targetDB.Close()

	sourceMock.MatchExpectationsInOrder(false)
	targetMock.MatchExpectationsInOrder(false)

	// 源库：短锁取位点
	sourceMock.ExpectExec(regexp.QuoteMeta("FLUSH TABLES WITH READ LOCK")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	sourceMock.ExpectQuery(regexp.QuoteMeta("SHOW MASTER STATUS")).
		WillReturnRows(sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
			AddRow("mysql-bin.000001", uint32(4), "", "", ""))
	sourceMock.ExpectExec(regexp.QuoteMeta("UNLOCK TABLES")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	// 目标库：库级别重建。第一个 DROP 故意延迟，让测试有机会在重建过程中暂停任务；
	// 暂停后第二个库的迭代应被 isTaskStopped 拦截，不会执行 DROP/CREATE。
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

	// 在重建过程中暂停任务：延迟 100ms 确保 executeFullSync 已通过早停检查并进入 rebuild。
	go func() {
		time.Sleep(100 * time.Millisecond)
		task.Pause()
	}()

	ts.executeSync(context.Background(), task.Config.ID, runtime)

	assert.Equal(t, taskEntity.TaskStatusPaused, task.Context.Status,
		"paused database rebuild must NOT be marked COMPLETED")
	assert.Equal(t, taskEntity.SyncPhaseFullStarted, task.Context.SyncPhase,
		"paused database rebuild must keep FULL_STARTED phase, not flip to FULL_FAILED")

	require.NoError(t, sourceMock.ExpectationsWereMet())
	require.NoError(t, targetMock.ExpectationsWereMet())
}

// TestErrFullSyncStoppedByUser_IsSentinel 守住 sentinel 的可识别性：
// errors.Is 必须对包装后的错误仍为 true，否则上层 switch 会把暂停误判为失败。
func TestErrFullSyncStoppedByUser_IsSentinel(t *testing.T) {
	wrapped := fmt.Errorf("syncDatabasePair: %w", errFullSyncStoppedByUser)
	assert.True(t, errors.Is(wrapped, errFullSyncStoppedByUser))

	other := fmt.Errorf("something else")
	assert.False(t, errors.Is(other, errFullSyncStoppedByUser))
}

// TestFormatBinlogPosition 覆盖小工具，避免空位点污染任务存档。
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

// mysqlPositionLike 单测内联辅助：把测试参数转成 mysql.Position，避免在每个 case 内手写转换。
type mysqlPositionLike struct {
	Name string
	Pos  uint32
}

func (p mysqlPositionLike) toMysql() mysql.Position {
	return mysql.Position{Name: p.Name, Pos: p.Pos}
}

// ==================== 节流型位点回写 ====================

func TestThrottledPositionPersister_OnlyWritesAfterMinInterval(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "throttle"})
	ts.tasks[task.Config.ID] = task

	persist := ts.makeThrottledIncrementalPositionPersister(50 * time.Millisecond)

	persist("throttle", mysqlPositionLike{Name: "mysql-bin.000100", Pos: 1}.toMysql())
	assert.Equal(t, "mysql-bin.000100:1", task.Context.LastIncrementalPosition)

	// 立即再写一次（在 throttle 窗口内）应被丢弃，不更新存档
	persist("throttle", mysqlPositionLike{Name: "mysql-bin.000100", Pos: 2}.toMysql())
	assert.Equal(t, "mysql-bin.000100:1", task.Context.LastIncrementalPosition,
		"second call within throttle window must be dropped")

	// 等过节流窗口后再写应放行
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
// 运行时进度追踪单元测试
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

	// 验证所有表初始状态为 pending
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

	// 启动一个不存在的表，不应 panic
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

	// 模拟处理了 2000 行，耗时 2 秒
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

	// 处理超过总行数
	ts.updateTableProgress(taskID, "db1", "users", 150, 1.0, time.Now().Add(-time.Second), 100)

	rp, err := ts.GetTaskProgress(taskID)
	assert.NoError(t, err)

	for _, ti := range rp.Tables {
		if ti.Schema == "db1" && ti.Table == "users" {
			assert.Equal(t, int64(150), ti.ProcessedRows)
			assert.Equal(t, 100.0, ti.ProgressPct) // 封顶 100
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

	// elapsed=0 时不应计算速度（避免除零）
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

	// 第一张表完成
	ts.startTableProgress(taskID, "db1", "users", 1000)
	ts.updateTableProgress(taskID, "db1", "users", 1000, 1.0, time.Now().Add(-10*time.Second), 3000)
	ts.completeTableProgress(taskID, "db1", "users")

	// 第二张表进行中
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
	// 全部完成时，预估剩余应为 -1
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

	// 确认存在
	_, err := ts.GetTaskProgress(taskID)
	assert.NoError(t, err)

	// 清除
	ts.clearRunningProgress(taskID)

	// 确认已清除
	_, err = ts.GetTaskProgress(taskID)
	assert.Error(t, err)
}

func TestProgressMethods_NoRunningProgress(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	defer ts.Close()

	taskID := "test_no_rp"

	// 所有方法在无 RunningProgress 时不应 panic
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

	// 完成任务
	ts.completeTask(taskID)

	// 进度应被清除
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

	// 暂停任务
	_ = ts.PauseTask(taskID)

	// 进度应被清除
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
