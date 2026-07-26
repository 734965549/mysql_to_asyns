package fullload

import (
	"context"
	"errors"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mysql-to-sync/internal/metadata/domain/entity"
	"mysql-to-sync/internal/metrics"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestEngine_NoPKEndToEnd 用双 sqlmock 验证无主键表的完整流水线：
// 读取 → 队列 → 会话优化 → 事务写入 → 提交 → 进度回调。
func TestEngine_NoPKEndToEnd(t *testing.T) {
	srcDB, srcMock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer srcDB.Close()
	dstDB, dstMock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp),
		sqlmock.MonitorPingsOption(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer dstDB.Close()
	dstMock.MatchExpectationsInOrder(false)
	srcMock.MatchExpectationsInOrder(false)

	// 源：InnoDB 预检 + 表级一致性快照 + MDL 后权威校验 + 流式 SELECT。
	expectInnoDBTable(srcMock, "s", "t")
	expectConsistentSnapshot(srcMock, "s", "t", "id")
	expectInnoDBTable(srcMock, "s", "t")
	srcMock.ExpectQuery("SELECT `id`, `name` FROM `s`.`t`").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).
			AddRow(int64(1), "a").
			AddRow(int64(2), "b"))
	expectSnapshotCommit(srcMock)

	// 目标：InnoDB 预检 + schema 互斥锁 + marker 表 + 结构校验 + 会话优化 + 事务写入 + marker + 提交 + 按 run_id 清理。
	expectInnoDBTable(dstMock, "s", "t")
	expectTxMarkerSchemaLock(dstMock, "s")
	dstMock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `s`.`__mts_fl_tx`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	expectTxMarkerTableOK(dstMock, "s")
	dstMock.ExpectExec("SET @@SESSION.FOREIGN_KEY_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	dstMock.ExpectExec("SET @@SESSION.UNIQUE_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	dstMock.ExpectQuery("SELECT @@SESSION.FOREIGN_KEY_CHECKS").
		WillReturnRows(sqlmock.NewRows([]string{"fk", "uk"}).AddRow(0, 0))
	dstMock.ExpectExec("SET SESSION innodb_lock_wait_timeout=300").WillReturnResult(sqlmock.NewResult(0, 0))
	dstMock.ExpectBegin()
	dstMock.ExpectPrepare("INSERT INTO `s`.`t`")
	dstMock.ExpectPrepare("INSERT INTO `s`.`t`").
		ExpectExec().WillReturnResult(sqlmock.NewResult(0, 2))
	dstMock.ExpectExec("INSERT INTO `s`.`__mts_fl_tx`").WillReturnResult(sqlmock.NewResult(0, 1))
	dstMock.ExpectCommit()
	dstMock.ExpectExec("SET @@SESSION.FOREIGN_KEY_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))
	dstMock.ExpectExec("SET @@SESSION.UNIQUE_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))
	dstMock.ExpectExec("SET SESSION innodb_lock_wait_timeout=50").WillReturnResult(sqlmock.NewResult(0, 0))
	dstMock.ExpectExec(regexp.QuoteMeta("DELETE FROM `s`.`__mts_fl_tx` WHERE `run_id` = ?")).
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	expectTxMarkerSchemaUnlock(dstMock, "s")

	spec := &TableSpec{
		SourceSchema: "s", TargetSchema: "s", SourceTable: "t", TargetTable: "t",
		Identity: &entity.TableIdentity{
			Strategy:     entity.FullColumnsStrategy,
			IdentifyCols: []string{"id", "name"},
			Columns:      []entity.ColumnMeta{{Name: "id"}, {Name: "name"}},
		},
	}

	var mu sync.Mutex
	var committedRows int64
	var readyFired int
	eng := &Engine{
		SourceDB: srcDB,
		TargetDB: dstDB,
		Options:  ResolveOptions(RawOptions{ReadWorkers: 1, WriteWorkers: 1, BatchSize: 1000}),
		Stats:    &Stats{},
		TaskID:   "test",
		OnCommit: func(schema, table string, rows, bytes int64) {
			mu.Lock()
			committedRows += rows
			mu.Unlock()
		},
		OnTableDataReady: func(schema, table string) error {
			mu.Lock()
			readyFired++
			mu.Unlock()
			return nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := eng.Run(ctx, []*TableSpec{spec}); err != nil {
		t.Fatalf("engine run: %v", err)
	}

	mu.Lock()
	got := committedRows
	ready := readyFired
	mu.Unlock()
	if got != 2 {
		t.Fatalf("committed rows=%d want 2", got)
	}
	if ready != 1 {
		t.Fatalf("OnTableDataReady fired=%d want 1", ready)
	}
	if snap := eng.Stats.Snapshot(); snap.CommittedRows != 2 {
		t.Fatalf("stats committed=%d want 2", snap.CommittedRows)
	}
	if err := srcMock.ExpectationsWereMet(); err != nil {
		t.Errorf("source unmet: %v", err)
	}
	if err := dstMock.ExpectationsWereMet(); err != nil {
		t.Errorf("target unmet: %v", err)
	}
}

func TestEngineCanceledContextCannotReportSuccess(t *testing.T) {
	srcDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer srcDB.Close()
	dstDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer dstDB.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	eng := &Engine{
		SourceDB: srcDB,
		TargetDB: dstDB,
		Options:  ResolveOptions(RawOptions{ReadWorkers: 1, WriteWorkers: 1}),
	}
	spec := &TableSpec{SourceSchema: "s", SourceTable: "t", Identity: &entity.TableIdentity{
		Strategy: entity.FullColumnsStrategy,
		Columns:  []entity.ColumnMeta{{Name: "id"}},
	}}
	if err := eng.Run(ctx, []*TableSpec{spec}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled engine must fail with context cancellation, got %v", err)
	}
}

func TestPushFinalMetricsClearsStaleSnapshotGauges(t *testing.T) {
	eng := &Engine{
		TaskID:  "task-final",
		Stats:   &Stats{},
		limiter: newSnapshotLimiter(2, 2),
	}
	atomic.StoreInt64(&eng.Stats.ActiveSnapshotGroups, 3)
	atomic.StoreInt64(&eng.Stats.OldestSnapshotAgeMillis, 12_000)
	eng.reported = eng.Stats.Snapshot()

	metrics.GetMetrics().SetTaskFullLoadOldestSnapshotAgeMillis("task-final", 12_000)
	metrics.GetMetrics().SetTaskFullLoadOldestSnapshotAgeMillis("task-other", 9_000)

	eng.pushFinalMetrics()

	snap := eng.Stats.Snapshot()
	if snap.ActiveSnapshotGroups != 0 {
		t.Fatalf("ActiveSnapshotGroups=%d want 0", snap.ActiveSnapshotGroups)
	}
	if snap.OldestSnapshotAgeMillis != 0 {
		t.Fatalf("OldestSnapshotAgeMillis=%d want 0", snap.OldestSnapshotAgeMillis)
	}
	// 本任务清零后，全局 gauge 仍应保留其他任务的 max。
	if got := testutil.ToFloat64(metrics.GetMetrics().FullLoadOldestSnapshotMs); got != 9000 {
		t.Fatalf("global oldest snapshot gauge=%v want 9000", got)
	}
	metrics.GetMetrics().ClearTaskFullLoadOldestSnapshotAge("task-other")
}
