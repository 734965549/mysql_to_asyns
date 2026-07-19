package fullload

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"mysql-to-sync/internal/metadata/domain/entity"

	"github.com/DATA-DOG/go-sqlmock"
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

	// 源：一次流式 SELECT 返回 2 行。
	srcMock.ExpectQuery("SELECT `id`, `name` FROM `s`.`t`").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).
			AddRow(int64(1), "a").
			AddRow(int64(2), "b"))

	// 目标：会话优化 + 事务写入 + 提交 + 恢复会话。
	dstMock.ExpectExec("SET @@SESSION.FOREIGN_KEY_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	dstMock.ExpectExec("SET @@SESSION.UNIQUE_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	dstMock.ExpectQuery("SELECT @@SESSION.FOREIGN_KEY_CHECKS").
		WillReturnRows(sqlmock.NewRows([]string{"fk", "uk"}).AddRow(0, 0))
	dstMock.ExpectExec("SET SESSION innodb_lock_wait_timeout=300").WillReturnResult(sqlmock.NewResult(0, 0))
	dstMock.ExpectBegin()
	dstMock.ExpectPrepare("INSERT INTO `s`.`t`")
	dstMock.ExpectPrepare("INSERT INTO `s`.`t`").
		ExpectExec().WillReturnResult(sqlmock.NewResult(0, 2))
	dstMock.ExpectCommit()
	dstMock.ExpectExec("SET @@SESSION.FOREIGN_KEY_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))
	dstMock.ExpectExec("SET @@SESSION.UNIQUE_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))
	dstMock.ExpectExec("SET SESSION innodb_lock_wait_timeout=50").WillReturnResult(sqlmock.NewResult(0, 0))

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
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := eng.Run(ctx, []*TableSpec{spec}); err != nil {
		t.Fatalf("engine run: %v", err)
	}

	mu.Lock()
	got := committedRows
	mu.Unlock()
	if got != 2 {
		t.Fatalf("committed rows=%d want 2", got)
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
