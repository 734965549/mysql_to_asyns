package fullload

import (
	"context"
	"errors"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"mysql-to-sync/internal/metadata/domain/entity"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestEngine_NoPKEndToEnd 用双 sqlmock 验证无主键表的完整流水线：
// 读取 → 队列 → 会话优化 → 事务写入 → 提交 → 进度回调（普通短查询，不开一致性快照）。
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

	// 源：plain 路径仅流式 SELECT，不得出现一致性快照或表锁。
	srcMock.ExpectQuery("SELECT `id`, `name` FROM `s`.`t`").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).
			AddRow(int64(1), "a").
			AddRow(int64(2), "b"))

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

	var committedRows int64
	eng := &Engine{
		SourceDB: srcDB,
		TargetDB: dstDB,
		Options:  ResolveOptions(RawOptions{ReadWorkers: 2, WriteWorkers: 1, BatchSize: 1000}),
		Stats:    &Stats{},
		TaskID:   "test",
		OnCommit: func(schema, table string, rows, bytes int64) {
			atomic.AddInt64(&committedRows, rows)
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := eng.Run(ctx, []*TableSpec{spec}); err != nil {
		t.Fatalf("engine run: %v", err)
	}
	if got := atomic.LoadInt64(&committedRows); got != 2 {
		t.Fatalf("committed rows=%d want 2", got)
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

func TestGroupChunksByTable(t *testing.T) {
	specA := &TableSpec{SourceSchema: "s", SourceTable: "a"}
	specB := &TableSpec{SourceSchema: "s", SourceTable: "b"}
	grouped := groupChunksByTable([]*Chunk{
		{ID: "a#0", Spec: specA},
		{ID: "a#1", Spec: specA},
		{ID: "b#0", Spec: specB},
	})
	if len(grouped["s.a"]) != 2 || len(grouped["s.b"]) != 1 {
		t.Fatalf("unexpected grouping: %+v", grouped)
	}
}
