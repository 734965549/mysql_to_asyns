package fullload

import (
	"context"
	"database/sql/driver"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestInsertPrefix(t *testing.T) {
	b := &RowBatch{TargetSchema: "db", TargetTable: "t", Columns: []string{"id", "name"}}
	got := insertPrefix(b)
	want := "INSERT INTO `db`.`t` (`id`, `name`) VALUES "
	if got != want {
		t.Fatalf("prefix=%q want %q", got, want)
	}
}

func TestIsRetryableTxConflict(t *testing.T) {
	cases := map[string]bool{
		"Error 1213: Deadlock found when trying to get lock": true,
		"Error 1205: Lock wait timeout exceeded":             true,
		"try restarting transaction":                         true,
		"invalid connection":                                 false,
		"bad connection":                                     false,
		"Error 1062: Duplicate entry '1' for key 'PRIMARY'":  false,
		"syntax error":                                       false,
	}
	for msg, want := range cases {
		if got := isRetryableTxConflict(errors.New(msg)); got != want {
			t.Errorf("isRetryableTxConflict(%q)=%v want %v", msg, got, want)
		}
	}
	if isRetryableTxConflict(nil) {
		t.Error("nil should be non-retryable")
	}
}

func TestIsConnRetryable(t *testing.T) {
	cases := map[string]bool{
		"invalid connection":             true,
		"driver: bad connection":         true,
		"unexpected packet":              true,
		"connection was bad":             true,
		"dial tcp: connection refused":   true,
		"write: broken pipe":             true,
		"read: connection reset by peer": true,
		"Error 1213: Deadlock found":     false,
		"Error 1062: Duplicate entry":    false,
	}
	for msg, want := range cases {
		if got := isConnRetryable(errors.New(msg)); got != want {
			t.Errorf("isConnRetryable(%q)=%v want %v", msg, got, want)
		}
	}
	if !isConnRetryable(driver.ErrBadConn) {
		t.Error("driver.ErrBadConn should be retryable")
	}
	if isConnRetryable(nil) {
		t.Error("nil should be non-retryable")
	}
}

func TestInsertPrefixEscapesIdentifiers(t *testing.T) {
	b := &RowBatch{TargetSchema: "d`b", TargetTable: "t`1", Columns: []string{"c`1"}}
	want := "INSERT INTO `d``b`.`t``1` (`c``1`) VALUES "
	if got := insertPrefix(b); got != want {
		t.Fatalf("prefix=%q want %q", got, want)
	}
}

func TestWriteBatchInTx_MultiValueInsert(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	batch := &RowBatch{
		TargetSchema: "db",
		TargetTable:  "t",
		Columns:      []string{"id", "name"},
		Rows: [][]any{
			{int64(1), "a"},
			{int64(2), "b"},
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `db`.`t`").
		WithArgs(int64(1), "a", int64(2), "b").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	opt := ResolveOptions(RawOptions{BatchSize: 1000})
	if err := writeBatchInTx(context.Background(), tx, batch, opt); err != nil {
		t.Fatalf("writeBatchInTx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestWriteBatchInTx_SplitByPlaceholder(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 3 行、每行 1 列，限制每条语句最多 2 行 → 拆成 2 条 INSERT。
	batch := &RowBatch{
		TargetSchema: "db", TargetTable: "t",
		Columns: []string{"id"},
		Rows:    [][]any{{int64(1)}, {int64(2)}, {int64(3)}},
	}
	opt := ResolveOptions(RawOptions{BatchSize: 2})

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `db`.`t`").WithArgs(int64(1), int64(2)).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("INSERT INTO `db`.`t`").WithArgs(int64(3)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	tx, _ := db.Begin()
	if err := writeBatchInTx(context.Background(), tx, batch, opt); err != nil {
		t.Fatalf("writeBatchInTx: %v", err)
	}
	tx.Commit()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestWriteBatchInTxRejectsMismatchedRowWidth(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectRollback()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	batch := &RowBatch{Columns: []string{"id", "name"}, Rows: [][]any{{int64(1)}}}
	err = writeBatchInTx(context.Background(), tx, batch, ResolveOptions(RawOptions{}))
	if err == nil || !strings.Contains(err.Error(), "want 2 columns") {
		t.Fatalf("expected row width error, got %v", err)
	}
	_ = tx.Rollback()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestWriterLoopReplaysWholeTransactionOnDeadlock(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectWriteSession(mock, false)

	query := regexp.QuoteMeta("INSERT INTO `db`.`t` (`id`) VALUES (?)")
	mock.ExpectBegin()
	mock.ExpectPrepare(query)
	prepared := mock.ExpectPrepare(query)
	prepared.ExpectExec().WithArgs(int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	prepared.ExpectExec().WithArgs(int64(2)).WillReturnError(errors.New("Error 1213: Deadlock found"))
	mock.ExpectRollback()
	mock.ExpectBegin()
	replayPrepared := mock.ExpectPrepare(query)
	replayPrepared.ExpectExec().WithArgs(int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	replayPrepared.ExpectExec().WithArgs(int64(2)).WillReturnError(errors.New("lock wait timeout"))
	mock.ExpectRollback()
	mock.ExpectBegin()
	secondReplay := mock.ExpectPrepare(query)
	secondReplay.ExpectExec().WithArgs(int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	secondReplay.ExpectExec().WithArgs(int64(2)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `db`.`__mts_fl_tx` (`id`, `run_id`) VALUES (?, ?)")).
		WithArgs(sqlmock.AnyArg(), "run-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectRestoreWriteSession(mock, false)

	q := newBatchQueue(1024, &Stats{})
	for i := int64(1); i <= 2; i++ {
		if err := q.Put(context.Background(), &RowBatch{
			Schema: "s", Table: "t", TargetSchema: "db", TargetTable: "t",
			Columns: []string{"id"}, Rows: [][]any{{i}}, ApproxBytes: 8,
		}); err != nil {
			t.Fatal(err)
		}
	}
	q.Close()
	stats := &Stats{}
	var committed int64
	err = writerLoop(context.Background(), 0, db, q,
		ResolveOptions(RawOptions{BatchSize: 1, CommitRows: 10}), stats,
		func(_, _ string, rows, _ int64) { committed += rows }, nil, nil, "run-1")
	if err != nil {
		t.Fatalf("writerLoop: %v", err)
	}
	if committed != 2 || stats.Snapshot().TxReplays != 2 || stats.Snapshot().LockRetries != 2 || stats.Snapshot().CommittedRows != 2 {
		t.Fatalf("committed=%d stats=%+v", committed, stats.Snapshot())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestWriterLoopReconnectsOnInvalidConnection(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	expectWriteSession(mock, false)
	query := regexp.QuoteMeta("INSERT INTO `db`.`t` (`id`) VALUES (?)")
	mock.ExpectBegin()
	mock.ExpectPrepare(query)
	prepared := mock.ExpectPrepare(query)
	prepared.ExpectExec().WithArgs(int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	prepared.ExpectExec().WithArgs(int64(2)).WillReturnError(errors.New("invalid connection"))
	mock.ExpectRollback()

	// 换连后重建会话并重放未提交批次（含已成功但未提交的 #1）。
	expectWriteSession(mock, false)
	mock.ExpectBegin()
	mock.ExpectPrepare(query)
	replayPrepared := mock.ExpectPrepare(query)
	replayPrepared.ExpectExec().WithArgs(int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	replayPrepared.ExpectExec().WithArgs(int64(2)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `db`.`__mts_fl_tx` (`id`, `run_id`) VALUES (?, ?)")).
		WithArgs(sqlmock.AnyArg(), "run-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectRestoreWriteSession(mock, false)

	q := newBatchQueue(1024, &Stats{})
	for i := int64(1); i <= 2; i++ {
		if err := q.Put(context.Background(), &RowBatch{
			Schema: "s", Table: "t", TargetSchema: "db", TargetTable: "t",
			Columns: []string{"id"}, Rows: [][]any{{i}}, ApproxBytes: 8, ChunkID: "chunk",
		}); err != nil {
			t.Fatal(err)
		}
	}
	q.Close()
	stats := &Stats{}
	var committed int64
	err = writerLoop(context.Background(), 0, db, q,
		ResolveOptions(RawOptions{BatchSize: 1, CommitRows: 10}), stats,
		func(_, _ string, rows, _ int64) { committed += rows }, nil, nil, "run-1")
	if err != nil {
		t.Fatalf("writerLoop: %v", err)
	}
	if committed != 2 || stats.Snapshot().CommittedRows != 2 || stats.Snapshot().TxReplays != 1 {
		t.Fatalf("committed=%d stats=%+v", committed, stats.Snapshot())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestWriterLoopRecoversUnknownCommitWhenRolledBack(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectWriteSession(mock, false)
	mock.ExpectBegin()
	query := regexp.QuoteMeta("INSERT INTO `db`.`t` (`id`) VALUES (?)")
	mock.ExpectPrepare(query)
	prepared := mock.ExpectPrepare(query)
	prepared.ExpectExec().WithArgs(int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `db`.`__mts_fl_tx` (`id`, `run_id`) VALUES (?, ?)")).
		WithArgs(sqlmock.AnyArg(), "run-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(errors.New("invalid connection"))

	// 换连后锁定读探测 marker：不存在 → 已回滚，重放后再提交。
	expectWriteSession(mock, false)
	mock.ExpectBegin()
	verifyQ := regexp.QuoteMeta("SELECT 1 FROM `db`.`__mts_fl_tx` WHERE `id` = ? FOR UPDATE")
	mock.ExpectQuery(verifyQ).WithArgs(sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"1"}))
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectPrepare(query)
	replayPrepared := mock.ExpectPrepare(query)
	replayPrepared.ExpectExec().WithArgs(int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `db`.`__mts_fl_tx` (`id`, `run_id`) VALUES (?, ?)")).
		WithArgs(sqlmock.AnyArg(), "run-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectRestoreWriteSession(mock, false)

	q := newBatchQueue(1024, &Stats{})
	_ = q.Put(context.Background(), &RowBatch{
		Schema: "s", Table: "t", TargetSchema: "db", TargetTable: "t",
		Columns: []string{"id"},
		Rows:    [][]any{{int64(1)}}, ApproxBytes: 8,
	})
	q.Close()
	stats := &Stats{}
	var committed int64
	err = writerLoop(context.Background(), 0, db, q, ResolveOptions(RawOptions{BatchSize: 1}), stats,
		func(_, _ string, rows, _ int64) { committed += rows }, nil, nil, "run-1")
	if err != nil {
		t.Fatalf("writerLoop: %v", err)
	}
	if committed != 1 || stats.Snapshot().CommittedRows != 1 || stats.Snapshot().TxReplays != 1 {
		t.Fatalf("committed=%d stats=%+v", committed, stats.Snapshot())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestWriterLoopRecoversUnknownCommitWhenApplied(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectWriteSession(mock, false)
	mock.ExpectBegin()
	query := regexp.QuoteMeta("INSERT INTO `db`.`t` (`id`) VALUES (?)")
	mock.ExpectPrepare(query)
	prepared := mock.ExpectPrepare(query)
	prepared.ExpectExec().WithArgs(int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `db`.`__mts_fl_tx` (`id`, `run_id`) VALUES (?, ?)")).
		WithArgs(sqlmock.AnyArg(), "run-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(errors.New("bad connection"))

	// 换连后锁定读探测 marker：已存在 → 视为提交成功，不重放。
	expectWriteSession(mock, false)
	mock.ExpectBegin()
	verifyQ := regexp.QuoteMeta("SELECT 1 FROM `db`.`__mts_fl_tx` WHERE `id` = ? FOR UPDATE")
	mock.ExpectQuery(verifyQ).WithArgs(sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectCommit()
	expectRestoreWriteSession(mock, false)

	q := newBatchQueue(1024, &Stats{})
	_ = q.Put(context.Background(), &RowBatch{
		Schema: "s", Table: "t", TargetSchema: "db", TargetTable: "t",
		Columns: []string{"id"},
		Rows:    [][]any{{int64(1)}}, ApproxBytes: 8,
	})
	q.Close()
	stats := &Stats{}
	var committed int64
	err = writerLoop(context.Background(), 0, db, q, ResolveOptions(RawOptions{BatchSize: 1}), stats,
		func(_, _ string, rows, _ int64) { committed += rows }, nil, nil, "run-1")
	if err != nil {
		t.Fatalf("writerLoop: %v", err)
	}
	if committed != 1 || stats.Snapshot().CommittedRows != 1 || stats.Snapshot().TxReplays != 0 {
		t.Fatalf("committed=%d stats=%+v", committed, stats.Snapshot())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestWriterLoopCommitNonConnErrorStillFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectWriteSession(mock, false)
	mock.ExpectBegin()
	query := regexp.QuoteMeta("INSERT INTO `db`.`t` (`id`) VALUES (?)")
	mock.ExpectPrepare(query)
	prepared := mock.ExpectPrepare(query)
	prepared.ExpectExec().WithArgs(int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `db`.`__mts_fl_tx` (`id`, `run_id`) VALUES (?, ?)")).
		WithArgs(sqlmock.AnyArg(), "run-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(errors.New("disk full"))
	expectRestoreWriteSession(mock, false)

	q := newBatchQueue(1024, &Stats{})
	_ = q.Put(context.Background(), &RowBatch{
		Schema: "s", Table: "t", TargetSchema: "db", TargetTable: "t",
		Columns: []string{"id"},
		Rows:    [][]any{{int64(1)}}, ApproxBytes: 8,
	})
	q.Close()
	err = writerLoop(context.Background(), 0, db, q, ResolveOptions(RawOptions{BatchSize: 1}), &Stats{}, nil, nil, nil, "run-1")
	if err == nil || !strings.Contains(err.Error(), "disk full") || strings.Contains(err.Error(), "outcome unknown") {
		t.Fatalf("expected plain commit error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestWriterLoopFullColumnsIdenticalRowsAcrossTxUsesDistinctMarkers 证明：即使业务行完全相同，
// 各写事务仍使用独立 marker；Commit 未知时不会因前一事务已提交的相同行而误判。
func TestWriterLoopFullColumnsIdenticalRowsAcrossTxUsesDistinctMarkers(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectWriteSession(mock, false)

	query := regexp.QuoteMeta("INSERT INTO `db`.`t` (`a`, `b`) VALUES (?, ?)")
	markerQ := regexp.QuoteMeta("INSERT INTO `db`.`__mts_fl_tx` (`id`, `run_id`) VALUES (?, ?)")
	verifyQ := regexp.QuoteMeta("SELECT 1 FROM `db`.`__mts_fl_tx` WHERE `id` = ? FOR UPDATE")

	// 事务 1：提交成功（含相同业务行 R）。
	mock.ExpectBegin()
	mock.ExpectPrepare(query)
	p1 := mock.ExpectPrepare(query)
	p1.ExpectExec().WithArgs("R", 1).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(markerQ).WithArgs(sqlmock.AnyArg(), "run-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	// 事务 2：写入相同行 R，Commit 连接错误；marker 不存在 → 判定回滚并重放。
	mock.ExpectBegin()
	p2 := mock.ExpectPrepare(query)
	p2.ExpectExec().WithArgs("R", 1).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(markerQ).WithArgs(sqlmock.AnyArg(), "run-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(errors.New("invalid connection"))

	expectWriteSession(mock, false)
	mock.ExpectBegin()
	mock.ExpectQuery(verifyQ).WithArgs(sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"1"}))
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectPrepare(query)
	replay := mock.ExpectPrepare(query)
	replay.ExpectExec().WithArgs("R", 1).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(markerQ).WithArgs(sqlmock.AnyArg(), "run-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectRestoreWriteSession(mock, false)

	q := newBatchQueue(1024, &Stats{})
	for i := 0; i < 2; i++ {
		if err := q.Put(context.Background(), &RowBatch{
			Schema: "s", Table: "t", TargetSchema: "db", TargetTable: "t",
			Columns: []string{"a", "b"},
			Rows:    [][]any{{"R", 1}}, ApproxBytes: 8,
		}); err != nil {
			t.Fatal(err)
		}
	}
	q.Close()
	stats := &Stats{}
	var committed int64
	err = writerLoop(context.Background(), 0, db, q,
		ResolveOptions(RawOptions{BatchSize: 1, CommitRows: 1}), stats,
		func(_, _ string, rows, _ int64) { committed += rows }, nil, nil, "run-1")
	if err != nil {
		t.Fatalf("writerLoop: %v", err)
	}
	if committed != 2 || stats.Snapshot().TxReplays != 1 {
		t.Fatalf("committed=%d stats=%+v", committed, stats.Snapshot())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTxMarkerAppliedLockingRead(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1 FROM `db`.`__mts_fl_tx` WHERE `id` = ? FOR UPDATE")).
		WithArgs("m1").
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectCommit()
	ok, err := txMarkerApplied(context.Background(), conn, "db", "m1")
	if err != nil || !ok {
		t.Fatalf("applied=%v err=%v", ok, err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1 FROM `db`.`__mts_fl_tx` WHERE `id` = ? FOR UPDATE")).
		WithArgs("m2").
		WillReturnRows(sqlmock.NewRows([]string{"1"}))
	mock.ExpectRollback()
	ok, err = txMarkerApplied(context.Background(), conn, "db", "m2")
	if err != nil || ok {
		t.Fatalf("applied=%v err=%v want false", ok, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAssertTargetTablesInnoDBRejectsMyISAM(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")).
		WithArgs("db", "t").
		WillReturnRows(sqlmock.NewRows([]string{"ENGINE"}).AddRow("MyISAM"))
	err = assertTargetTablesInnoDB(context.Background(), db, []*TableSpec{{
		TargetSchema: "db", TargetTable: "t",
	}})
	if err == nil || !strings.Contains(err.Error(), "not InnoDB") {
		t.Fatalf("expected non-InnoDB error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestWriterLoopCommitsOnIntervalWithoutNextBatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectWriteSession(mock, false)
	mock.ExpectBegin()
	query := regexp.QuoteMeta("INSERT INTO `db`.`t` (`id`) VALUES (?)")
	mock.ExpectPrepare(query)
	prepared := mock.ExpectPrepare(query)
	prepared.ExpectExec().WithArgs(int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `db`.`__mts_fl_tx` (`id`, `run_id`) VALUES (?, ?)")).
		WithArgs(sqlmock.AnyArg(), "run-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectRestoreWriteSession(mock, false)

	q := newBatchQueue(1024, &Stats{})
	_ = q.Put(context.Background(), &RowBatch{
		Schema: "s", Table: "t", TargetSchema: "db", TargetTable: "t",
		Columns: []string{"id"}, Rows: [][]any{{int64(1)}}, ApproxBytes: 8,
	})
	opt := ResolveOptions(RawOptions{BatchSize: 1, CommitRows: 10})
	opt.CommitInterval = 20 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = writerLoop(ctx, 0, db, q, opt, &Stats{}, func(_, _ string, _, _ int64) { q.Close() }, nil, nil, "run-1")
	if err != nil {
		t.Fatalf("writerLoop: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func expectWriteSession(mock sqlmock.Sqlmock, skipBinlog bool) {
	mock.ExpectExec("SET @@SESSION.FOREIGN_KEY_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET @@SESSION.UNIQUE_CHECKS=0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT @@SESSION.FOREIGN_KEY_CHECKS").WillReturnRows(sqlmock.NewRows([]string{"fk", "uk"}).AddRow(0, 0))
	mock.ExpectExec("SET SESSION innodb_lock_wait_timeout=300").WillReturnResult(sqlmock.NewResult(0, 0))
	if skipBinlog {
		mock.ExpectExec("SET SESSION sql_log_bin=0").WillReturnResult(sqlmock.NewResult(0, 0))
	}
}

func expectRestoreWriteSession(mock sqlmock.Sqlmock, skipBinlog bool) {
	mock.ExpectExec("SET @@SESSION.FOREIGN_KEY_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET @@SESSION.UNIQUE_CHECKS=1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET SESSION innodb_lock_wait_timeout=50").WillReturnResult(sqlmock.NewResult(0, 0))
	if skipBinlog {
		mock.ExpectExec("SET SESSION sql_log_bin=1").WillReturnResult(sqlmock.NewResult(0, 0))
	}
}
