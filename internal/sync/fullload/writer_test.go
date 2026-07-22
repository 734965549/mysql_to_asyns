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
		func(_, _ string, rows, _ int64) { committed += rows }, nil, nil)
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
		func(_, _ string, rows, _ int64) { committed += rows }, nil, nil)
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

func TestWriterLoopDoesNotReplayUnknownCommitOutcome(t *testing.T) {
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
	mock.ExpectCommit().WillReturnError(errors.New("bad connection"))
	expectRestoreWriteSession(mock, false)

	q := newBatchQueue(1024, &Stats{})
	_ = q.Put(context.Background(), &RowBatch{
		Schema: "s", Table: "t", TargetSchema: "db", TargetTable: "t",
		Columns: []string{"id"}, Rows: [][]any{{int64(1)}}, ApproxBytes: 8,
	})
	q.Close()
	stats := &Stats{}
	err = writerLoop(context.Background(), 0, db, q, ResolveOptions(RawOptions{BatchSize: 1}), stats, nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "outcome unknown") {
		t.Fatalf("expected unknown commit outcome error, got %v", err)
	}
	if snap := stats.Snapshot(); snap.CommittedRows != 0 || snap.TxReplays != 0 {
		t.Fatalf("commit failure must not count/replay: %+v", snap)
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
	err = writerLoop(ctx, 0, db, q, opt, &Stats{}, func(_, _ string, _, _ int64) { q.Close() }, nil, nil)
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
