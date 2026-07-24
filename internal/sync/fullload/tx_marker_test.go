package fullload

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestAssertNoReservedTargetTableNames(t *testing.T) {
	err := assertNoReservedTargetTableNames([]*TableSpec{
		{TargetSchema: "db", TargetTable: "orders"},
		{TargetSchema: "db", TargetTable: "__MTS_FL_TX"},
	})
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("expected reserved name conflict, got %v", err)
	}

	if err := assertNoReservedTargetTableNames([]*TableSpec{
		{TargetSchema: "db", TargetTable: "orders"},
		nil,
		{TargetSchema: "db", TargetTable: "users"},
	}); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}

func TestEnsureTxMarkerTablesRejectsReservedName(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = ensureTxMarkerTables(context.Background(), db, []*TableSpec{
		{TargetSchema: "db", TargetTable: "__mts_fl_tx"},
	})
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("expected reserved name rejection before DDL, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureTxMarkerTablesRejectsExistingMyISAM(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `db`.`__mts_fl_tx`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT TABLE_TYPE, ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")).
		WithArgs("db", txMarkerTableName).
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_TYPE", "ENGINE"}).AddRow("BASE TABLE", "MyISAM"))

	err = ensureTxMarkerTables(context.Background(), db, []*TableSpec{
		{TargetSchema: "db", TargetTable: "t"},
	})
	if err == nil || !strings.Contains(err.Error(), "not InnoDB") {
		t.Fatalf("expected MyISAM marker rejection, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureTxMarkerTablesRejectsMissingUniqueKey(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `db`.`__mts_fl_tx`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT TABLE_TYPE, ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")).
		WithArgs("db", txMarkerTableName).
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_TYPE", "ENGINE"}).AddRow("BASE TABLE", "InnoDB"))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?")).
		WithArgs("db", txMarkerTableName, "id").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH"}).
			AddRow("id", "NO", "char", int64(36)))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?")).
		WithArgs("db", txMarkerTableName, "run_id").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH"}).
			AddRow("run_id", "NO", "varchar", int64(txMarkerRunIDMaxLen)))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT INDEX_NAME, SUB_PART FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ? AND NON_UNIQUE = 0 AND SEQ_IN_INDEX = 1")).
		WithArgs("db", txMarkerTableName, "id").
		WillReturnRows(sqlmock.NewRows([]string{"INDEX_NAME", "SUB_PART"}))

	err = ensureTxMarkerTables(context.Background(), db, []*TableSpec{
		{TargetSchema: "db", TargetTable: "t"},
	})
	if err == nil || !strings.Contains(err.Error(), "PRIMARY KEY") {
		t.Fatalf("expected unique key rejection, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureTxMarkerTablesRejectsPrefixUniqueKey(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `db`.`__mts_fl_tx`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT TABLE_TYPE, ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")).
		WithArgs("db", txMarkerTableName).
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_TYPE", "ENGINE"}).AddRow("BASE TABLE", "InnoDB"))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?")).
		WithArgs("db", txMarkerTableName, "id").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH"}).
			AddRow("id", "NO", "char", int64(36)))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?")).
		WithArgs("db", txMarkerTableName, "run_id").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH"}).
			AddRow("run_id", "NO", "varchar", int64(txMarkerRunIDMaxLen)))
	// UNIQUE(id(1))：单列但 SUB_PART=1，必须拒绝。
	mock.ExpectQuery(regexp.QuoteMeta("SELECT INDEX_NAME, SUB_PART FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ? AND NON_UNIQUE = 0 AND SEQ_IN_INDEX = 1")).
		WithArgs("db", txMarkerTableName, "id").
		WillReturnRows(sqlmock.NewRows([]string{"INDEX_NAME", "SUB_PART"}).AddRow("id", int64(1)))

	err = ensureTxMarkerTables(context.Background(), db, []*TableSpec{
		{TargetSchema: "db", TargetTable: "t"},
	})
	if err == nil || !strings.Contains(err.Error(), "SUB_PART") {
		t.Fatalf("expected prefix unique key rejection, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureTxMarkerTablesRejectsMissingRunID(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `db`.`__mts_fl_tx`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT TABLE_TYPE, ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")).
		WithArgs("db", txMarkerTableName).
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_TYPE", "ENGINE"}).AddRow("BASE TABLE", "InnoDB"))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?")).
		WithArgs("db", txMarkerTableName, "id").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH"}).
			AddRow("id", "NO", "char", int64(36)))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?")).
		WithArgs("db", txMarkerTableName, "run_id").
		WillReturnError(sql.ErrNoRows)

	err = ensureTxMarkerTables(context.Background(), db, []*TableSpec{
		{TargetSchema: "db", TargetTable: "t"},
	})
	if err == nil || !strings.Contains(err.Error(), "run_id") {
		t.Fatalf("expected missing run_id rejection, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureTxMarkerTablesRejectsShortIDColumn(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `db`.`__mts_fl_tx`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT TABLE_TYPE, ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")).
		WithArgs("db", txMarkerTableName).
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_TYPE", "ENGINE"}).AddRow("BASE TABLE", "InnoDB"))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?")).
		WithArgs("db", txMarkerTableName, "id").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH"}).
			AddRow("id", "NO", "varchar", int64(16)))

	err = ensureTxMarkerTables(context.Background(), db, []*TableSpec{
		{TargetSchema: "db", TargetTable: "t"},
	})
	if err == nil || !strings.Contains(err.Error(), "cannot store UUID") {
		t.Fatalf("expected short id type rejection, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureTxMarkerTablesOK(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `db`.`__mts_fl_tx`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	expectTxMarkerTableOK(mock, "db")

	err = ensureTxMarkerTables(context.Background(), db, []*TableSpec{
		{TargetSchema: "db", TargetTable: "t"},
		{TargetSchema: "db", TargetTable: "t2"}, // same schema: create once
	})
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCleanupTxMarkerRowsDedupesSchemas(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `db`.`__mts_fl_tx` WHERE `run_id` = ?")).
		WithArgs("run-abc").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `other`.`__mts_fl_tx` WHERE `run_id` = ?")).
		WithArgs("run-abc").
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = cleanupTxMarkerRows(context.Background(), db, []*TableSpec{
		{TargetSchema: "db", TargetTable: "t1"},
		{TargetSchema: "db", TargetTable: "t2"},
		{TargetSchema: "other", TargetTable: "t3"},
		nil,
	}, "run-abc")
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireTxMarkerSchemaLocksRejectsBusy(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)

	name := txMarkerSchemaLockName("db")
	mock.ExpectQuery(regexp.QuoteMeta("SELECT @@SESSION.wait_timeout")).
		WillReturnRows(sqlmock.NewRows([]string{"wait_timeout"}).AddRow(int64(28800)))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, ?)")).
		WithArgs(name, txMarkerSchemaLockTimeoutSec).
		WillReturnRows(sqlmock.NewRows([]string{"GET_LOCK(?, ?)"}).AddRow(0))

	_, err = acquireTxMarkerSchemaLocks(context.Background(), db, []*TableSpec{
		{TargetSchema: "db", TargetTable: "t"},
	})
	if err == nil || !strings.Contains(err.Error(), "locked by another") {
		t.Fatalf("expected busy schema lock rejection, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireSchemaLocksRejectsSmallTargetPool(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	_, err = AcquireSchemaLocks(context.Background(), db, []string{"db"})
	if err == nil || !strings.Contains(err.Error(), "target_max_open_conns >= 2") {
		t.Fatalf("expected pool capacity rejection, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaLockHeartbeatIntervalRespectsWaitTimeout(t *testing.T) {
	cases := []struct {
		wait int64
		want time.Duration
	}{
		{28800, schemaLockHeartbeatDefault},
		{60, schemaLockHeartbeatDefault}, // 60/3=20s capped to 15s
		{30, 10 * time.Second},
		{10, time.Duration(10) * time.Second / 3},
		{0, schemaLockHeartbeatDefault},
	}
	for _, tc := range cases {
		got := schemaLockHeartbeatInterval(tc.wait)
		if got != tc.want {
			t.Fatalf("wait_timeout=%d: got %v want %v", tc.wait, got, tc.want)
		}
		if tc.wait > 0 && got >= time.Duration(tc.wait)*time.Second {
			t.Fatalf("wait_timeout=%d: interval %v must be < wait_timeout", tc.wait, got)
		}
	}
}

func TestPrepareSchemaLockSessionRaisesLowWaitTimeout(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT @@SESSION.wait_timeout")).
		WillReturnRows(sqlmock.NewRows([]string{"wait_timeout"}).AddRow(int64(10)))
	mock.ExpectExec(regexp.QuoteMeta("SET SESSION wait_timeout = ?")).
		WithArgs(schemaLockMinWaitTimeoutSec).
		WillReturnResult(sqlmock.NewResult(0, 0))

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	gotWait, gotHB, err := prepareSchemaLockSession(context.Background(), conn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if gotWait != schemaLockMinWaitTimeoutSec {
		t.Fatalf("wait_timeout: got %d want %d", gotWait, schemaLockMinWaitTimeoutSec)
	}
	if gotHB != schemaLockHeartbeatInterval(schemaLockMinWaitTimeoutSec) {
		t.Fatalf("heartbeat: got %v", gotHB)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStartHeartbeatCallsOnLostAndUsesConfiguredInterval(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	name := txMarkerSchemaLockName("db")
	locks := &SchemaLocks{
		conn:           conn,
		names:          []string{name},
		heartbeatEvery: 20 * time.Millisecond,
	}

	lost := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT CONNECTION_ID()")).
		WillReturnError(errors.New("connection closed"))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT RELEASE_LOCK(?)")).
		WithArgs(name).
		WillReturnRows(sqlmock.NewRows([]string{"RELEASE_LOCK(?)"}).AddRow(1))

	locks.StartHeartbeat(ctx, func() { lost <- struct{}{} })

	select {
	case <-lost:
	case <-time.After(2 * time.Second):
		t.Fatal("expected onLost after heartbeat failure")
	}
	if err := locks.Release(context.Background()); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaLocksReleaseTimesOutAndClosesConn(t *testing.T) {
	old := schemaLockReleaseTimeout
	schemaLockReleaseTimeout = 80 * time.Millisecond
	defer func() { schemaLockReleaseTimeout = old }()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	name := txMarkerSchemaLockName("db")
	locks := &SchemaLocks{conn: conn, names: []string{name}}

	// RELEASE_LOCK 故意拖过超时；Release 应在超时后返回并仍关闭连接。
	mock.ExpectQuery(regexp.QuoteMeta("SELECT RELEASE_LOCK(?)")).
		WithArgs(name).
		WillDelayFor(2 * time.Second).
		WillReturnRows(sqlmock.NewRows([]string{"RELEASE_LOCK(?)"}).AddRow(1))

	start := time.Now()
	err = locks.Release(context.Background())
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected timeout error from RELEASE_LOCK")
	}
	// sqlmock 在 ctx 超时时返回 ErrCancelled；真实驱动通常为 context.DeadlineExceeded。
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, sqlmock.ErrCancelled) {
		t.Fatalf("expected DeadlineExceeded or sqlmock cancel, got %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Release took too long (%v); expected independent release timeout", elapsed)
	}
	if locks.conn != nil {
		t.Fatal("expected lock conn cleared after Release")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaLocksReleaseIgnoresCanceledParent(t *testing.T) {
	old := schemaLockReleaseTimeout
	schemaLockReleaseTimeout = 200 * time.Millisecond
	defer func() { schemaLockReleaseTimeout = old }()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	name := txMarkerSchemaLockName("db")
	locks := &SchemaLocks{conn: conn, names: []string{name}}

	parent, cancel := context.WithCancel(context.Background())
	cancel() // 已取消：WithoutCancel 前会让 RELEASE_LOCK 立即失败

	mock.ExpectQuery(regexp.QuoteMeta("SELECT RELEASE_LOCK(?)")).
		WithArgs(name).
		WillDelayFor(30 * time.Millisecond).
		WillReturnRows(sqlmock.NewRows([]string{"RELEASE_LOCK(?)"}).AddRow(1))

	if err := locks.Release(parent); err != nil {
		t.Fatalf("Release should get independent timeout window despite canceled parent: %v", err)
	}
	if locks.conn != nil {
		t.Fatal("expected lock conn cleared after Release")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaLocksReleaseCancelsInFlightHeartbeat(t *testing.T) {
	old := schemaLockReleaseTimeout
	schemaLockReleaseTimeout = 100 * time.Millisecond
	defer func() { schemaLockReleaseTimeout = old }()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	name := txMarkerSchemaLockName("db")
	locks := &SchemaLocks{
		conn:           conn,
		names:          []string{name},
		heartbeatEvery: 20 * time.Millisecond,
	}

	// 心跳探测故意拖很久；StopHeartbeat 应主动取消，使 Release 不必等满探测超时。
	mock.ExpectQuery(regexp.QuoteMeta("SELECT CONNECTION_ID()")).
		WillDelayFor(3 * time.Second).
		WillReturnRows(sqlmock.NewRows([]string{"CONNECTION_ID()"}).AddRow(int64(42)))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT RELEASE_LOCK(?)")).
		WithArgs(name).
		WillReturnRows(sqlmock.NewRows([]string{"RELEASE_LOCK(?)"}).AddRow(1))

	ctx := context.Background()
	locks.StartHeartbeat(ctx, func() {
		t.Error("onLost must not fire when StopHeartbeat cancels in-flight probe")
	})

	// 等到心跳进入延迟中的 CONNECTION_ID 查询。
	deadline := time.Now().Add(2 * time.Second)
	for {
		locks.hbMu.Lock()
		hasCancel := locks.hbCancel != nil
		locks.hbMu.Unlock()
		if hasCancel {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("heartbeat probe did not start in time")
		}
		time.Sleep(5 * time.Millisecond)
	}

	start := time.Now()
	if err := locks.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	elapsed := time.Since(start)
	// 若未取消进行中心跳，会先等 ~3s 再 RELEASE，远超 500ms。
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Release took %v; expected StopHeartbeat to cancel in-flight ownership probe", elapsed)
	}
	if locks.conn != nil {
		t.Fatal("expected lock conn cleared after Release")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaLockLostError(t *testing.T) {
	if SchemaLockLostError(context.Background()) != nil {
		t.Fatal("background should be nil")
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(ErrSchemaLockLost)
	if !errors.Is(SchemaLockLostError(ctx), ErrSchemaLockLost) {
		t.Fatalf("expected ErrSchemaLockLost, got %v", SchemaLockLostError(ctx))
	}
}

func TestTxMarkerIDTypeOK(t *testing.T) {
	if !txMarkerIDTypeOK("char", sql.NullInt64{Int64: 36, Valid: true}) {
		t.Fatal("char(36)")
	}
	if txMarkerIDTypeOK("varchar", sql.NullInt64{Int64: 35, Valid: true}) {
		t.Fatal("varchar(35) should fail")
	}
	if !txMarkerIDTypeOK("text", sql.NullInt64{}) {
		t.Fatal("text")
	}
	if txMarkerIDTypeOK("int", sql.NullInt64{}) {
		t.Fatal("int should fail")
	}
}

func TestTxMarkerUniquePrefixOK(t *testing.T) {
	if !txMarkerUniquePrefixOK(sql.NullInt64{}) {
		t.Fatal("NULL SUB_PART (full column) should pass")
	}
	if !txMarkerUniquePrefixOK(sql.NullInt64{Int64: 36, Valid: true}) {
		t.Fatal("SUB_PART=36 should pass")
	}
	if txMarkerUniquePrefixOK(sql.NullInt64{Int64: 1, Valid: true}) {
		t.Fatal("SUB_PART=1 should fail")
	}
}
