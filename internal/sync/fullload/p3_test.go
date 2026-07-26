package fullload

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestCleanupStaleStagingTables_EmptyListNoop 空清单不做全库扫描删除。
func TestCleanupStaleStagingTables_EmptyListNoop(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	dropped, err := CleanupStaleStagingTables(context.Background(), db, nil)
	if err != nil {
		t.Fatalf("CleanupStaleStagingTables: %v", err)
	}
	if dropped != 0 {
		t.Fatalf("expected 0 dropped, got %d", dropped)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

// TestCleanupStaleStagingTables_NilDB 验证 nil db 返回错误。
func TestCleanupStaleStagingTables_NilDB(t *testing.T) {
	_, err := CleanupStaleStagingTables(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("expected error for nil db")
	}
}

// TestCleanupStaleStagingTables_ExactRefs 按精确表名清理。
func TestCleanupStaleStagingTables_ExactRefs(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	staging := "__mts_staging_users_2"

	mock.ExpectQuery("SELECT 1 FROM information_schema.TABLES").
		WithArgs("target_db", staging).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectExec("DROP TABLE IF EXISTS .*" + staging + ".*").WillReturnResult(sqlmock.NewResult(0, 0))

	dropped, err := CleanupStaleStagingTables(ctx, db, []StagingTableRef{
		{Schema: "target_db", Table: staging},
	})
	if err != nil {
		t.Fatalf("CleanupStaleStagingTables: %v", err)
	}
	if dropped != 1 {
		t.Fatalf("expected 1 dropped, got %d", dropped)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

// TestTableStateCallback 验证状态变更回调被触发。
func TestTableStateCallback(t *testing.T) {
	specs := []*TableSpec{
		{SourceSchema: "s", SourceTable: "t1"},
	}
	tracker := newTableStateTracker(specs)

	var callbackPhase string
	var callbackAttempt int
	var callbackErr string
	var callbackStaging string
	tracker.onChange = func(schema, table, phase string, attemptID int, stagingTable, errMsg string, committedRows int64) error {
		callbackPhase = phase
		callbackAttempt = attemptID
		callbackErr = errMsg
		callbackStaging = stagingTable
		return nil
	}

	id, err := tracker.startAttempt("s", "t1", 3)
	if err != nil {
		t.Fatalf("startAttempt: %v", err)
	}
	if id != 1 {
		t.Fatalf("expected attempt 1, got %d", id)
	}
	if callbackPhase != "SNAPSHOT_OPENING" {
		t.Fatalf("expected phase SNAPSHOT_OPENING, got %s", callbackPhase)
	}
	if callbackAttempt != 1 {
		t.Fatalf("expected attempt 1, got %d", callbackAttempt)
	}

	if err := tracker.setStagingTable("s", "t1", "__mts_staging_t1_1"); err != nil {
		t.Fatalf("setStagingTable: %v", err)
	}
	if callbackStaging != "__mts_staging_t1_1" {
		t.Fatalf("expected staging name, got %q", callbackStaging)
	}

	if err := tracker.transitionTo("s", "t1", PhaseCopying); err != nil {
		t.Fatalf("transitionTo: %v", err)
	}
	if callbackPhase != "COPYING" {
		t.Fatalf("expected COPYING, got %s", callbackPhase)
	}

	_ = tracker.recordError("s", "t1", boomErr{})
	if callbackPhase != "FAILED" {
		t.Fatalf("expected FAILED, got %s", callbackPhase)
	}
	if callbackErr == "" {
		t.Fatal("expected error message")
	}
}

type boomErr struct{}

func (boomErr) Error() string { return "boom" }

// TestStagingTableName_RespectsMySQLIdentifierLimit 长表名不超过 64。
func TestStagingTableName_RespectsMySQLIdentifierLimit(t *testing.T) {
	long := make([]byte, 80)
	for i := range long {
		long[i] = 'a'
	}
	name := stagingTableName(string(long), 1)
	if len(name) > 64 {
		t.Fatalf("staging name len=%d > 64: %s", len(name), name)
	}
}

// TestOptionsValidate_RetryRequiresStaging fail-closed 校验。
func TestOptionsValidate_RetryRequiresStaging(t *testing.T) {
	opt := ResolveOptions(RawOptions{ReadRetryTimes: 2, EnableStaging: false})
	if err := opt.Validate(); err == nil {
		t.Fatal("expected validation error when retry without staging")
	}
	opt2 := ResolveOptions(RawOptions{ReadRetryTimes: 2, EnableStaging: true})
	if err := opt2.Validate(); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}
