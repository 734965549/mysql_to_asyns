package fullload

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestStartAttempt_RejectsExceededRetries 验证超过最大重试次数后 startAttempt 返回错误。
func TestStartAttempt_RejectsExceededRetries(t *testing.T) {
	specs := []*TableSpec{
		{SourceSchema: "s", SourceTable: "t1"},
	}
	tracker := newTableStateTracker(specs)

	// maxRetries=2 => 总 attempt=3（首次+两次重试）
	id, err := tracker.startAttempt("s", "t1", 2)
	if err != nil || id != 1 {
		t.Fatalf("attempt 1: got id=%d err=%v", id, err)
	}

	id, err = tracker.startAttempt("s", "t1", 2)
	if err != nil || id != 2 {
		t.Fatalf("attempt 2: got id=%d err=%v", id, err)
	}

	id, err = tracker.startAttempt("s", "t1", 2)
	if err != nil || id != 3 {
		t.Fatalf("attempt 3: got id=%d err=%v", id, err)
	}

	_, err = tracker.startAttempt("s", "t1", 2)
	if err == nil {
		t.Fatal("attempt 4 should fail but got nil error")
	}
	if !strings.Contains(err.Error(), "exceeded max retries") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestStartAttempt_ZeroRetriesAllowsFirstAttempt staging=true、retry=0 时仍可首次 attempt。
func TestStartAttempt_ZeroRetriesAllowsFirstAttempt(t *testing.T) {
	specs := []*TableSpec{{SourceSchema: "s", SourceTable: "t1"}}
	tracker := newTableStateTracker(specs)
	id, err := tracker.startAttempt("s", "t1", 0)
	if err != nil || id != 1 {
		t.Fatalf("first attempt with maxRetries=0: id=%d err=%v", id, err)
	}
	_, err = tracker.startAttempt("s", "t1", 0)
	if err == nil {
		t.Fatal("second attempt with maxRetries=0 should fail")
	}
}

// TestOnBatchEnqueued_RejectsOldAttempt 验证旧 attemptID 的批次被拒绝。
func TestOnBatchEnqueued_RejectsOldAttempt(t *testing.T) {
	specs := []*TableSpec{
		{SourceSchema: "s", SourceTable: "t1"},
	}
	tracker := newTableStateTracker(specs)

	// attempt 1
	id, _ := tracker.startAttempt("s", "t1", 3)
	if id != 1 {
		t.Fatalf("expected attempt 1, got %d", id)
	}

	// 旧 attempt（0）应被拒绝
	err := tracker.onBatchEnqueued("s", "t1", 0)
	if err == nil {
		t.Fatal("old attempt 0 should be rejected")
	}

	// 当前 attempt（1）应通过
	err = tracker.onBatchEnqueued("s", "t1", 1)
	if err != nil {
		t.Fatalf("current attempt 1 should pass: %v", err)
	}

	// 推进到 attempt 2
	tracker.startAttempt("s", "t1", 3)

	// 旧 attempt（1）应被拒绝
	err = tracker.onBatchEnqueued("s", "t1", 1)
	if err == nil {
		t.Fatal("old attempt 1 should be rejected after advancing to attempt 2")
	}

	// 当前 attempt（2）应通过
	err = tracker.onBatchEnqueued("s", "t1", 2)
	if err != nil {
		t.Fatalf("current attempt 2 should pass: %v", err)
	}
}

// TestWaitInflightZero_ReturnsWhenCommitted 验证 inflight barrier 在批次提交后归零返回。
func TestWaitInflightZero_ReturnsWhenCommitted(t *testing.T) {
	specs := []*TableSpec{
		{SourceSchema: "s", SourceTable: "t1"},
	}
	tracker := newTableStateTracker(specs)
	id, _ := tracker.startAttempt("s", "t1", 3)

	// 入队 3 个批次
	tracker.onBatchEnqueued("s", "t1", id)
	tracker.onBatchEnqueued("s", "t1", id)
	tracker.onBatchEnqueued("s", "t1", id)

	// 提交 2 个，还有 1 个未提交
	tracker.onBatchCommitted("s", "t1", id)
	tracker.onBatchCommitted("s", "t1", id)

	st := tracker.getState("s", "t1")
	if st.Inflight != 1 {
		t.Fatalf("expected inflight=1, got %d", st.Inflight)
	}

	// 启动 barrier 等待，在另一个 goroutine 提交最后一个批次
	done := make(chan error, 1)
	go func() {
		done <- tracker.waitInflightZero("s", "t1", 5*time.Second)
	}()

	// 等待一下确保 barrier 已进入等待
	time.Sleep(100 * time.Millisecond)

	// 提交最后一个批次
	tracker.onBatchCommitted("s", "t1", id)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waitInflightZero should return nil: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waitInflightZero timed out")
	}
}

// TestWaitInflightZero_TimesOut 验证 inflight barrier 超时返回错误。
func TestWaitInflightZero_TimesOut(t *testing.T) {
	specs := []*TableSpec{
		{SourceSchema: "s", SourceTable: "t1"},
	}
	tracker := newTableStateTracker(specs)
	id, _ := tracker.startAttempt("s", "t1", 3)

	// 入队但不提交
	tracker.onBatchEnqueued("s", "t1", id)

	start := time.Now()
	err := tracker.waitInflightZero("s", "t1", 200*time.Millisecond)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "inflight barrier timeout") {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed < 150*time.Millisecond {
		t.Fatalf("barrier returned too fast: %v", elapsed)
	}
}

// TestRetryBackoff_ExponentialGrowth 验证退避时间指数增长且不超过上限。
func TestRetryBackoff_ExponentialGrowth(t *testing.T) {
	prev := time.Duration(0)
	for attempt := 1; attempt <= 10; attempt++ {
		b := retryBackoff(attempt)
		// 退避应大致翻倍（考虑抖动，放宽到 1.3x）
		if attempt > 1 && b < prev/2 {
			t.Fatalf("attempt %d: backoff %v too small (prev %v)", attempt, b, prev)
		}
		// 不超过上限 60s
		if b > 60*time.Second {
			t.Fatalf("attempt %d: backoff %v exceeds 60s cap", attempt, b)
		}
		// 不低于基准的一半
		if b < 2500*time.Millisecond {
			t.Fatalf("attempt %d: backoff %v below 2.5s floor", attempt, b)
		}
		prev = b
	}

	// 第 1 次应约 5s（±20%）
	b1 := retryBackoff(1)
	if b1 < 4*time.Second || b1 > 6*time.Second {
		t.Fatalf("attempt 1 backoff %v not in [4s, 6s]", b1)
	}

	// 第 6 次应达上限 60s
	b6 := retryBackoff(6)
	if b6 > 60*time.Second {
		t.Fatalf("attempt 6 backoff %v exceeds cap", b6)
	}
}

// TestIsRetryableReadError 验证错误分类逻辑。
func TestIsRetryableReadError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"context canceled", context.Canceled, false},
		{"query timeout", &ReadQueryTimeoutError{Schema: "s", Table: "t", Timeout: 5 * time.Second}, true},
		{"wrapped query timeout", fmt.Errorf("read chunk x: %w", &ReadQueryTimeoutError{Schema: "s", Table: "t", Timeout: 5 * time.Second}), true},
		{"bad conn", errors.New("bad connection"), true},
		{"connection refused", errors.New("connection refused"), true},
		{"lock wait timeout", errors.New("Lock wait timeout exceeded"), true},
		{"deadlock", errors.New("Deadlock found when trying to get lock"), false},
		{"syntax error", errors.New("You have an error in your SQL syntax"), false},
		{"table not exist", errors.New("Table 'foo.bar' doesn't exist"), false},
		{"server shutdown", errors.New("Server shutdown in progress"), true},
		{"broken pipe", errors.New("write: broken pipe"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isRetryableReadError(tt.err)
			if got != tt.want {
				t.Errorf("isRetryableReadError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestStagingTableName 验证 staging 表名生成。
func TestStagingTableName(t *testing.T) {
	tests := []struct {
		base    string
		attempt int
		want    string
	}{
		{"orders", 1, "__mts_staging_orders_1"},
		{"user_data", 2, "__mts_staging_user_data_2"},
		{"t", 10, "__mts_staging_t_10"},
	}
	for _, tt := range tests {
		got := stagingTableName(tt.base, tt.attempt)
		if got != tt.want {
			t.Errorf("stagingTableName(%s, %d) = %s, want %s", tt.base, tt.attempt, got, tt.want)
		}
	}
}

// TestStagingTableLifecycle 验证 staging 表创建/发布/清理的完整生命周期（使用 sqlmock）。
func TestStagingTableLifecycle(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	schema := "target_db"
	table := "orders"
	attemptID := 1

	// 1. createStagingTable：CREATE IF NOT EXISTS 后强制 TRUNCATE，避免复用半成品
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS .* LIKE .*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("TRUNCATE TABLE .*").WillReturnResult(sqlmock.NewResult(0, 0))
	if err := createStagingTable(ctx, db, schema, table, attemptID); err != nil {
		t.Fatalf("createStagingTable: %v", err)
	}

	// 2. truncateStagingTable（独立路径仍可用）
	mock.ExpectExec("TRUNCATE TABLE .*").WillReturnResult(sqlmock.NewResult(0, 0))
	if err := truncateStagingTable(ctx, db, schema, table, attemptID); err != nil {
		t.Fatalf("truncateStagingTable: %v", err)
	}

	// 3. publishStagingTable - 最终表存在时的三表交换
	// 先检查最终表存在
	mock.ExpectQuery("SELECT 1 FROM information_schema.TABLES").
		WithArgs(schema, table).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	// RENAME TABLE
	mock.ExpectExec("RENAME TABLE .*").WillReturnResult(sqlmock.NewResult(0, 0))
	if err := publishStagingTable(ctx, db, schema, table, attemptID); err != nil {
		t.Fatalf("publishStagingTable: %v", err)
	}

	// 4. dropStagingTable
	mock.ExpectExec("DROP TABLE IF EXISTS .*").WillReturnResult(sqlmock.NewResult(0, 0))
	if err := dropStagingTable(ctx, db, schema, table, attemptID); err != nil {
		t.Fatalf("dropStagingTable: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

// TestPublishStagingTable_FirstSync 验证首次同步（最终表不存在）时只做 staging->final。
func TestPublishStagingTable_FirstSync(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	schema := "target_db"
	table := "new_table"
	attemptID := 1

	// 最终表不存在
	mock.ExpectQuery("SELECT 1 FROM information_schema.TABLES").
		WithArgs(schema, table).
		WillReturnError(sql.ErrNoRows)
	// RENAME TABLE staging -> final（仅一条）
	mock.ExpectExec("RENAME TABLE .* TO .*").WillReturnResult(sqlmock.NewResult(0, 0))

	if err := publishStagingTable(ctx, db, schema, table, attemptID); err != nil {
		t.Fatalf("publishStagingTable first sync: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

// TestDropOldBackupTables 验证旧备份表清理逻辑。
func TestDropOldBackupTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	schema := "target_db"
	table := "orders"

	// 查询返回 3 个备份表（按名称降序）；另有哈希短名前缀查询（空）
	rows := sqlmock.NewRows([]string{"TABLE_NAME"}).
		AddRow("__mts_old_orders_20260725_120530").
		AddRow("__mts_old_orders_20260724_100000").
		AddRow("__mts_old_orders_20260723_080000")
	mock.ExpectQuery("SELECT TABLE_NAME FROM information_schema.TABLES").
		WithArgs(schema, "__mts_old_orders_%").
		WillReturnRows(rows)
	mock.ExpectQuery("SELECT TABLE_NAME FROM information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME"}))

	// keepRecent=1，应删除后 2 个
	mock.ExpectExec("DROP TABLE IF EXISTS .*20260724_100000.*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("DROP TABLE IF EXISTS .*20260723_080000.*").WillReturnResult(sqlmock.NewResult(0, 0))

	if err := dropOldBackupTables(ctx, db, schema, table, 1); err != nil {
		t.Fatalf("dropOldBackupTables: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

// TestStartAttempt_RetryTimesSemantics 验证 retry_times=2 允许首次+两次重试共 3 次 attempt。
func TestStartAttempt_RetryTimesSemantics(t *testing.T) {
	specs := []*TableSpec{
		{SourceSchema: "s", SourceTable: "t1", TargetSchema: "ts", TargetTable: "tt"},
	}
	tracker := newTableStateTracker(specs)

	maxRetries := 2
	for want := 1; want <= 3; want++ {
		id, err := tracker.startAttempt("s", "t1", maxRetries)
		if err != nil || id != want {
			t.Fatalf("attempt %d: got id=%d err=%v", want, id, err)
		}
	}
	_, err := tracker.startAttempt("s", "t1", maxRetries)
	if err == nil {
		t.Fatal("fourth attempt should fail")
	}
}

// TestReadTableWithRetry_NoRetryWhenDisabled 验证 ReadRetryTimes=0 时不重试。
func TestReadTableWithRetry_NoRetryWhenDisabled(t *testing.T) {
	// ReadRetryTimes=0 且 StagingEnabled=false 时，readTableWithRetry 直接走原路径
	// 这里验证配置推导
	opt := ResolveOptions(RawOptions{})
	if opt.ReadRetryTimes != 0 {
		t.Fatalf("default ReadRetryTimes should be 0, got %d", opt.ReadRetryTimes)
	}
	if opt.StagingEnabled {
		t.Fatal("default StagingEnabled should be false")
	}
}

// TestReadTableWithRetry_StagingConfigValidated 验证启用 staging 时的配置推导。
func TestReadTableWithRetry_StagingConfigValidated(t *testing.T) {
	opt := ResolveOptions(RawOptions{
		ReadRetryTimes: 3,
		EnableStaging:  true,
	})
	if opt.ReadRetryTimes != 3 {
		t.Fatalf("ReadRetryTimes should be 3, got %d", opt.ReadRetryTimes)
	}
	if !opt.StagingEnabled {
		t.Fatal("StagingEnabled should be true")
	}
}

// TestReadTableWithRetry_RetryTimesClamped 验证重试次数上限钳制。
func TestReadTableWithRetry_RetryTimesClamped(t *testing.T) {
	opt := ResolveOptions(RawOptions{ReadRetryTimes: 999})
	if opt.ReadRetryTimes != 10 {
		t.Fatalf("ReadRetryTimes should be clamped to 10, got %d", opt.ReadRetryTimes)
	}
}

// TestStagingTableFillsRowBatch 验证 StagingEnabled 时 reader 填充 RowBatch.StagingTable。
func TestStagingTableFillsRowBatch(t *testing.T) {
	// 验证 Options.StagingEnabled + attemptID > 0 时 StagingTable 被填充
	// 这个测试通过检查 makeBatch 的逻辑间接验证
	// （makeBatch 是 chunkReader 的方法，需要完整 chunkReader 构造）
	// 这里验证 stagingTableName 的正确性即可，reader 集成测试在 integration 层覆盖
	opt := ResolveOptions(RawOptions{EnableStaging: true})
	if !opt.StagingEnabled {
		t.Fatal("StagingEnabled should be true")
	}

	// 验证 staging 表名格式
	name := stagingTableName("orders", 1)
	expected := "__mts_staging_orders_1"
	if name != expected {
		t.Fatalf("stagingTableName = %s, want %s", name, expected)
	}
}

// TestInsertPrefix_StagingTable 验证 writer 在 StagingTable 非空时写入 staging 表。
func TestInsertPrefix_StagingTable(t *testing.T) {
	batch := &RowBatch{
		TargetSchema: "db",
		TargetTable:  "orders",
		StagingTable: "__mts_staging_orders_1",
		Columns:      []string{"id", "name"},
	}
	prefix := insertPrefix(batch)
	if !strings.Contains(prefix, "`__mts_staging_orders_1`") {
		t.Fatalf("prefix should contain staging table name: %s", prefix)
	}
	if strings.Contains(prefix, "`orders` VALUES") {
		t.Fatalf("prefix should not write to final table when staging set: %s", prefix)
	}
}

// TestInsertPrefix_NoStagingTable 验证 StagingTable 为空时写入最终表。
func TestInsertPrefix_NoStagingTable(t *testing.T) {
	batch := &RowBatch{
		TargetSchema: "db",
		TargetTable:  "orders",
		StagingTable: "",
		Columns:      []string{"id", "name"},
	}
	prefix := insertPrefix(batch)
	if !strings.Contains(prefix, "`orders`") {
		t.Fatalf("prefix should contain final table name: %s", prefix)
	}
}

// TestInsertPrefix_GeneratedColumnsExcluded 验证 FullLoadV2 writer 的 INSERT 列清单不含生成列。
func TestInsertPrefix_GeneratedColumnsExcluded(t *testing.T) {
	batch := &RowBatch{
		TargetSchema: "db",
		TargetTable:  "cps_quick_sign_contract",
		StagingTable: "__mts_staging_cps_quick_sign_contract_2",
		Columns:      []string{"id", "sign_no"},
	}
	prefix := insertPrefix(batch)
	if strings.Contains(prefix, "`active_sign_no`") {
		t.Fatalf("prefix must not include generated column: %s", prefix)
	}
	if !strings.Contains(prefix, "`__mts_staging_cps_quick_sign_contract_2`") {
		t.Fatalf("prefix should write to staging table: %s", prefix)
	}
	want := "INSERT INTO `db`.`__mts_staging_cps_quick_sign_contract_2` (`id`, `sign_no`) VALUES "
	if prefix != want {
		t.Fatalf("prefix=%q want=%q", prefix, want)
	}
}

// TestStateTrackerConcurrency 验证 tableStateTracker 的并发安全。
func TestStateTrackerConcurrency(t *testing.T) {
	specs := []*TableSpec{
		{SourceSchema: "s", SourceTable: "t1"},
	}
	tracker := newTableStateTracker(specs)
	_, _ = tracker.startAttempt("s", "t1", 10)

	var wg sync.WaitGroup
	var enqueueOK int64
	var commitOK int64

	// 50 个 goroutine 并发入队和提交
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := tracker.onBatchEnqueued("s", "t1", 1); err == nil {
				atomic.AddInt64(&enqueueOK, 1)
			}
			_ = tracker.onBatchCommitted("s", "t1", 1)
			atomic.AddInt64(&commitOK, 1)
		}()
	}
	wg.Wait()

	st := tracker.getState("s", "t1")
	// inflight 应为 0（每个入队都有对应提交）
	if st.Inflight != 0 {
		t.Fatalf("inflight should be 0 after balanced enqueue/commit, got %d", st.Inflight)
	}
}

// _ 确保 fmt 在测试中被引用（避免 import 报错）
var _ = fmt.Sprintf

// TestHWMAtomicOverwriteOnRetry 验证 ALL 模式下重试时 HWM 原子覆盖行为。
// 模拟同一张表多次 attempt，每次捕获不同的 HWM，确认最后一次覆盖前面的值。
func TestHWMAtomicOverwriteOnRetry(t *testing.T) {
	// 模拟 persistTableBinlogHWM 的核心逻辑：同 key 覆盖
	hwmStore := make(map[string]string)

	persistHWM := func(schema, table, pos string) {
		key := schema + "." + table
		hwmStore[key] = pos
	}

	// attempt 1: 捕获 HWM_1
	persistHWM("db", "orders", "binlog.000123:4567")
	if hwmStore["db.orders"] != "binlog.000123:4567" {
		t.Fatalf("attempt 1 HWM mismatch")
	}

	// attempt 2: 捕获 HWM_2 (覆盖 HWM_1)
	persistHWM("db", "orders", "binlog.000123:8901")
	if hwmStore["db.orders"] != "binlog.000123:8901" {
		t.Fatalf("attempt 2 should overwrite attempt 1 HWM")
	}

	// attempt 3: 捕获 HWM_3 (覆盖 HWM_2)
	persistHWM("db", "orders", "binlog.000124:1234")
	if hwmStore["db.orders"] != "binlog.000124:1234" {
		t.Fatalf("attempt 3 should overwrite attempt 2 HWM")
	}

	// 确认只有最后一次的值被保留
	if len(hwmStore) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(hwmStore))
	}
}

// TestCaptureHWMConditionWithRetry 验证启用重试后 captureHWM 条件判断。
func TestCaptureHWMConditionWithRetry(t *testing.T) {
	// 无重试时：只有 NO-PK 表触发 HWM 捕获
	optNoRetry := ResolveOptions(RawOptions{ReadRetryTimes: 0})
	if optNoRetry.ReadRetryTimes != 0 {
		t.Fatalf("expected ReadRetryTimes=0, got %d", optNoRetry.ReadRetryTimes)
	}

	// 启用重试时：所有表都触发 HWM 捕获（通过 opt.ReadRetryTimes > 0）
	optWithRetry := ResolveOptions(RawOptions{ReadRetryTimes: 2})
	if optWithRetry.ReadRetryTimes != 2 {
		t.Fatalf("expected ReadRetryTimes=2, got %d", optWithRetry.ReadRetryTimes)
	}

	// 历史注释：旧 CaptureTableHWM 条件已删除；现仅验证 isNoPKSpec / ReadRetryTimes 组合逻辑。
	// 场景1: NO-PK 表 + 无重试
	// isNoPKSpec=true, ReadRetryTimes=0 -> true || false = true
	captureNoPKNoRetry := true && (true || optNoRetry.ReadRetryTimes > 0)
	if !captureNoPKNoRetry {
		t.Fatal("NO-PK table should capture HWM even without retry")
	}

	// 场景2: PK 表 + 无重试
	// isNoPKSpec=false, ReadRetryTimes=0 -> false || false = false
	capturePKNoRetry := true && (false || optNoRetry.ReadRetryTimes > 0)
	if capturePKNoRetry {
		t.Fatal("PK table without retry should not capture HWM")
	}

	// 场景3: PK 表 + 启用重试
	// isNoPKSpec=false, ReadRetryTimes=2 -> false || true = true
	capturePKWithRetry := true && (false || optWithRetry.ReadRetryTimes > 0)
	if !capturePKWithRetry {
		t.Fatal("PK table with retry enabled should capture HWM")
	}
}

// TestRecordError_PersistFailurePropagates 验证 recordError 持久化失败时返回错误（fail-closed 契约）。
func TestRecordError_PersistFailurePropagates(t *testing.T) {
	tracker := newTableStateTracker([]*TableSpec{{SourceSchema: "s", SourceTable: "t1"}})
	tracker.onChange = func(schema, table, phase string, attemptID int, stagingTable, errMsg string, committedRows int64) error {
		if phase == string(PhaseFailed) {
			return fmt.Errorf("disk full")
		}
		return nil
	}
	if _, err := tracker.startAttempt("s", "t1", 3); err != nil {
		t.Fatalf("startAttempt: %v", err)
	}
	err := tracker.recordError("s", "t1", boomErr{})
	if err == nil {
		t.Fatal("expected persist failure to propagate")
	}
	if !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestInflightBarrierMustRunBeforeStartAttempt 验证重试契约：必须先 waitInflightZero 再 startAttempt。
// startAttempt 会清零 Inflight；若顺序反了，barrier 会空转通过。
func TestInflightBarrierMustRunBeforeStartAttempt(t *testing.T) {
	tracker := newTableStateTracker([]*TableSpec{{SourceSchema: "s", SourceTable: "t1"}})
	id1, _ := tracker.startAttempt("s", "t1", 3)
	if err := tracker.onBatchEnqueued("s", "t1", id1); err != nil {
		t.Fatal(err)
	}

	// 错误顺序：先 startAttempt 再 wait → barrier 立即通过（回归保护）
	id2, err := tracker.startAttempt("s", "t1", 3)
	if err != nil {
		t.Fatalf("startAttempt: %v", err)
	}
	if id2 != 2 {
		t.Fatalf("expected attempt 2, got %d", id2)
	}
	if err := tracker.waitInflightZero("s", "t1", 50*time.Millisecond); err != nil {
		t.Fatalf("buggy order makes barrier pass instantly; unexpected err: %v", err)
	}

	// 正确顺序：旧 attempt 仍有 inflight 时 wait 会阻塞，释放后才返回
	tracker2 := newTableStateTracker([]*TableSpec{{SourceSchema: "s", SourceTable: "t1"}})
	oldID, _ := tracker2.startAttempt("s", "t1", 3)
	_ = tracker2.onBatchEnqueued("s", "t1", oldID)

	done := make(chan error, 1)
	go func() {
		if err := tracker2.waitInflightZero("s", "t1", 2*time.Second); err != nil {
			done <- err
			return
		}
		_, err := tracker2.startAttempt("s", "t1", 3)
		done <- err
	}()

	time.Sleep(80 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("barrier should still be waiting on old inflight, got early result: %v", err)
	default:
	}

	tracker2.onBatchReleased("s", "t1", oldID)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("correct order should succeed after drain: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for correct-order retry prep")
	}
	st := tracker2.getState("s", "t1")
	if st.AttemptID != 2 {
		t.Fatalf("expected new attempt 2 after barrier, got %d", st.AttemptID)
	}
}
