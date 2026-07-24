package fullload

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// 事务标记表：每个待提交写事务插入唯一 UUID。Commit 结果未知时，用该主键的
// 锁定当前读等待原事务结束，再判定已提交/已回滚，避免业务行存在性误判。
const txMarkerTableName = "__mts_fl_tx"

// txMarkerTableComment 写入 information_schema / SHOW CREATE TABLE，便于 DBA 识别系统表。
const txMarkerTableComment = "mysql-to-sync 全量V2写事务提交标记表；与业务INSERT同事务提交，用于Commit结果未知时的锁定探测，请勿手工删除或改名"

// txMarkerCleanupTimeout 数据流水线成功后按 run_id 删除本任务行的独立短超时，
// 避免清理被其他会话 MDL 拖死而卡在成功收尾。
const txMarkerCleanupTimeout = 5 * time.Second

// txMarkerSchemaLockTimeoutSec 获取 schema 级互斥锁的等待秒数；超时则 fail-closed。
const txMarkerSchemaLockTimeoutSec = 5

// txMarkerRunIDMaxLen 与建表 DDL 中 run_id VARCHAR 长度一致。
const txMarkerRunIDMaxLen = 64

func newTxMarkerID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	// UUID v4
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

func ensureTxMarkerTables(ctx context.Context, db *sql.DB, specs []*TableSpec) error {
	if db == nil {
		return fmt.Errorf("ensure tx marker tables: target db is nil")
	}
	if err := assertNoReservedTargetTableNames(specs); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		if spec == nil || spec.TargetSchema == "" {
			continue
		}
		if _, ok := seen[spec.TargetSchema]; ok {
			continue
		}
		seen[spec.TargetSchema] = struct{}{}
		ddl := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s ("+
				"`id` CHAR(36) NOT NULL COMMENT '写事务唯一标记 UUID，与业务数据同事务提交；Commit 结果未知时用锁定当前读探测',"+
				"`run_id` VARCHAR(%d) NOT NULL COMMENT '本趟全量流水线运行 ID，成功收尾时按此删除本任务行，禁止 DROP 共享表',"+
				"`created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '标记行写入时间（服务端时钟）',"+
				"PRIMARY KEY (`id`),"+
				"KEY `idx_run_id` (`run_id`)"+
				") ENGINE=InnoDB COMMENT='%s'",
			quoteIdentifier(spec.TargetSchema), quoteIdentifier(txMarkerTableName),
			txMarkerRunIDMaxLen, txMarkerTableComment,
		)
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("ensure tx marker table %s.%s: %w", spec.TargetSchema, txMarkerTableName, err)
		}
		// CREATE IF NOT EXISTS 不会校验/修复已有同名表；MyISAM 等同名表会导致
		// marker 在业务事务 Commit 前永久落库，回滚后探测仍误判 applied。
		if err := assertTxMarkerTable(ctx, db, spec.TargetSchema); err != nil {
			return err
		}
	}
	return nil
}

// cleanupTxMarkerRows 在数据流水线成功后按 run_id 删除本任务写入的 marker 行。
// 不 DROP 共享表，避免误删其他任务仍在使用的对象；使用独立短超时。
// 使用 context.WithoutCancel 确保即使父 ctx 已取消也能获得完整的清理窗口。
func cleanupTxMarkerRows(ctx context.Context, db *sql.DB, specs []*TableSpec, runID string) error {
	if db == nil {
		return fmt.Errorf("cleanup tx marker rows: target db is nil")
	}
	if runID == "" {
		return fmt.Errorf("cleanup tx marker rows: run_id is required")
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), txMarkerCleanupTimeout)
	defer cancel()

	seen := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		if spec == nil || spec.TargetSchema == "" {
			continue
		}
		if _, ok := seen[spec.TargetSchema]; ok {
			continue
		}
		seen[spec.TargetSchema] = struct{}{}
		q := fmt.Sprintf("DELETE FROM %s.%s WHERE `run_id` = ?",
			quoteIdentifier(spec.TargetSchema), quoteIdentifier(txMarkerTableName))
		if _, err := db.ExecContext(cctx, q, runID); err != nil {
			return fmt.Errorf("cleanup tx marker rows %s.%s run_id=%s: %w",
				spec.TargetSchema, txMarkerTableName, runID, err)
		}
	}
	return nil
}

// ErrSchemaLockLost 表示目标 schema 顾问锁心跳失败（连接断开或锁被夺走）。
// 调用层应以 WithCancelCause 取消派生 context，并在破坏性 DDL / MarkFullSyncCompleted 前检查该 cause。
var ErrSchemaLockLost = errors.New("target schema advisory lock lost (fail-closed)")

// SchemaLockLostError 若 ctx 因 ErrSchemaLockLost 取消则返回该错误，否则返回 nil。
func SchemaLockLostError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := context.Cause(ctx); errors.Is(err, ErrSchemaLockLost) {
		return err
	}
	return nil
}

// SchemaLocks 持有跨进程 schema 级互斥（MySQL GET_LOCK），连接级生命周期。
// 由调用层在首次目标端 DDL 前获取，持有到索引恢复及任务级收尾完成后释放。
// 要求 MySQL >= 5.7.5：同一连接可同时持有多个命名锁；更早版本第二次 GET_LOCK 会释放旧锁。
type SchemaLocks struct {
	conn           *sql.Conn
	names          []string
	waitTimeoutSec int64
	heartbeatEvery time.Duration

	// heartbeat bookkeeping
	stopHB   chan struct{}
	hbDone   chan struct{}
	hbMu     sync.Mutex
	hbCancel context.CancelFunc // 取消进行中的所有权查询，使 StopHeartbeat 不必等满 5s
}

func txMarkerSchemaLockName(schema string) string {
	const prefix = "mts_fl_v2:"
	name := prefix + schema
	if len(name) <= 64 {
		return name
	}
	sum := sha256.Sum256([]byte(schema))
	// 64 字节上限：prefix(10) + 54 hex chars。
	return fmt.Sprintf("%s%x", prefix, sum[:27])
}

// AcquireSchemaLocks 对给定目标 schema 集合获取跨进程互斥锁（MySQL GET_LOCK）。
// 调用层应在首次目标端 DDL 前调用，并持有到索引恢复及任务级收尾完成后释放。
// schemas 应已去重并按确定性顺序排序，避免死锁。
// 在取连接前 fail-fast 校验目标池 MaxOpenConnections >= 2（锁连接占用 1 槽）。
func AcquireSchemaLocks(ctx context.Context, db *sql.DB, schemas []string) (*SchemaLocks, error) {
	if db == nil {
		return nil, fmt.Errorf("acquire schema locks: target db is nil")
	}
	if len(schemas) == 0 {
		return &SchemaLocks{}, nil
	}
	if maxOpen := db.Stats().MaxOpenConnections; maxOpen > 0 && maxOpen < 2 {
		return nil, fmt.Errorf("acquire schema locks: target_max_open_conns >= 2 required (current=%d); lock connection reserves 1 slot", maxOpen)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire schema locks: get conn: %w", err)
	}
	waitTimeoutSec, hbEvery, err := prepareSchemaLockSession(ctx, conn)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	locks := &SchemaLocks{conn: conn, waitTimeoutSec: waitTimeoutSec, heartbeatEvery: hbEvery}
	for _, schema := range schemas {
		name := txMarkerSchemaLockName(schema)
		var got sql.NullInt64
		if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", name, txMarkerSchemaLockTimeoutSec).Scan(&got); err != nil {
			_ = locks.Release(context.Background())
			return nil, fmt.Errorf("GET_LOCK(%s) for target schema %q: %w", name, schema, err)
		}
		if !got.Valid || got.Int64 != 1 {
			_ = locks.Release(context.Background())
			return nil, fmt.Errorf("target schema %q is locked by another full-load V2 task (GET_LOCK %s timed out or denied); same schema must not run concurrent V2 (fail-closed)",
				schema, name)
		}
		locks.names = append(locks.names, name)
	}
	return locks, nil
}

// schemaLockMinWaitTimeoutSec 锁会话最低 wait_timeout：心跳间隔必须远小于此值，
// 避免 MySQL 先关闭空闲会话并隐式释放 GET_LOCK。
const schemaLockMinWaitTimeoutSec int64 = 60

// schemaLockReleaseTimeout 锁释放独立超时：网络半开时 RELEASE_LOCK 可能长期阻塞收尾；
// 超时后关闭专用连接，由会话终止隐式释放剩余锁。可在测试中覆盖。
var schemaLockReleaseTimeout = 5 * time.Second

// schemaLockHeartbeatDefault / Min 默认与下限心跳间隔。
const (
	schemaLockHeartbeatDefault = 15 * time.Second
	schemaLockHeartbeatMin     = time.Second
)

// prepareSchemaLockSession 读取并必要时抬高锁连接 wait_timeout，返回有效超时与心跳间隔。
func prepareSchemaLockSession(ctx context.Context, conn *sql.Conn) (waitTimeoutSec int64, heartbeatEvery time.Duration, err error) {
	if err := conn.QueryRowContext(ctx, "SELECT @@SESSION.wait_timeout").Scan(&waitTimeoutSec); err != nil {
		return 0, 0, fmt.Errorf("acquire schema locks: read session wait_timeout: %w", err)
	}
	if waitTimeoutSec < schemaLockMinWaitTimeoutSec {
		if _, err := conn.ExecContext(ctx, "SET SESSION wait_timeout = ?", schemaLockMinWaitTimeoutSec); err != nil {
			return 0, 0, fmt.Errorf("acquire schema locks: raise session wait_timeout from %d to %d: %w",
				waitTimeoutSec, schemaLockMinWaitTimeoutSec, err)
		}
		waitTimeoutSec = schemaLockMinWaitTimeoutSec
	}
	return waitTimeoutSec, schemaLockHeartbeatInterval(waitTimeoutSec), nil
}

// schemaLockHeartbeatInterval 根据 session wait_timeout 计算心跳间隔：默认 15s，
// 但必须严格小于 wait_timeout（取 wait_timeout/3），避免空闲断连先于检测发生。
func schemaLockHeartbeatInterval(waitTimeoutSec int64) time.Duration {
	if waitTimeoutSec <= 0 {
		return schemaLockHeartbeatDefault
	}
	d := time.Duration(waitTimeoutSec) * time.Second / 3
	if d > schemaLockHeartbeatDefault {
		d = schemaLockHeartbeatDefault
	}
	if d < schemaLockHeartbeatMin {
		d = schemaLockHeartbeatMin
	}
	wt := time.Duration(waitTimeoutSec) * time.Second
	if d >= wt {
		d = wt / 2
		if d < schemaLockHeartbeatMin {
			d = schemaLockHeartbeatMin
		}
	}
	return d
}

// acquireTxMarkerSchemaLocks 内部版本，从 specs 中提取 schema 并获取锁。
func acquireTxMarkerSchemaLocks(ctx context.Context, db *sql.DB, specs []*TableSpec) (*SchemaLocks, error) {
	schemas := extractTargetSchemas(specs)
	return AcquireSchemaLocks(ctx, db, schemas)
}

// extractTargetSchemas 从 specs 中提取去重的目标 schema 列表。
func extractTargetSchemas(specs []*TableSpec) []string {
	schemas := make([]string, 0)
	seen := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		if spec == nil || spec.TargetSchema == "" {
			continue
		}
		if _, ok := seen[spec.TargetSchema]; ok {
			continue
		}
		seen[spec.TargetSchema] = struct{}{}
		schemas = append(schemas, spec.TargetSchema)
	}
	return schemas
}

// Release 释放全部 GET_LOCK 并归还连接；可重复调用。
// 先 StopHeartbeat（会取消进行中的所有权查询），再使用独立有限超时（默认 5s，
// 基于 WithoutCancel，不继承调用方取消）执行 RELEASE_LOCK，避免网络半开或父 ctx
// 已取消时阻塞/跳过收尾；超时后仍关闭连接，由 MySQL 随会话终止释放剩余锁。
func (l *SchemaLocks) Release(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return nil
	}
	l.StopHeartbeat()
	conn := l.conn
	names := l.names
	l.conn = nil
	l.names = nil

	if ctx == nil {
		ctx = context.Background()
	}
	timeout := schemaLockReleaseTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()

	var firstErr error
	for i := len(names) - 1; i >= 0; i-- {
		var unused sql.NullInt64
		if err := conn.QueryRowContext(releaseCtx, "SELECT RELEASE_LOCK(?)", names[i]).Scan(&unused); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("RELEASE_LOCK(%s): %w", names[i], err)
			if releaseCtx.Err() != nil {
				break
			}
		}
	}
	// 无论 RELEASE_LOCK 是否完成/超时，都关闭连接以释放会话级锁。
	if err := conn.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// StartHeartbeat 启动定期所有权校验。若连接断开或锁被其他会话夺走，
// 立即调用 onLost 以便调用层以 ErrSchemaLockLost 取消派生 context 并 fail-closed。
// 心跳间隔取自锁会话 wait_timeout（见 schemaLockHeartbeatInterval），可重复调用（后续为空操作）。
func (l *SchemaLocks) StartHeartbeat(ctx context.Context, onLost func()) {
	if l == nil || l.conn == nil || len(l.names) == 0 {
		return
	}
	if l.stopHB != nil {
		return
	}
	l.stopHB = make(chan struct{})
	l.hbDone = make(chan struct{})
	interval := l.heartbeatEvery
	if interval <= 0 {
		interval = schemaLockHeartbeatInterval(l.waitTimeoutSec)
	}

	go func() {
		defer close(l.hbDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-l.stopHB:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				// 心跳查询与任务 ctx 解耦（WithoutCancel），但可被 StopHeartbeat 主动取消，
				// 避免 Release 等待满探测超时再叠加 RELEASE 超时。
				probeCtx, probeCancel := context.WithCancel(context.WithoutCancel(ctx))
				l.hbMu.Lock()
				l.hbCancel = probeCancel
				l.hbMu.Unlock()

				err := l.verifyOwnership(probeCtx)

				l.hbMu.Lock()
				l.hbCancel = nil
				l.hbMu.Unlock()
				probeCancel()

				if err != nil {
					// StopHeartbeat 取消进行中的探测时不视为锁丢失。
					select {
					case <-l.stopHB:
						return
					default:
					}
					if onLost != nil {
						onLost()
					}
					return
				}
			}
		}
	}()
}

// StopHeartbeat 停止心跳 goroutine；可重复调用。
// 会取消进行中的所有权查询，使等待尽快结束（不必等满 verifyOwnership 的 5s）。
func (l *SchemaLocks) StopHeartbeat() {
	if l == nil || l.stopHB == nil {
		return
	}
	select {
	case <-l.stopHB:
	default:
		close(l.stopHB)
	}
	l.hbMu.Lock()
	if l.hbCancel != nil {
		l.hbCancel()
	}
	l.hbMu.Unlock()
	if l.hbDone != nil {
		<-l.hbDone
	}
	l.stopHB = nil
	l.hbDone = nil
}

// verifyOwnership 在锁连接上检查所有已持有锁仍属于当前 CONNECTION_ID()。
func (l *SchemaLocks) verifyOwnership(ctx context.Context) error {
	if l.conn == nil {
		return fmt.Errorf("schema lock conn is nil")
	}
	hbCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var connID int64
	if err := l.conn.QueryRowContext(hbCtx, "SELECT CONNECTION_ID()").Scan(&connID); err != nil {
		return fmt.Errorf("schema lock heartbeat: cannot get CONNECTION_ID: %w", err)
	}
	for _, name := range l.names {
		var holder sql.NullInt64
		if err := l.conn.QueryRowContext(hbCtx, "SELECT IS_USED_LOCK(?)", name).Scan(&holder); err != nil {
			return fmt.Errorf("schema lock heartbeat: IS_USED_LOCK(%s) failed: %w", name, err)
		}
		if !holder.Valid || holder.Int64 != connID {
			return fmt.Errorf("schema lock heartbeat: lock %s no longer owned by connection %d (holder=%v)", name, connID, holder)
		}
	}
	return nil
}

// assertNoReservedTargetTableNames 拒绝业务目标表占用事务标记保留名（大小写不敏感，
// 兼容 lower_case_table_names）。
func assertNoReservedTargetTableNames(specs []*TableSpec) error {
	for _, spec := range specs {
		if spec == nil {
			continue
		}
		if strings.EqualFold(spec.TargetTable, txMarkerTableName) {
			return fmt.Errorf("target table %s.%s conflicts with reserved full-load tx marker table name %q (fail-closed)",
				spec.TargetSchema, spec.TargetTable, txMarkerTableName)
		}
	}
	return nil
}

// assertTxMarkerTable fail-closed 校验已存在的 marker 表结构满足事务探测前提。
func assertTxMarkerTable(ctx context.Context, db *sql.DB, schema string) error {
	var tableType, engine sql.NullString
	tableQ := "SELECT TABLE_TYPE, ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	if err := db.QueryRowContext(ctx, tableQ, schema, txMarkerTableName).Scan(&tableType, &engine); err != nil {
		return fmt.Errorf("check tx marker table %s.%s: %w", schema, txMarkerTableName, err)
	}
	if !tableType.Valid || !strings.EqualFold(strings.TrimSpace(tableType.String), "BASE TABLE") {
		tt := ""
		if tableType.Valid {
			tt = tableType.String
		}
		return fmt.Errorf("tx marker %s.%s TABLE_TYPE %q is not BASE TABLE (fail-closed)", schema, txMarkerTableName, tt)
	}
	if !engine.Valid || !strings.EqualFold(strings.TrimSpace(engine.String), "InnoDB") {
		eng := ""
		if engine.Valid {
			eng = engine.String
		}
		return fmt.Errorf("tx marker %s.%s engine %q is not InnoDB: marker must be transactional or Commit-unknown recovery can silently drop data (fail-closed)",
			schema, txMarkerTableName, eng)
	}

	if err := assertTxMarkerIDColumn(ctx, db, schema); err != nil {
		return err
	}
	if err := assertTxMarkerRunIDColumn(ctx, db, schema); err != nil {
		return err
	}

	ok, err := txMarkerIDHasUniqueKey(ctx, db, schema)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("tx marker %s.%s.`id` requires PRIMARY KEY or single-column UNIQUE index covering full UUID (SUB_PART NULL or >= 36) (fail-closed)", schema, txMarkerTableName)
	}
	return nil
}

func assertTxMarkerIDColumn(ctx context.Context, db *sql.DB, schema string) error {
	var colName, isNullable, dataType sql.NullString
	var maxLen sql.NullInt64
	colQ := "SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?"
	if err := db.QueryRowContext(ctx, colQ, schema, txMarkerTableName, "id").Scan(&colName, &isNullable, &dataType, &maxLen); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("tx marker %s.%s missing required column `id` (fail-closed)", schema, txMarkerTableName)
		}
		return fmt.Errorf("check tx marker column %s.%s.id: %w", schema, txMarkerTableName, err)
	}
	if !colName.Valid || !strings.EqualFold(colName.String, "id") {
		return fmt.Errorf("tx marker %s.%s missing required column `id` (fail-closed)", schema, txMarkerTableName)
	}
	if !isNullable.Valid || !strings.EqualFold(isNullable.String, "NO") {
		return fmt.Errorf("tx marker %s.%s.`id` must be NOT NULL (fail-closed)", schema, txMarkerTableName)
	}
	if !dataType.Valid || !txMarkerIDTypeOK(dataType.String, maxLen) {
		dt := ""
		if dataType.Valid {
			dt = dataType.String
		}
		return fmt.Errorf("tx marker %s.%s.`id` type %q cannot store UUID CHAR(36) (fail-closed)", schema, txMarkerTableName, dt)
	}
	return nil
}

func assertTxMarkerRunIDColumn(ctx context.Context, db *sql.DB, schema string) error {
	var colName, isNullable, dataType sql.NullString
	var maxLen sql.NullInt64
	colQ := "SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?"
	if err := db.QueryRowContext(ctx, colQ, schema, txMarkerTableName, "run_id").Scan(&colName, &isNullable, &dataType, &maxLen); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("tx marker %s.%s missing required column `run_id` (drop the table to recreate, or ALTER ADD run_id VARCHAR(%d) NOT NULL) (fail-closed)",
				schema, txMarkerTableName, txMarkerRunIDMaxLen)
		}
		return fmt.Errorf("check tx marker column %s.%s.run_id: %w", schema, txMarkerTableName, err)
	}
	if !colName.Valid || !strings.EqualFold(colName.String, "run_id") {
		return fmt.Errorf("tx marker %s.%s missing required column `run_id` (fail-closed)", schema, txMarkerTableName)
	}
	if !isNullable.Valid || !strings.EqualFold(isNullable.String, "NO") {
		return fmt.Errorf("tx marker %s.%s.`run_id` must be NOT NULL (fail-closed)", schema, txMarkerTableName)
	}
	if !dataType.Valid || !txMarkerRunIDTypeOK(dataType.String, maxLen) {
		dt := ""
		if dataType.Valid {
			dt = dataType.String
		}
		return fmt.Errorf("tx marker %s.%s.`run_id` type %q cannot store run_id VARCHAR(%d) (fail-closed)",
			schema, txMarkerTableName, dt, txMarkerRunIDMaxLen)
	}
	return nil
}

func txMarkerIDTypeOK(dataType string, maxLen sql.NullInt64) bool {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "char", "varchar":
		return maxLen.Valid && maxLen.Int64 >= 36
	case "tinytext", "text", "mediumtext", "longtext":
		return true
	default:
		return false
	}
}

func txMarkerRunIDTypeOK(dataType string, maxLen sql.NullInt64) bool {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "char", "varchar":
		return maxLen.Valid && maxLen.Int64 >= txMarkerRunIDMaxLen
	case "tinytext", "text", "mediumtext", "longtext":
		return true
	default:
		return false
	}
}

// txMarkerIDHasUniqueKey 要求存在仅含 `id` 一列的 PRIMARY/UNIQUE 索引，且为完整列
// 或前缀长度至少 36（覆盖 UUID）；拒绝 UNIQUE(id(1)) 等短前缀索引。
func txMarkerIDHasUniqueKey(ctx context.Context, db *sql.DB, schema string) (bool, error) {
	idxQ := "SELECT INDEX_NAME, SUB_PART FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ? AND NON_UNIQUE = 0 AND SEQ_IN_INDEX = 1"
	rows, err := db.QueryContext(ctx, idxQ, schema, txMarkerTableName, "id")
	if err != nil {
		return false, fmt.Errorf("check tx marker unique key %s.%s: %w", schema, txMarkerTableName, err)
	}
	defer rows.Close()

	type candidate struct {
		name    string
		subPart sql.NullInt64
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.name, &c.subPart); err != nil {
			return false, fmt.Errorf("check tx marker unique key %s.%s: %w", schema, txMarkerTableName, err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("check tx marker unique key %s.%s: %w", schema, txMarkerTableName, err)
	}
	for _, c := range candidates {
		if !txMarkerUniquePrefixOK(c.subPart) {
			continue
		}
		var n int
		cntQ := "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ?"
		if err := db.QueryRowContext(ctx, cntQ, schema, txMarkerTableName, c.name).Scan(&n); err != nil {
			return false, fmt.Errorf("check tx marker unique key width %s.%s.%s: %w", schema, txMarkerTableName, c.name, err)
		}
		if n == 1 {
			return true, nil
		}
	}
	return false, nil
}

// txMarkerUniquePrefixOK：SUB_PART NULL 表示完整列索引；否则前缀须覆盖 UUID 的 36 字符。
func txMarkerUniquePrefixOK(subPart sql.NullInt64) bool {
	if !subPart.Valid {
		return true
	}
	return subPart.Int64 >= 36
}

func insertTxMarker(ctx context.Context, tx *sql.Tx, schema, markerID, runID string) error {
	if tx == nil {
		return fmt.Errorf("insert tx marker: tx is nil")
	}
	if schema == "" || markerID == "" || runID == "" {
		return fmt.Errorf("insert tx marker: schema, marker id and run_id are required")
	}
	if len(runID) > txMarkerRunIDMaxLen {
		return fmt.Errorf("insert tx marker: run_id length %d exceeds %d", len(runID), txMarkerRunIDMaxLen)
	}
	q := fmt.Sprintf("INSERT INTO %s.%s (`id`, `run_id`) VALUES (?, ?)",
		quoteIdentifier(schema), quoteIdentifier(txMarkerTableName))
	if _, err := tx.ExecContext(ctx, q, markerID, runID); err != nil {
		return fmt.Errorf("insert tx marker %s into %s.%s: %w", markerID, schema, txMarkerTableName, err)
	}
	return nil
}

// txMarkerApplied 在新连接上用显式事务 + SELECT ... FOR UPDATE 判定原写事务是否已提交。
// 若原 COMMIT 仍在服务端处理，锁定当前读会等待其结束，避免普通一致性读的“无行”竞态。
func txMarkerApplied(ctx context.Context, conn *sql.Conn, schema, markerID string) (bool, error) {
	if conn == nil {
		return false, fmt.Errorf("verify tx marker: conn is nil")
	}
	if schema == "" || markerID == "" {
		return false, fmt.Errorf("verify tx marker: schema and marker id are required")
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("verify tx marker begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	q := fmt.Sprintf("SELECT 1 FROM %s.%s WHERE `id` = ? FOR UPDATE",
		quoteIdentifier(schema), quoteIdentifier(txMarkerTableName))
	var one int
	scanErr := tx.QueryRowContext(ctx, q, markerID).Scan(&one)
	if errors.Is(scanErr, sql.ErrNoRows) {
		return false, nil
	}
	if scanErr != nil {
		return false, fmt.Errorf("verify tx marker: %w", scanErr)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("verify tx marker commit: %w", err)
	}
	return true, nil
}

// assertTargetTablesInnoDB 在启动 writers 前 fail-closed 校验所有目标表为 InnoDB。
// 事务原子性与 marker 探测均依赖目标端事务引擎。
func assertTargetTablesInnoDB(ctx context.Context, db *sql.DB, specs []*TableSpec) error {
	if db == nil {
		return fmt.Errorf("assert target InnoDB: target db is nil")
	}
	seen := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		if spec == nil {
			continue
		}
		key := spec.TargetSchema + "\x00" + spec.TargetTable
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if err := assertTargetInnoDBTable(ctx, db, spec.TargetSchema, spec.TargetTable); err != nil {
			return err
		}
	}
	return nil
}

func assertTargetInnoDBTable(ctx context.Context, q rowQueryer, schema, table string) error {
	var engine sql.NullString
	query := "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	if err := q.QueryRowContext(ctx, query, schema, table).Scan(&engine); err != nil {
		return fmt.Errorf("check target table engine for %s.%s: %w", schema, table, err)
	}
	if !engine.Valid || !strings.EqualFold(strings.TrimSpace(engine.String), "InnoDB") {
		eng := ""
		if engine.Valid {
			eng = engine.String
		}
		return fmt.Errorf("target table %s.%s engine %q is not InnoDB: transactional write recovery requires InnoDB (fail-closed)", schema, table, eng)
	}
	return nil
}
