package fullload

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"mysql-to-sync/internal/metadata/domain/entity"
	"mysql-to-sync/pkg/logger"

	"github.com/go-mysql-org/go-mysql/mysql"
)

const snapshotStepTimeout = 5 * time.Second

// snapshotQueryer 是 chunk 读取使用的查询接口；必须绑定单连接快照，禁止 *sql.DB 连接池。
type snapshotQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// TableSnapshotCallback 在 ALL+无 PK/UK 表级快照就绪且捕获 HWM 位点后调用。
type TableSnapshotCallback func(schema, table string, pos mysql.Position) error

// SnapshotOptions 控制表级快照打开行为。
type SnapshotOptions struct {
	CaptureHWM         bool
	OnReady            TableSnapshotCallback
	LockWaitTimeoutSec int
}

type tableSnapshot struct {
	conn *sql.Conn
}

func (ts *tableSnapshot) commit(_ context.Context) error {
	// 与 rollback/UNLOCK 一致：收尾 COMMIT 不能绑在可能已取消的父 ctx 上。
	// 读表完成进入 commit 时，共享 cctx 可能因停止、写端失败或其他表错误已被 cancel；
	// 若此处跟随父 ctx，会把正常收尾误报成 context canceled。
	commitCtx, cancel := context.WithTimeout(context.Background(), snapshotStepTimeout)
	defer cancel()
	_, err := ts.conn.ExecContext(commitCtx, "COMMIT")
	return err
}

func (ts *tableSnapshot) rollback(ctx context.Context) {
	if ts.conn == nil {
		return
	}
	rbCtx, cancel := context.WithTimeout(context.Background(), snapshotStepTimeout)
	defer cancel()
	if _, err := ts.conn.ExecContext(rbCtx, "ROLLBACK"); err != nil {
		logger.Warn("[FullLoadV2] snapshot rollback failed: %v", err)
	}
}

func (ts *tableSnapshot) close() {
	if ts.conn != nil {
		_ = ts.conn.Close()
		ts.conn = nil
	}
}

func closeSnapshots(snaps []*tableSnapshot) {
	for _, s := range snaps {
		if s != nil {
			s.close()
		}
	}
}

func rollbackSnapshots(ctx context.Context, snaps []*tableSnapshot) {
	for _, s := range snaps {
		if s != nil {
			s.rollback(ctx)
		}
	}
}

func commitSnapshots(ctx context.Context, snaps []*tableSnapshot) error {
	var firstErr error
	for i, s := range snaps {
		if s == nil {
			continue
		}
		if err := s.commit(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("commit snapshot[%d]: %w", i, err)
		}
	}
	return firstErr
}

// openTableSnapshot 在单连接上打开 InnoDB RR 一致性只读快照。
// CaptureHWM 时在协调连接上短锁表，于快照连接读取 SHOW MASTER STATUS 后解锁；失败 fail-closed。
func openTableSnapshot(ctx context.Context, db *sql.DB, schema, table, firstCol string, opt SnapshotOptions) (*tableSnapshot, error) {
	snaps, err := openAlignedTableSnapshots(ctx, db, schema, table, firstCol, 1, opt)
	if err != nil {
		return nil, err
	}
	return snaps[0], nil
}

// openAlignedTableSnapshots 打开 N 个已对齐的表级 RR 只读一致性快照。
// N>1 时必须短暂 FTWRL，使各连接 ReadView 看到同一表版本；N=1 且无需 HWM 时可无显式写阻塞锁。
//
// 关键顺序：先拿齐真实 reader（及可选协调）连接，再取表锁、建快照。禁止「已持表锁再阻塞等连接池」。
func openAlignedTableSnapshots(ctx context.Context, db *sql.DB, schema, table, firstCol string, n int, opt SnapshotOptions) ([]*tableSnapshot, error) {
	if n < 1 {
		return nil, fmt.Errorf("snapshot reader count must be >= 1")
	}
	if err := assertInnoDBTable(ctx, db, schema, table); err != nil {
		return nil, err
	}

	needLock := n > 1 || opt.CaptureHWM

	var lockConn *sql.Conn
	var restoreLockWaitTimeout func()
	readerConns := make([]*sql.Conn, 0, n)
	locked := false
	closeLockConn := func() {
		if lockConn == nil {
			return
		}
		if restoreLockWaitTimeout != nil {
			restoreLockWaitTimeout()
			restoreLockWaitTimeout = nil
		}
		_ = lockConn.Close()
		lockConn = nil
	}
	defer func() {
		if lockConn != nil {
			if locked {
				if unlockErr := unlockTableReadLock(lockConn); unlockErr != nil {
					logger.Warn("[FullLoadV2] defer UNLOCK TABLES for %s.%s failed: %v", schema, table, unlockErr)
				}
			}
			closeLockConn()
		}
	}()
	releaseAcquired := func() {
		for _, c := range readerConns {
			if c != nil {
				_ = c.Close()
			}
		}
		readerConns = nil
		closeLockConn()
	}

	// 1) 先拿齐真实连接，避免持锁期间阻塞在 db.Conn。
	if needLock {
		var err error
		lockConn, err = db.Conn(ctx)
		if err != nil {
			return nil, fmt.Errorf("get table lock connection for %s.%s: %w", schema, table, err)
		}
	}
	for i := 0; i < n; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			releaseAcquired()
			return nil, fmt.Errorf("get snapshot connection[%d] for %s.%s: %w", i, schema, table, err)
		}
		readerConns = append(readerConns, conn)
	}

	// 2) 持齐连接后再取表锁。
	if needLock {
		restore, err := setLockWaitTimeout(ctx, lockConn, opt.LockWaitTimeoutSec)
		if err != nil {
			releaseAcquired()
			return nil, fmt.Errorf("set lock_wait_timeout for %s.%s: %w", schema, table, err)
		}
		restoreLockWaitTimeout = restore

		lockCtx := ctx
		var lockCancel context.CancelFunc
		if opt.LockWaitTimeoutSec > 0 {
			lockCtx, lockCancel = context.WithTimeout(ctx, time.Duration(opt.LockWaitTimeoutSec)*time.Second)
			defer lockCancel()
		}
		lockSQL := fmt.Sprintf("FLUSH TABLES %s.%s WITH READ LOCK",
			quoteIdentifier(schema), quoteIdentifier(table))
		if _, err := lockConn.ExecContext(lockCtx, lockSQL); err != nil {
			releaseAcquired()
			return nil, fmt.Errorf("acquire table read lock for %s.%s: %w", schema, table, err)
		}
		locked = true

		// 持锁后权威校验：锁阻塞 ALTER ENGINE，引擎此后不可变。
		if err := assertInnoDBTable(ctx, lockConn, schema, table); err != nil {
			releaseAcquired()
			return nil, err
		}
	}

	snaps := make([]*tableSnapshot, 0, n)
	cleanup := func() {
		rollbackSnapshots(ctx, snaps)
		closeSnapshots(snaps)
		// 尚未挂入 snaps 的连接也要回滚后归还，避免脏事务回池。
		for _, c := range readerConns {
			if c != nil {
				rollbackConn(c)
				_ = c.Close()
			}
		}
		readerConns = nil
	}

	// 3) 在已持有的连接上建立一致性快照。
	for i, conn := range readerConns {
		ts := &tableSnapshot{conn: conn}
		readerConns[i] = nil // 所有权转移到 snaps / 失败清理路径
		if err := startConsistentSnapshot(ctx, conn, schema, table, firstCol); err != nil {
			// startConsistentSnapshot 在 START 成功后的失败路径已 ROLLBACK；此处再关连接归还池。
			ts.close()
			cleanup()
			return nil, fmt.Errorf("start consistent snapshot[%d] for %s.%s: %w", i, schema, table, err)
		}
		// 无显式表锁路径：LIMIT 1 已持有表级 MDL 后再校验，关闭 DDL 竞态窗口。
		if !needLock {
			if err := assertInnoDBTable(ctx, conn, schema, table); err != nil {
				ts.rollback(ctx)
				ts.close()
				cleanup()
				return nil, err
			}
		}
		snaps = append(snaps, ts)
	}
	readerConns = nil

	if opt.CaptureHWM {
		posCtx, posCancel := context.WithTimeout(ctx, snapshotStepTimeout)
		pos, err := queryMasterPosition(posCtx, snaps[0].conn)
		posCancel()
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("capture table HWM for %s.%s: %w", schema, table, err)
		}
		if err := unlockTableReadLock(lockConn); err != nil {
			cleanup()
			return nil, fmt.Errorf("release table read lock for %s.%s: %w", schema, table, err)
		}
		locked = false

		if opt.OnReady != nil {
			if err := opt.OnReady(schema, table, pos); err != nil {
				cleanup()
				return nil, fmt.Errorf("table snapshot ready callback for %s.%s: %w", schema, table, err)
			}
		}
	} else if needLock {
		if err := unlockTableReadLock(lockConn); err != nil {
			cleanup()
			return nil, fmt.Errorf("release table read lock for %s.%s: %w", schema, table, err)
		}
		locked = false
	}

	return snaps, nil
}

// setLockWaitTimeout 设置 SESSION lock_wait_timeout，并返回归还连接池前的恢复函数。
// go-sql-driver/mysql 不会在 Conn.Close 时自动还原会话变量；若不恢复，池内连接会残留超时。
func setLockWaitTimeout(ctx context.Context, conn *sql.Conn, sec int) (func(), error) {
	var prev int
	if err := conn.QueryRowContext(ctx, "SELECT @@SESSION.lock_wait_timeout").Scan(&prev); err != nil {
		return nil, fmt.Errorf("read session lock_wait_timeout: %w", err)
	}
	if sec <= 0 {
		sec = defaultLockWaitTimeoutSec
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION lock_wait_timeout = %d", sec)); err != nil {
		return nil, err
	}
	return func() {
		restoreCtx, cancel := context.WithTimeout(context.Background(), snapshotStepTimeout)
		defer cancel()
		if _, err := conn.ExecContext(restoreCtx, fmt.Sprintf("SET SESSION lock_wait_timeout = %d", prev)); err != nil {
			logger.Warn("[FullLoadV2] restore session lock_wait_timeout=%d failed: %v", prev, err)
		}
	}, nil
}

func assertInnoDBTable(ctx context.Context, q rowQueryer, schema, table string) error {
	var engine sql.NullString
	query := "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	if err := q.QueryRowContext(ctx, query, schema, table).Scan(&engine); err != nil {
		return fmt.Errorf("check table engine for %s.%s: %w", schema, table, err)
	}
	if !engine.Valid || !strings.EqualFold(strings.TrimSpace(engine.String), "InnoDB") {
		eng := ""
		if engine.Valid {
			eng = engine.String
		}
		return fmt.Errorf("table %s.%s engine %q is not InnoDB: consistent snapshot requires InnoDB (fail-closed)", schema, table, eng)
	}
	return nil
}

type rowQueryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func startConsistentSnapshot(ctx context.Context, conn *sql.Conn, schema, table, firstCol string) error {
	if _, err := conn.ExecContext(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return fmt.Errorf("set RR isolation: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"); err != nil {
		return fmt.Errorf("start consistent snapshot: %w", err)
	}
	holdSQL := fmt.Sprintf("SELECT %s FROM %s.%s LIMIT 1",
		quoteIdentifier(firstCol),
		quoteIdentifier(schema), quoteIdentifier(table))
	rows, err := conn.QueryContext(ctx, holdSQL)
	if err != nil {
		// database/sql 不知道原始 SQL 事务；必须显式 ROLLBACK，否则 Close 会把未回滚事务送回连接池。
		rollbackConn(conn)
		return fmt.Errorf("hold MDL with LIMIT 1: %w", err)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		rollbackConn(conn)
		return fmt.Errorf("hold MDL with LIMIT 1: %w", err)
	}
	return nil
}

func rollbackConn(conn *sql.Conn) {
	if conn == nil {
		return
	}
	rbCtx, cancel := context.WithTimeout(context.Background(), snapshotStepTimeout)
	defer cancel()
	if _, err := conn.ExecContext(rbCtx, "ROLLBACK"); err != nil {
		logger.Warn("[FullLoadV2] connection rollback failed: %v", err)
	}
}

func unlockTableReadLock(conn *sql.Conn) error {
	ctx, cancel := context.WithTimeout(context.Background(), snapshotStepTimeout)
	defer cancel()
	_, err := conn.ExecContext(ctx, "UNLOCK TABLES")
	return err
}

type masterStatusQueryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func queryMasterPosition(ctx context.Context, db masterStatusQueryer) (mysql.Position, error) {
	var binlogFile, binlogDoDB, binlogIgnoreDB, executedGtidSet string
	var binlogPos uint32
	if err := db.QueryRowContext(ctx, "SHOW MASTER STATUS").Scan(
		&binlogFile, &binlogPos, &binlogDoDB, &binlogIgnoreDB, &executedGtidSet,
	); err != nil {
		if err2 := db.QueryRowContext(ctx, "SHOW MASTER STATUS").Scan(
			&binlogFile, &binlogPos, &binlogDoDB, &binlogIgnoreDB,
		); err2 != nil {
			return mysql.Position{}, fmt.Errorf("show master status failed (5col: %v, 4col: %v)", err, err2)
		}
	}
	if binlogFile == "" || binlogPos == 0 {
		return mysql.Position{}, fmt.Errorf("empty master status position")
	}
	return mysql.Position{Name: binlogFile, Pos: binlogPos}, nil
}

func isNoPKSpec(spec *TableSpec) bool {
	return spec != nil && spec.Identity != nil && spec.Identity.Strategy == entity.FullColumnsStrategy
}

// decideTableReadersForSpec 在规划 chunk 之前，仅根据估算行数决定对齐快照连接数。
// 无 PK 表始终 1；中小表 1；超大表用 TableParallelReaders。
func decideTableReadersForSpec(spec *TableSpec, opt Options) int {
	if spec == nil || isNoPKSpec(spec) {
		return 1
	}
	if opt.LargeTableRows > 0 && (spec.EstimatedRows <= 0 || spec.EstimatedRows < opt.LargeTableRows) {
		return 1
	}
	n := opt.TableParallelReaders
	if n < 1 {
		n = 1
	}
	return n
}

// decideTableReaders 在已有 chunk 时收紧并行度（不超过 chunk 数）。
func decideTableReaders(job *tableReadJob, opt Options) int {
	n := decideTableReadersForSpec(job.spec, opt)
	if job == nil || len(job.chunks) <= 1 {
		return 1
	}
	if n > len(job.chunks) {
		n = len(job.chunks)
	}
	return n
}
