package fullload

import (
	"context"
	"database/sql"
	"time"

	"mysql-to-sync/pkg/logger"
)

// setupWriteSession 为目标写入连接应用批量装载优化：关闭外键/唯一检查、放宽锁等待、
// 可选关闭 binlog。连接归还前由 restoreWriteSession 恢复。
func setupWriteSession(ctx context.Context, conn *sql.Conn, skipBinlog bool) error {
	if _, err := conn.ExecContext(ctx, "SET @@SESSION.FOREIGN_KEY_CHECKS=0"); err != nil {
		return err
	}
	configured := false
	defer func() {
		if !configured {
			restoreWriteSession(conn, false)
		}
	}()
	if _, err := conn.ExecContext(ctx, "SET @@SESSION.UNIQUE_CHECKS=0"); err != nil {
		logger.Warn("[FullLoadV2] disable unique checks failed (continuing, write-only optimization): %v", err)
	}

	var fk, uk int
	if err := conn.QueryRowContext(ctx, "SELECT @@SESSION.FOREIGN_KEY_CHECKS, @@SESSION.UNIQUE_CHECKS").Scan(&fk, &uk); err != nil {
		return err
	}
	if fk != 0 {
		return errForeignKeyChecksStillOn
	}

	if _, err := conn.ExecContext(ctx, "SET SESSION innodb_lock_wait_timeout=300"); err != nil {
		logger.Warn("[FullLoadV2] set lock wait timeout failed (continuing): %v", err)
	}

	if skipBinlog {
		if _, err := conn.ExecContext(ctx, "SET SESSION sql_log_bin=0"); err != nil {
			return err
		}
	}

	configured = true
	return nil
}

func restoreWriteSession(conn *sql.Conn, skipBinlog bool) {
	if conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := conn.ExecContext(ctx, "SET @@SESSION.FOREIGN_KEY_CHECKS=1"); err != nil {
		logger.Warn("[FullLoadV2] restore foreign key checks failed: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "SET @@SESSION.UNIQUE_CHECKS=1"); err != nil {
		logger.Warn("[FullLoadV2] restore unique checks failed: %v", err)
	}
	if skipBinlog {
		if _, err := conn.ExecContext(ctx, "SET SESSION sql_log_bin=1"); err != nil {
			logger.Warn("[FullLoadV2] restore sql_log_bin failed: %v", err)
		}
	}
}

type sessionError string

func (e sessionError) Error() string { return string(e) }

const errForeignKeyChecksStillOn sessionError = "target write session still has FOREIGN_KEY_CHECKS enabled"
