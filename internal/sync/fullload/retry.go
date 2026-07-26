// Package fullload 实现 P2.5: 表级读取重试的错误分类与退避策略。
package fullload

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"mysql-to-sync/pkg/logger"
)

// isRetryableReadError 判断源端读取错误是否值得表级重试。
//
// 可重试的错误类别：
//   - 查询超时 (ReadQueryTimeoutError)
//   - 连接失效 (driver.ErrBadConn 及网络断连)
//   - 锁等待超时 (Lock wait timeout)
//   - MySQL 临时不可用 (Server shutdown / Lost connection during query)
//
// 不可重试的错误（直接失败）：
//   - 语法错误 / 表不存在 / 列不存在
//   - 上下文取消 (用户主动停止)
//   - 权限不足
func isRetryableReadError(err error) bool {
	if err == nil {
		return false
	}
	// 查询超时可重试（须先于通用 DeadlineExceeded 判断，因可能被 %w 包装）
	if IsReadQueryTimeout(err) {
		return true
	}
	// 上下文取消不重试（用户主动停止 / 任务级取消）
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	// 连接失效可重试
	if errors.Is(err, driver.ErrBadConn) {
		return true
	}
	s := strings.ToLower(err.Error())
	if strings.Contains(s, "invalid connection") ||
		strings.Contains(s, "bad connection") ||
		strings.Contains(s, "connection refused") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "connection was bad") {
		return true
	}
	if strings.Contains(s, "lock wait timeout") ||
		strings.Contains(s, "try restarting transaction") {
		return true
	}
	if strings.Contains(s, "server shutdown") ||
		strings.Contains(s, "lost connection") ||
		strings.Contains(s, "unexpected eof") ||
		strings.Contains(s, "unexpected packet") {
		return true
	}
	return false
}

// retryBackoff 计算第 attempt 次重试的退避时间（指数退避 + 抖动）。
// attempt 从 1 开始（第 1 次重试 = 第 1 次失败后）。
func retryBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	const (
		base    = 5 * time.Second
		maxWait = 60 * time.Second
	)
	wait := base << (attempt - 1)
	if wait > maxWait {
		wait = maxWait
	}
	jitter := time.Duration(rand.Int63n(int64(wait) / 5))
	if rand.Intn(2) == 0 {
		wait += jitter
	} else {
		wait -= jitter
	}
	if wait < base/2 {
		wait = base / 2
	}
	if wait > maxWait {
		wait = maxWait
	}
	return wait
}

// readTableWithRetry 包装 readTableWithSnapshot，提供表级自动重试。
//
// 当 StagingEnabled=true 时，每次 attempt 创建独立 staging 表，读取完成后发布到最终表。
// ReadRetryTimes 是额外重试次数：总 attempt = 1 + ReadRetryTimes。
// ReadRetryTimes>0 时调用方必须已通过 Options.Validate() 强制 staging。
//
// 使用 attempt 级子 context：可重试错误只取消当前 attempt，不取消任务级 ctx。
func readTableWithRetry(
	ctx context.Context,
	db *sql.DB,
	job *tableReadJob,
	q *batchQueue,
	eng *Engine,
	lim *snapshotLimiter,
	opt Options,
	stats *Stats,
	isStopped func() bool,
	taskCancel context.CancelFunc,
	stateTracker *tableStateTracker,
) error {
	if job == nil || job.spec == nil {
		return fmt.Errorf("invalid table read job")
	}
	schema := job.spec.SourceSchema
	table := job.spec.SourceTable

	maxRetries := opt.ReadRetryTimes
	if maxRetries <= 0 && !opt.StagingEnabled {
		return readTableWithSnapshot(ctx, db, job, q, eng, lim, opt, stats, isStopped, taskCancel, nil)
	}
	if stateTracker == nil {
		return readTableWithSnapshot(ctx, db, job, q, eng, lim, opt, stats, isStopped, taskCancel, nil)
	}
	if maxRetries > 0 && !opt.StagingEnabled {
		return fmt.Errorf("table %s.%s: read retry requires staging (fail-closed)", schema, table)
	}

	targetSchema := job.spec.TargetSchema
	targetTable := job.spec.TargetTable
	var prevAttemptID int

	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil || (isStopped != nil && isStopped()) {
			if taskCancel != nil {
				taskCancel()
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return context.Canceled
		}

		// 必须先按旧 attempt 等待 inflight 排空并 DROP staging，再 startAttempt。
		// startAttempt 会把 Inflight 重置为 0 并切换 AttemptID；若先切换，barrier 会空转通过。
		if opt.StagingEnabled && prevAttemptID > 0 {
			if err := stateTracker.waitInflightZero(schema, table, 5*time.Minute); err != nil {
				return fmt.Errorf("inflight barrier before retry for %s.%s: %w", schema, table, err)
			}
			_ = dropStagingTable(ctx, eng.TargetDB, targetSchema, targetTable, prevAttemptID)
			if stats != nil {
				atomic.AddInt64(&stats.ActiveStagingTables, -1)
			}
		}

		attemptID, aErr := stateTracker.startAttempt(schema, table, maxRetries)
		if aErr != nil {
			return fmt.Errorf("table %s.%s retry exhausted: %w", schema, table, aErr)
		}
		job.AttemptID = attemptID

		if opt.StagingEnabled {
			if err := createStagingTable(ctx, eng.TargetDB, targetSchema, targetTable, attemptID); err != nil {
				return fmt.Errorf("create staging table for %s.%s attempt %d: %w", targetSchema, targetTable, attemptID, err)
			}
			stagingName := stagingTableName(targetTable, attemptID)
			if err := stateTracker.setStagingTable(schema, table, stagingName); err != nil {
				_ = dropStagingTable(ctx, eng.TargetDB, targetSchema, targetTable, attemptID)
				return err
			}
			if stats != nil {
				atomic.AddInt64(&stats.ActiveStagingTables, 1)
			}
		}

		if err := stateTracker.transitionTo(schema, table, PhaseCopying); err != nil {
			return err
		}

		// attempt 级子 context：并行 chunk 失败只取消本 attempt
		attemptCtx, attemptCancel := context.WithCancel(ctx)
		readErr := readTableWithSnapshot(attemptCtx, db, job, q, eng, lim, opt, stats, isStopped, taskCancel, attemptCancel)
		attemptCancel()

		if readErr == nil {
			stateTracker.markReadDone(schema, table)
			if opt.StagingEnabled {
				if err := stateTracker.waitInflightZero(schema, table, 5*time.Minute); err != nil {
					return fmt.Errorf("inflight barrier before publish for %s.%s: %w", schema, table, err)
				}
				if err := stateTracker.transitionTo(schema, table, PhaseDataReady); err != nil {
					return err
				}
				if err := publishStagingTable(ctx, eng.TargetDB, targetSchema, targetTable, attemptID); err != nil {
					return fmt.Errorf("publish staging table for %s.%s: %w", targetSchema, targetTable, err)
				}
				if stats != nil {
					atomic.AddInt64(&stats.ActiveStagingTables, -1)
				}
				if err := stateTracker.transitionTo(schema, table, PhasePublished); err != nil {
					return err
				}
				if err := dropOldBackupTables(ctx, eng.TargetDB, targetSchema, targetTable, 1); err != nil {
					logger.Warn("[FullLoadV2] cleanup old backup tables for %s.%s: %v", targetSchema, targetTable, err)
				}
			} else {
				if err := stateTracker.transitionTo(schema, table, PhasePublished); err != nil {
					return err
				}
			}
			return nil
		}

		prevAttemptID = attemptID
		if err := stateTracker.recordError(schema, table, readErr); err != nil {
			// 状态落盘失败必须 fail-closed：不可在未知 FAILED 持久化结果下继续重试。
			return fmt.Errorf("persist FAILED state for %s.%s: %w (read error: %v)", schema, table, err, readErr)
		}

		if !isRetryableReadError(readErr) {
			if opt.StagingEnabled {
				if err := stateTracker.waitInflightZero(schema, table, 5*time.Minute); err != nil {
					logger.Warn("[FullLoadV2] inflight barrier before drop staging %s.%s: %v", schema, table, err)
				}
				_ = dropStagingTable(ctx, eng.TargetDB, targetSchema, targetTable, attemptID)
				if stats != nil {
					atomic.AddInt64(&stats.ActiveStagingTables, -1)
				}
			}
			return readErr
		}

		// attempt 已用尽额外重试：1+maxRetries
		if attempt > maxRetries {
			logger.Warn("[FullLoadV2] table %s.%s retry exhausted after %d attempts: %v", schema, table, attempt, readErr)
			if stats != nil {
				atomic.AddInt64(&stats.TableRetryExhausted, 1)
			}
			if opt.StagingEnabled {
				if err := stateTracker.waitInflightZero(schema, table, 5*time.Minute); err != nil {
					logger.Warn("[FullLoadV2] inflight barrier before drop staging %s.%s: %v", schema, table, err)
				}
				_ = dropStagingTable(ctx, eng.TargetDB, targetSchema, targetTable, attemptID)
				if stats != nil {
					atomic.AddInt64(&stats.ActiveStagingTables, -1)
				}
			}
			return fmt.Errorf("table %s.%s retry exhausted after %d attempts: %w", schema, table, attempt, readErr)
		}

		backoff := retryBackoff(attempt)
		logger.Warn("[FullLoadV2] table %s.%s read failed (attempt %d), retrying in %s: %v",
			schema, table, attempt, backoff, readErr)
		if stats != nil {
			atomic.AddInt64(&stats.TableRetries, 1)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
}
