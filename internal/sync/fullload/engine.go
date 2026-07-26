package fullload

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"mysql-to-sync/internal/metrics"
	"mysql-to-sync/pkg/logger"
)

// Engine 是全量 V2 任务级流水线引擎。
type Engine struct {
	SourceDB  *sql.DB
	TargetDB  *sql.DB
	Options   Options
	Stats     *Stats
	OnCommit  CommitCallback // 事务提交后按表回调，用于推进进度
	IsStopped func() bool    // 返回 true 时引擎尽快停止（用户暂停/取消）
	TaskID    string

	// SchemaLocks 由调用层预先获取的 schema 级互斥锁。
	// 非 nil 时 Engine 不再内部获取/释放锁，由调用层负责生命周期管理。
	SchemaLocks *SchemaLocks

	// OnTableDataReady 在表数据全部提交后调用；用于逐表重建索引（P2）。
	OnTableDataReady TableDataReadyCallback
	// OnTableStateChange 在表级状态变更时调用(P3 持久化);由集成层落盘到任务存档。
	OnTableStateChange TableStateCallback

	tracker      *tableCompletionTracker
	stateTracker *tableStateTracker // P2.5: 表级重试状态跟踪
	reported     StatsSnapshot      // 已上报到 Prometheus 的累计快照（仅 reportLoop / pushFinalMetrics 访问）
}

// TableStateCallback 表级状态变更回调(P3 持久化用)。
// schema/table 为源端表名;phase 为新阶段;attemptID 为当前 attempt 序号;
// stagingTable 为 staging 表名(空表示未启用);errMsg 为错误信息(仅 FAILED 时非空);
// committedRows 为当前 attempt 已提交行数。
type TableStateCallback func(schema, table, phase string, attemptID int, stagingTable, errMsg string, committedRows int64) error

// Run 对给定表集合执行任务级流水线全量复制。
func (e *Engine) Run(ctx context.Context, specs []*TableSpec) error {
	if e.Stats == nil {
		e.Stats = &Stats{}
	}
	opt := e.Options
	if e.SourceDB == nil || e.TargetDB == nil {
		return fmt.Errorf("full-load V2 requires non-nil source and target databases")
	}
	if opt.ReadWorkers < 1 || opt.WriteWorkers < 1 || opt.BatchRows < 1 || opt.BufferBytes < 1 {
		return fmt.Errorf("full-load V2 received unresolved or invalid options")
	}
	if err := opt.Validate(); err != nil {
		return err
	}

	// 读取并发预算不得超过真实源池上限（默认 source_max_open_conns=32）。
	poolMax := e.SourceDB.Stats().MaxOpenConnections
	beforeReadWorkers := opt.ReadWorkers
	beforeTableParallel := opt.TableParallelReaders
	opt.CapBySourcePool(poolMax)
	e.Options = opt
	if poolMax > 0 && (opt.ReadWorkers != beforeReadWorkers || opt.TableParallelReaders != beforeTableParallel) {
		logger.Info("[Task %s] FullLoadV2: capped read_workers %d -> %d table_parallel_readers %d -> %d by source pool max_open=%d",
			e.TaskID, beforeReadWorkers, opt.ReadWorkers, beforeTableParallel, opt.TableParallelReaders, poolMax)
	}

	// [P2] 目标连接池容量校验：锁连接占用 1 个池槽，写路径至少需要 1 个额外连接。
	targetPoolMax := e.TargetDB.Stats().MaxOpenConnections
	if targetPoolMax > 0 {
		if targetPoolMax < 2 {
			return fmt.Errorf("full-load V2 requires target_max_open_conns >= 2 (current=%d); lock connection reserves 1 slot", targetPoolMax)
		}
		usable := targetPoolMax - 1
		if opt.WriteWorkers > usable {
			logger.Info("[Task %s] FullLoadV2: capping WriteWorkers %d -> %d (target pool %d minus 1 lock conn)",
				e.TaskID, opt.WriteWorkers, usable, targetPoolMax)
			opt.WriteWorkers = usable
			e.Options = opt
		}
	}

	// 目标端必须为 InnoDB：写事务原子性与 Commit 未知时的 marker 探测都依赖事务引擎。
	if err := assertTargetTablesInnoDB(ctx, e.TargetDB, specs); err != nil {
		return err
	}
	// 同一目标 schema 禁止并发 V2：由调用层预持有锁则跳过内部获取。
	var schemaLocks *SchemaLocks
	if e.SchemaLocks != nil {
		schemaLocks = e.SchemaLocks
	} else {
		var err error
		schemaLocks, err = acquireTxMarkerSchemaLocks(ctx, e.TargetDB, specs)
		if err != nil {
			return err
		}
		defer func() {
			if rErr := schemaLocks.Release(context.Background()); rErr != nil {
				logger.Warn("[Task %s] FullLoadV2: release tx marker schema locks: %v", e.TaskID, rErr)
			}
		}()
	}
	_ = schemaLocks // used implicitly via connection-level lock
	if err := ensureTxMarkerTables(ctx, e.TargetDB, specs); err != nil {
		return err
	}
	runID, err := newTxMarkerID()
	if err != nil {
		return fmt.Errorf("allocate full-load run_id: %w", err)
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopWatchDone := make(chan struct{})
	if e.IsStopped != nil {
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stopWatchDone:
					return
				case <-cctx.Done():
					return
				case <-ticker.C:
					if e.IsStopped() {
						cancel()
						return
					}
				}
			}
		}()
	}
	defer close(stopWatchDone)

	// chunk 在每表快照事务内规划（P2），此处仅组装表级任务。
	jobs := make([]*tableReadJob, 0, len(specs))
	for _, spec := range specs {
		jobs = append(jobs, &tableReadJob{spec: spec})
	}
	e.tracker = newTableCompletionTracker(specs, e.OnTableDataReady)
	// P2.5: 启用重试或 staging 时创建表级状态跟踪器
	if opt.ReadRetryTimes > 0 || opt.StagingEnabled {
		e.stateTracker = newTableStateTracker(specs)
		// P3: 绑定状态变更回调,由集成层持久化到任务存档
		e.stateTracker.onChange = e.OnTableStateChange
	}

	q := newBatchQueue(opt.BufferBytes, e.Stats)
	q.watchContext(cctx)
	e.reported = e.Stats.Snapshot()

	logger.Info("[Task %s] FullLoadV2: %d table(s), read_workers=%d write_workers=%d buffer=%dMiB commit_rows=%d commit_bytes=%dMiB (plain-short-query)",
		e.TaskID, len(specs), opt.ReadWorkers, opt.WriteWorkers,
		opt.BufferBytes/1024/1024, opt.CommitRows, opt.CommitBytes/1024/1024)

	// 周期性聚合日志与 Prometheus 上报。
	stopReport := make(chan struct{})
	var reportWG sync.WaitGroup
	reportWG.Add(1)
	go func() {
		defer reportWG.Done()
		e.reportLoop(cctx, stopReport)
	}()

	var (
		readerErr error
		writerErr error
		wg        sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		writerErr = runWriters(cctx, e.TargetDB, q, opt, e.Stats, e.OnCommit, e.tracker, e.stateTracker, e.IsStopped, runID)
		if writerErr != nil {
			cancel()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		readerErr = runTableReaders(cctx, e.SourceDB, jobs, q, e, opt, e.Stats, e.IsStopped)
		// 读取全部完成（或出错）后关闭队列，写入 worker 排空后退出。
		q.Close()
		if readerErr != nil {
			cancel()
		}
	}()

	wg.Wait()
	close(stopReport)
	reportWG.Wait()
	e.pushFinalMetrics()

	snap := e.Stats.Snapshot()
	logger.Info("[Task %s] FullLoadV2 finished: committed_rows=%d committed_bytes=%d commits=%d tx_replays=%d lock_retries=%d chunks=%d/%d",
		e.TaskID, snap.CommittedRows, snap.CommittedBytes, snap.Commits, snap.TxReplays, snap.LockRetries, snap.ChunksDone, snap.ChunksTotal)

	if readerErr != nil {
		return readerErr
	}
	if writerErr != nil {
		return writerErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if cctx.Err() != nil {
		return context.Canceled
	}
	// 数据流水线成功后按 run_id 删除本任务 marker 行（不 DROP 共享表）。
	// 注意：此处早于可能存在的索引恢复等任务级收尾；清理失败只告警。
	if err := cleanupTxMarkerRows(ctx, e.TargetDB, specs, runID); err != nil {
		logger.Warn("[Task %s] FullLoadV2: cleanup tx marker rows after pipeline success failed (sync already committed): %v", e.TaskID, err)
	}
	return nil
}

func (e *Engine) reportLoop(ctx context.Context, stop <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			cur := e.Stats.Snapshot()
			e.pushMetrics(e.reported, cur)
			logger.Info("[Task %s] FullLoadV2 progress: read=%d rows write=%d rows commit=%d rows queue=%d/%d bytes readers=%d writers=%d replays=%d",
				e.TaskID, cur.ReadRows, cur.WrittenRows, cur.CommittedRows,
				cur.QueueBytes, cur.QueueCap, cur.ActiveReaders, cur.ActiveWriters, cur.TxReplays)
			e.reported = cur
		}
	}
}

func (e *Engine) pushMetrics(prev, cur StatsSnapshot) {
	m := metrics.GetMetrics()
	m.AddFullLoadRead(cur.ReadRows-prev.ReadRows, cur.ReadBytes-prev.ReadBytes)
	m.AddFullLoadWrite(cur.WrittenRows-prev.WrittenRows, cur.WrittenBytes-prev.WrittenBytes)
	m.AddFullLoadCommit(cur.CommittedRows-prev.CommittedRows, cur.CommittedBytes-prev.CommittedBytes, cur.Commits-prev.Commits)
	m.AddFullLoadTxReplays(cur.TxReplays - prev.TxReplays)
	m.AddFullLoadLockRetries(cur.LockRetries - prev.LockRetries)
	m.AddFullLoadQueueBytes(cur.QueueBytes - prev.QueueBytes)
	m.AddFullLoadActiveWorkers(cur.ActiveReaders-prev.ActiveReaders, cur.ActiveWriters-prev.ActiveWriters)
	// P3.6: P0/P2 可观测性指标
	m.AddFullLoadQueryTimeouts(cur.QueryTimeouts - prev.QueryTimeouts)
	m.AddFullLoadSlowQueries(cur.SlowQueries - prev.SlowQueries)
	m.AddFullLoadTableRetries(cur.TableRetries - prev.TableRetries)
	m.AddFullLoadTableRetryExhausted(cur.TableRetryExhausted - prev.TableRetryExhausted)
	m.AddFullLoadActiveStagingTables(cur.ActiveStagingTables - prev.ActiveStagingTables)
}

func (e *Engine) pushFinalMetrics() {
	// 结束时补一次增量（reportLoop 已在 reportWG.Wait() 后停止，无并发访问 e.reported）。
	cur := e.Stats.Snapshot()
	e.pushMetrics(e.reported, cur)
}

func groupChunksByTable(chunks []*Chunk) map[string][]*Chunk {
	m := make(map[string][]*Chunk)
	for _, c := range chunks {
		if c == nil || c.Spec == nil {
			continue
		}
		key := tableKey(c.Spec.SourceSchema, c.Spec.SourceTable)
		m[key] = append(m[key], c)
	}
	return m
}

func tableKey(schema, table string) string {
	return schema + "." + table
}
