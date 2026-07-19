package fullload

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
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

	reported StatsSnapshot // 已上报到 Prometheus 的累计快照（仅 reportLoop / pushFinalMetrics 访问）
}

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

	planner := NewPlanner(e.SourceDB)
	targetChunks := opt.ReadWorkers * opt.ChunkOvershoot
	chunks, err := planner.Plan(cctx, specs, targetChunks)
	if err != nil {
		return err
	}
	atomic.StoreInt64(&e.Stats.ChunksTotal, int64(len(chunks)))
	logger.Info("[Task %s] FullLoadV2: planned %d chunks over %d table(s), read_workers=%d write_workers=%d buffer=%dMiB commit_rows=%d commit_bytes=%dMiB",
		e.TaskID, len(chunks), len(specs), opt.ReadWorkers, opt.WriteWorkers,
		opt.BufferBytes/1024/1024, opt.CommitRows, opt.CommitBytes/1024/1024)

	q := newBatchQueue(opt.BufferBytes, e.Stats)
	q.watchContext(cctx)
	e.reported = e.Stats.Snapshot()

	chunkCh := make(chan *Chunk, len(chunks))
	for _, c := range chunks {
		chunkCh <- c
	}
	close(chunkCh)

	// 周期性聚合日志与 Prometheus 上报（P4：不再逐批 INFO 日志）。
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
		writerErr = runWriters(cctx, e.TargetDB, q, opt, e.Stats, e.OnCommit, e.IsStopped)
		if writerErr != nil {
			cancel()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		readerErr = runReaders(cctx, e.SourceDB, chunkCh, q, opt, e.Stats, e.IsStopped)
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
}

func (e *Engine) pushFinalMetrics() {
	// 结束时补一次增量（reportLoop 已在 reportWG.Wait() 后停止，无并发访问 e.reported）。
	cur := e.Stats.Snapshot()
	e.pushMetrics(e.reported, cur)
}
