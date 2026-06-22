package service

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"database/sql"

	"mysql-to-sync/internal/metadata/domain/entity"
	"mysql-to-sync/internal/sync/infrastructure/reader"
	"mysql-to-sync/internal/sync/infrastructure/writer"
	taskEntity "mysql-to-sync/internal/task/domain/entity"
	"mysql-to-sync/pkg/logger"
)

const (
	defaultChannelBufferMultiplier = 4
	maxChannelBufferMultiplier     = 64
)

// BatchTask 批次任务结构
type BatchTask struct {
	BatchID  int                      // 批次ID
	Data     []map[string]interface{} // 数据批次
	StartPK  interface{}              // 起始主键
	EndPK    interface{}              // 结束主键
	WorkerID int                      // 处理的worker ID
	Mark     string                   // 标记信息
}

// ChannelSyncStats 通道同步观测指标（便于日志/Prometheus 接入）
type ChannelSyncStats struct {
	EnqueueWaitNs   int64 // 累计投递等待纳秒
	EnqueueCount    int64 // 成功投递批次数
	PendingBatches  int   // 当前 channel 中待处理批次数
	ChannelCapacity int   // channel 容量
}

// ChannelSync 基于channel的并行同步器
type ChannelSync struct {
	batchChan   chan *BatchTask // 批次任务channel
	workerDone  chan error      // worker 退出信号（nil=成功，非 nil=首个失败原因）
	workerCount int             // worker数量
	batchSize   int             // 批次大小
	processed   int64           // 已处理行数
	totalRows   int64           // 总行数
	enqueueWait int64           // 累计投递等待纳秒
	enqueueN    int64           // 成功投递批次数
}

// EffectiveChannelBufferBatches 计算 batchChan 容量（单位：批次数，非行数）。
// configured<=0 时默认 intraWorkers×4；configured>0 时使用配置值，上限 intraWorkers×64。
func EffectiveChannelBufferBatches(workerCount, configured int) int {
	if workerCount < 1 {
		workerCount = 1
	}
	defaultBuf := workerCount * defaultChannelBufferMultiplier
	maxBuf := workerCount * maxChannelBufferMultiplier
	if configured <= 0 {
		return defaultBuf
	}
	if configured > maxBuf {
		return maxBuf
	}
	return configured
}

// NewChannelSync 创建channel同步器
func NewChannelSync(workerCount, batchSize, channelBufferBatches int) *ChannelSync {
	buf := EffectiveChannelBufferBatches(workerCount, channelBufferBatches)
	return &ChannelSync{
		batchChan:   make(chan *BatchTask, buf),
		workerDone:  make(chan error, workerCount),
		workerCount: workerCount,
		batchSize:   batchSize,
	}
}

// StartWorkers 启动worker池
func (cs *ChannelSync) StartWorkers(ctx context.Context, processFunc func(*BatchTask) error) {
	for i := 0; i < cs.workerCount; i++ {
		go func(workerID int) {
			var exitErr error
			defer func() {
				cs.workerDone <- exitErr
			}()

			for {
				select {
				case <-ctx.Done():
					return
				case task, ok := <-cs.batchChan:
					if !ok {
						return
					}

					task.WorkerID = workerID

					err := processFunc(task)
					if err != nil {
						exitErr = fmt.Errorf("worker %d failed on batch %d: %w", workerID, task.BatchID, err)
						return
					}

					atomic.AddInt64(&cs.processed, int64(len(task.Data)))
				}
			}
		}(i)
	}
}

// AddBatch 阻塞投递批次任务，直到 channel 有空间或 ctx 取消。
func (cs *ChannelSync) AddBatch(ctx context.Context, batchID int, data []map[string]interface{}, startPK, endPK interface{}, mark string) error {
	task := &BatchTask{
		BatchID: batchID,
		Data:    data,
		StartPK: startPK,
		EndPK:   endPK,
		Mark:    mark,
	}

	start := time.Now()
	select {
	case cs.batchChan <- task:
		atomic.AddInt64(&cs.enqueueWait, int64(time.Since(start)))
		atomic.AddInt64(&cs.enqueueN, 1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitForCompletion 关闭 batch channel 并等待所有 worker 退出。
func (cs *ChannelSync) WaitForCompletion(ctx context.Context) error {
	close(cs.batchChan)

	for i := 0; i < cs.workerCount; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-cs.workerDone:
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Stats 返回当前观测指标快照。
func (cs *ChannelSync) Stats() ChannelSyncStats {
	return ChannelSyncStats{
		EnqueueWaitNs:   atomic.LoadInt64(&cs.enqueueWait),
		EnqueueCount:    atomic.LoadInt64(&cs.enqueueN),
		PendingBatches:  len(cs.batchChan),
		ChannelCapacity: cap(cs.batchChan),
	}
}

// GetProgress 获取处理进度
func (cs *ChannelSync) GetProgress() (int64, int64) {
	return atomic.LoadInt64(&cs.processed), cs.totalRows
}

// SetTotalRows 设置总行数
func (cs *ChannelSync) SetTotalRows(totalRows int64) {
	cs.totalRows = totalRows
}

// ChannelSyncExecutor channel同步执行器
type ChannelSyncExecutor struct {
	sourceDB              *sql.DB
	targetDB              *sql.DB
	auditLogger           interface{} // *audit.AuditLogger
	isTaskStopped         func(string) bool
	incrementTaskProgress func(string, int64, string)
	updateTaskTotalRows   func(string, int64)
}

// NewChannelSyncExecutor 创建channel同步执行器
func NewChannelSyncExecutor(sourceDB, targetDB *sql.DB, auditLogger interface{}, isTaskStopped func(string) bool, incrementTaskProgress func(string, int64, string), updateTaskTotalRows func(string, int64)) *ChannelSyncExecutor {
	return &ChannelSyncExecutor{
		sourceDB:              sourceDB,
		targetDB:              targetDB,
		auditLogger:           auditLogger,
		isTaskStopped:         isTaskStopped,
		incrementTaskProgress: incrementTaskProgress,
		updateTaskTotalRows:   updateTaskTotalRows,
	}
}

// ExecuteFullSyncChannel 执行channel并行同步
func (cse *ChannelSyncExecutor) ExecuteFullSyncChannel(ctx context.Context, task *taskEntity.SyncTask, sourceSchema, targetSchema, tableName string, identity *entity.TableIdentity, intraWorkers, readLimit, txCommitEveryN, channelBufferBatches int) error {
	taskID := task.Config.ID

	channelSync := NewChannelSync(intraWorkers, readLimit, channelBufferBatches)

	rdr := reader.NewRangeShardingReader(cse.sourceDB, sourceSchema, tableName, identity)
	cursorCols := identity.EffectiveCursorCols()

	channelSync.StartWorkers(ctx, func(batchTask *BatchTask) error {
		return cse.processBatchTask(ctx, task, batchTask, sourceSchema, targetSchema, tableName, identity, txCommitEveryN)
	})

	dispatchErr := make(chan error, 1)
	go func() {
		var dispatchResult error
		defer func() {
			dispatchErr <- dispatchResult
		}()

		batchID := 0
		lastPK := interface{}(nil)

		for {
			if cse.isTaskStopped(taskID) {
				return
			}

			readStart := time.Now()
			batch, err := rdr.ReadBatchByKeys(ctx, lastPK, int64(readLimit))
			readDur := time.Since(readStart)
			if err != nil {
				dispatchResult = fmt.Errorf("read batch failed: %w", err)
				return
			}

			if len(batch) == 0 {
				logger.Info("[Task %s] Channel sync: reached end of data for %s.%s", taskID, sourceSchema, tableName)
				break
			}

			var firstPK interface{}
			if len(cursorCols) == 1 {
				firstPK = batch[0][cursorCols[0]]
				lastPK = batch[len(batch)-1][cursorCols[0]]
			} else {
				firstVals := make([]interface{}, len(cursorCols))
				lastVals := make([]interface{}, len(cursorCols))
				for i, col := range cursorCols {
					firstVals[i] = batch[0][col]
					lastVals[i] = batch[len(batch)-1][col]
				}
				firstPK = firstVals
				lastPK = lastVals
			}
			mark := fmt.Sprintf("%s.%s:batch%d", sourceSchema, tableName, batchID)

			enqueueStart := time.Now()
			err = channelSync.AddBatch(ctx, batchID, batch, firstPK, lastPK, mark)
			enqueueDur := time.Since(enqueueStart)
			if err != nil {
				dispatchResult = fmt.Errorf("add batch failed: %w", err)
				return
			}

			st := channelSync.Stats()
			logger.Info("[Task %s] Channel sync dispatch batch %d: read=%s enqueue=%s pending=%d/%d",
				taskID, batchID, readDur, enqueueDur, st.PendingBatches, st.ChannelCapacity)

			batchID++
			cse.incrementTaskProgress(taskID, int64(len(batch)), mark)
		}
	}()

	select {
	case err := <-dispatchErr:
		if err != nil {
			return fmt.Errorf("dispatch failed: %w", err)
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	err := channelSync.WaitForCompletion(ctx)
	if err != nil {
		return fmt.Errorf("channel sync failed: %w", err)
	}

	processed, total := channelSync.GetProgress()
	st := channelSync.Stats()
	logger.Info("[Task %s] Channel sync completed for %s.%s: %d/%d rows, enqueue_batches=%d enqueue_wait=%s pending=%d",
		taskID, sourceSchema, tableName, processed, total, st.EnqueueCount, time.Duration(st.EnqueueWaitNs), st.PendingBatches)

	return nil
}

// processBatchTask 处理单个批次任务
func (cse *ChannelSyncExecutor) processBatchTask(ctx context.Context, task *taskEntity.SyncTask, batchTask *BatchTask, sourceSchema, targetSchema, tableName string, identity *entity.TableIdentity, txCommitEveryN int) error {
	taskID := task.Config.ID

	writeStart := time.Now()

	conn, err := cse.targetDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("worker %d failed to get connection: %w", batchTask.WorkerID, err)
	}
	defer conn.Close()

	writeSessionLabel := fmt.Sprintf("%s.%s worker %d", targetSchema, tableName, batchTask.WorkerID)
	if err := disableFullSyncWriteSession(ctx, conn, writeSessionLabel); err != nil {
		return err
	}
	defer restoreFullSyncWriteSession(conn, writeSessionLabel)

	var curTx *sql.Tx
	var txW *writer.BatchWriter
	var txBatchN int
	var txStartMark string
	var commitDur time.Duration

	defer func() {
		if curTx != nil {
			curTx.Rollback()
		}
	}()

	doWrite := func(rows []map[string]interface{}, mark string) error {
		if curTx == nil {
			var e error
			curTx, e = conn.BeginTx(ctx, nil)
			if e != nil {
				return fmt.Errorf("worker %d begin tx at %s: %w", batchTask.WorkerID, mark, e)
			}
			txW = writer.NewBatchWriterWithTx(curTx, identity, task.Config.BatchSize, targetSchema)
			txBatchN = 0
			txStartMark = mark
		}

		if e := txW.WriteBatch(ctx, rows); e != nil {
			curTx.Rollback()
			curTx = nil
			return fmt.Errorf("worker %d write at %s (tx from %s) rolled back: %w", batchTask.WorkerID, mark, txStartMark, e)
		}

		txBatchN++
		if txBatchN >= txCommitEveryN {
			commitStart := time.Now()
			if e := curTx.Commit(); e != nil {
				curTx = nil
				return fmt.Errorf("worker %d commit at %s (tx from %s): %w", batchTask.WorkerID, mark, txStartMark, e)
			}
			commitDur += time.Since(commitStart)
			curTx = nil
		}

		return nil
	}

	logger.Info("[Task %s] Worker %d processing batch %d: %d rows (PK %v -> %v)",
		taskID, batchTask.WorkerID, batchTask.BatchID, len(batchTask.Data), batchTask.StartPK, batchTask.EndPK)

	if err := doWrite(batchTask.Data, batchTask.Mark); err != nil {
		return fmt.Errorf("worker %d failed to write batch %d: %w", batchTask.WorkerID, batchTask.BatchID, err)
	}

	if curTx != nil {
		commitStart := time.Now()
		if err := curTx.Commit(); err != nil {
			return fmt.Errorf("worker %d final commit failed: %w", batchTask.WorkerID, err)
		}
		commitDur += time.Since(commitStart)
		curTx = nil
	}

	logger.Info("[Task %s] Worker %d batch %d done: write+commit=%s commit=%s",
		taskID, batchTask.WorkerID, batchTask.BatchID, time.Since(writeStart), commitDur)

	return nil
}
