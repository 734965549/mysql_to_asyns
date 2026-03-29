package service

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"database/sql"

	"mysql-to-async/internal/metadata/domain/entity"
	"mysql-to-async/internal/sync/infrastructure/reader"
	"mysql-to-async/internal/sync/infrastructure/writer"
	taskEntity "mysql-to-async/internal/task/domain/entity"
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

// ChannelSync 基于channel的并行同步器
type ChannelSync struct {
	batchChan   chan *BatchTask // 批次任务channel
	resultChan  chan error      // 结果channel
	workerCount int             // worker数量
	batchSize   int             // 批次大小
	processed   int64           // 已处理行数
	totalRows   int64           // 总行数
}

// NewChannelSync 创建channel同步器
func NewChannelSync(workerCount, batchSize int) *ChannelSync {
	return &ChannelSync{
		batchChan:   make(chan *BatchTask, workerCount*2), // 缓冲区为worker数量的2倍
		resultChan:  make(chan error, workerCount),
		workerCount: workerCount,
		batchSize:   batchSize,
	}
}

// StartWorkers 启动worker池
func (cs *ChannelSync) StartWorkers(ctx context.Context, processFunc func(*BatchTask) error) {
	for i := 0; i < cs.workerCount; i++ {
		go func(workerID int) {
			for {
				select {
				case <-ctx.Done():
					return
				case task := <-cs.batchChan:
					if task == nil {
						return // channel关闭，退出
					}

					// 记录worker ID
					task.WorkerID = workerID

					// 处理批次
					err := processFunc(task)
					if err != nil {
						cs.resultChan <- fmt.Errorf("worker %d failed on batch %d: %v", workerID, task.BatchID, err)
						return
					}

					// 更新处理进度
					atomic.AddInt64(&cs.processed, int64(len(task.Data)))

					// 发送完成信号
					cs.resultChan <- nil
				}
			}
		}(i)
	}
}

// AddBatch 添加批次任务
func (cs *ChannelSync) AddBatch(batchID int, data []map[string]interface{}, startPK, endPK interface{}, mark string) error {
	task := &BatchTask{
		BatchID: batchID,
		Data:    data,
		StartPK: startPK,
		EndPK:   endPK,
		Mark:    mark,
	}

	select {
	case cs.batchChan <- task:
		return nil
	default:
		return fmt.Errorf("batch channel is full, batch %d rejected", batchID)
	}
}

// WaitForCompletion 等待所有任务完成
func (cs *ChannelSync) WaitForCompletion(ctx context.Context) error {
	// 关闭batch channel，通知worker结束
	close(cs.batchChan)

	// 等待所有worker完成
	completedWorkers := 0
	for completedWorkers < cs.workerCount {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-cs.resultChan:
			if err != nil {
				return err // 返回第一个错误
			}
			completedWorkers++
		}
	}

	return nil
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
func (cse *ChannelSyncExecutor) ExecuteFullSyncChannel(ctx context.Context, task *taskEntity.SyncTask, sourceSchema, targetSchema, tableName string, identity *entity.TableIdentity, intraWorkers, readLimit, txCommitEveryN int) error {
	taskID := task.Config.ID

	// 创建channel同步器
	channelSync := NewChannelSync(intraWorkers, readLimit)

	// 获取总行数用于进度跟踪
	// var totalRows int64
	// 这里需要实际的数据库查询，暂时跳过
	// if err := cse.sourceDB.QueryRowContext(ctx,
	// 	fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", sourceSchema, tableName)).Scan(&totalRows); err == nil {
	// 	channelSync.SetTotalRows(totalRows)
	// 	cse.updateTaskTotalRows(taskID, totalRows)
	// }

	// 创建reader
	rdr := reader.NewRangeShardingReader(cse.sourceDB, sourceSchema, tableName, identity)

	// 启动workers
	channelSync.StartWorkers(ctx, func(batchTask *BatchTask) error {
		return cse.processBatchTask(ctx, task, batchTask, sourceSchema, targetSchema, tableName, identity, txCommitEveryN)
	})

	// 启动数据分发器
	dispatchErr := make(chan error, 1)
	go func() {
		batchID := 0
		lastPK := interface{}(nil)

		for {
			if cse.isTaskStopped(taskID) {
				return
			}

			// 读取下一批数据
			batch, err := rdr.ReadBatchByKeys(ctx, lastPK, int64(readLimit))
			if err != nil {
				dispatchErr <- fmt.Errorf("read batch failed: %v", err)
				return
			}

			if len(batch) == 0 {
				log.Printf("[Task %s] Channel sync: reached end of data for %s.%s", taskID, sourceSchema, tableName)
				break
			}

			firstPK := batch[0][identity.IdentifyCols[0]]
			lastPK = batch[len(batch)-1][identity.IdentifyCols[0]]
			mark := fmt.Sprintf("%s.%s:batch%d", sourceSchema, tableName, batchID)

			// 添加批次到channel
			err = channelSync.AddBatch(batchID, batch, firstPK, lastPK, mark)
			if err != nil {
				log.Printf("[Task %s] Warning: %v", taskID, err)
				// 如果channel满了，等待一下再重试
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Millisecond * 100):
					err = channelSync.AddBatch(batchID, batch, firstPK, lastPK, mark)
					if err != nil {
						dispatchErr <- fmt.Errorf("add batch failed: %v", err)
						return
					}
				}
			}

			batchID++

			// 更新进度
			cse.incrementTaskProgress(taskID, int64(len(batch)), mark)
		}
	}()

	// 等待分发完成
	select {
	case err := <-dispatchErr:
		if err != nil {
			return fmt.Errorf("dispatch failed: %v", err)
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	// 等待所有worker完成
	err := channelSync.WaitForCompletion(ctx)
	if err != nil {
		return fmt.Errorf("channel sync failed: %v", err)
	}

	// 更新最终进度
	processed, total := channelSync.GetProgress()
	log.Printf("[Task %s] Channel sync completed for %s.%s: %d/%d rows processed", taskID, sourceSchema, tableName, processed, total)

	return nil
}

// processBatchTask 处理单个批次任务
func (cse *ChannelSyncExecutor) processBatchTask(ctx context.Context, task *taskEntity.SyncTask, batchTask *BatchTask, sourceSchema, targetSchema, tableName string, identity *entity.TableIdentity, txCommitEveryN int) error {
	taskID := task.Config.ID

	// 获取数据库连接
	conn, err := cse.targetDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("worker %d failed to get connection: %v", batchTask.WorkerID, err)
	}
	defer conn.Close()

	// 设置会话参数
	conn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS=0, UNIQUE_CHECKS=0")
	defer conn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS=1, UNIQUE_CHECKS=1")

	// 处理批次写入
	var curTx *sql.Tx
	var txW *writer.BatchWriter
	var txBatchN int
	var txStartMark string

	defer func() {
		if curTx != nil {
			curTx.Rollback()
		}
	}()

	// 写入函数
	doWrite := func(rows []map[string]interface{}, mark string) error {
		if curTx == nil {
			var e error
			curTx, e = conn.BeginTx(ctx, nil)
			if e != nil {
				return fmt.Errorf("worker %d begin tx at %s: %v", batchTask.WorkerID, mark, e)
			}
			txW = writer.NewBatchWriterWithTx(curTx, identity, task.Config.BatchSize, targetSchema)
			if cse.auditLogger != nil {
				// txW.SetAuditLogger(cse.auditLogger, taskID, sourceSchema, tableName)
			}
			txBatchN = 0
			txStartMark = mark
		}

		if e := txW.WriteBatch(ctx, rows); e != nil {
			curTx.Rollback()
			curTx = nil
			return fmt.Errorf("worker %d write at %s (tx from %s) rolled back: %v", batchTask.WorkerID, mark, txStartMark, e)
		}

		txBatchN++
		if txBatchN >= txCommitEveryN {
			if e := curTx.Commit(); e != nil {
				curTx = nil
				return fmt.Errorf("worker %d commit at %s (tx from %s): %v", batchTask.WorkerID, mark, txStartMark, e)
			}
			curTx = nil
		}

		return nil
	}

	// 写入批次数据
	log.Printf("[Task %s] Worker %d processing batch %d: %d rows (PK %v -> %v)",
		taskID, batchTask.WorkerID, batchTask.BatchID, len(batchTask.Data), batchTask.StartPK, batchTask.EndPK)

	if err := doWrite(batchTask.Data, batchTask.Mark); err != nil {
		return fmt.Errorf("worker %d failed to write batch %d: %v", batchTask.WorkerID, batchTask.BatchID, err)
	}

	// 提交当前事务
	if curTx != nil {
		if err := curTx.Commit(); err != nil {
			return fmt.Errorf("worker %d final commit failed: %v", batchTask.WorkerID, err)
		}
		curTx = nil
	}

	return nil
}
