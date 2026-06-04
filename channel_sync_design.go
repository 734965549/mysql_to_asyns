package main

import (
	"context"
	"fmt"
	"sync/atomic"

	"mysql-to-async/pkg/logger"
)

// BatchTask 批次任务结构
type BatchTask struct {
	BatchID  int                      // 批次ID
	Data     []map[string]interface{} // 数据批次
	StartPK  interface{}              // 起始主键
	EndPK    interface{}              // 结束主键
	WorkerID int                      // 处理的worker ID
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
				}
			}
		}(i)
	}
}

// AddBatch 添加批次任务
func (cs *ChannelSync) AddBatch(batchID int, data []map[string]interface{}, startPK, endPK interface{}) error {
	task := &BatchTask{
		BatchID: batchID,
		Data:    data,
		StartPK: startPK,
		EndPK:   endPK,
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
	completed := make(chan struct{})
	go func() {
		for i := 0; i < cs.workerCount; i++ {
			<-cs.resultChan // 等待每个worker的完成信号
		}
		close(completed)
	}()

	select {
	case <-completed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case err := <-cs.resultChan:
		return err // 返回第一个错误
	}
}

// GetProgress 获取处理进度
func (cs *ChannelSync) GetProgress() (int64, int64) {
	return atomic.LoadInt64(&cs.processed), cs.totalRows
}

// 使用示例
func ExampleChannelSync() {
	ctx := context.Background()
	sync := NewChannelSync(16, 1000) // 16个worker，每批1000条

	// 启动workers
	sync.StartWorkers(ctx, func(task *BatchTask) error {
		logger.Info("Worker %d processing batch %d: %d rows (PK %v -> %v)",
			task.WorkerID, task.BatchID, len(task.Data), task.StartPK, task.EndPK)

		// 这里执行实际的数据库写入操作
		// writer.WriteBatch(task.Data)

		return nil
	})

	// 模拟数据分发
	batchID := 0
	lastPK := interface{}(int64(0))

	for {
		// 从源数据库读取下一批数据
		data, nextPK, err := readNextBatch(lastPK, 1000)
		if err != nil {
			logger.Error("Read error: %v", err)
			break
		}

		if len(data) == 0 {
			break // 没有更多数据
		}

		// 添加到channel
		err = sync.AddBatch(batchID, data, lastPK, nextPK)
		if err != nil {
			logger.Error("Add batch error: %v", err)
			continue
		}

		lastPK = nextPK
		batchID++
	}

	// 等待完成
	err := sync.WaitForCompletion(ctx)
	if err != nil {
		logger.Error("Sync failed: %v", err)
		return
	}

	processed, total := sync.GetProgress()
	logger.Info("Sync completed: %d/%d rows processed", processed, total)
}

// 模拟数据读取函数
func readNextBatch(lastPK interface{}, batchSize int) ([]map[string]interface{}, interface{}, error) {
	// 这里实现实际的数据读取逻辑
	// 使用 ReadBatchByKeys 或其他读取方法

	// 模拟返回
	if lastPK == nil {
		lastPK = int64(0)
	}

	currentPK := lastPK.(int64)
	if currentPK > 10000 {
		return []map[string]interface{}{}, nil, nil // 模拟结束
	}

	data := make([]map[string]interface{}, batchSize)
	for i := 0; i < batchSize; i++ {
		data[i] = map[string]interface{}{
			"id":   currentPK + int64(i) + 1,
			"name": fmt.Sprintf("record_%d", currentPK+int64(i)+1),
		}
	}

	nextPK := currentPK + int64(batchSize)
	return data, nextPK, nil
}
