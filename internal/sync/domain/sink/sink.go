package sink

import (
	"context"
)

// Sink 统一目标端写入接口
// Write 成功返回才允许推进 checkpoint。
// Flush 用于批量 Sink 的主动提交。
// Close 必须幂等。
type Sink interface {
	// Type 返回 Sink 类型标识，如 "MYSQL"、"KAFKA"、"HTTP_WEBHOOK"
	Type() string
	// Open 初始化 Sink（建立连接、验证配置等）
	Open(ctx context.Context) error
	// Write 写入单条变更事件
	Write(ctx context.Context, event *ChangeEvent) error
	// Flush 刷新缓冲（批量 Sink 主动提交）
	Flush(ctx context.Context) error
	// Close 关闭 Sink 并释放资源（必须幂等）
	Close(ctx context.Context) error
}

// BatchSink 扩展接口，支持批量写入
type BatchSink interface {
	Sink
	// WriteBatch 批量写入变更事件
	WriteBatch(ctx context.Context, events []*ChangeEvent) error
}
