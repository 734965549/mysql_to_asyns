package fullload

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// 用户/服务侧取消原因，供 context.WithCancelCause 传播并在上层恢复。
var (
	ErrUserPaused      = errors.New("full sync paused by user")
	ErrUserStopped     = errors.New("full sync stopped by user")
	ErrServiceShutdown = errors.New("service shutdown")
)

func isCancelError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// selectPipelineError 在 reader/writer 并发结束时仲裁首个真实错误。
// 优先级：writer 非取消 > reader 非取消 > 父/流水线 context cause > writer 取消 > reader 取消。
func selectPipelineError(parentCtx, pipelineCtx context.Context, readerErr, writerErr error) error {
	if writerErr != nil && !isCancelError(writerErr) {
		return writerErr
	}
	if readerErr != nil && !isCancelError(readerErr) {
		return readerErr
	}
	if cause := context.Cause(parentCtx); cause != nil {
		return cause
	}
	if cause := context.Cause(pipelineCtx); cause != nil {
		return cause
	}
	if writerErr != nil {
		return writerErr
	}
	if readerErr != nil {
		return readerErr
	}
	if parentCtx.Err() != nil {
		return parentCtx.Err()
	}
	if pipelineCtx.Err() != nil {
		return pipelineCtx.Err()
	}
	return nil
}

func formatPipelineErr(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

// ReadQueryTimeoutError 表示源端查询超时错误，包含完整上下文用于日志和重试判断。
type ReadQueryTimeoutError struct {
	Schema  string
	Table   string
	ChunkID string
	Phase   string // "keyset" | "stream" | "pk_probe" | "payload_fetch"
	Cursor  []any
	Start   []any
	End     []any
	Timeout time.Duration
	Elapsed time.Duration
}

func (e *ReadQueryTimeoutError) Error() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("source query timeout after %s (limit %s): ", e.Elapsed, e.Timeout))
	sb.WriteString(fmt.Sprintf("table=%s.%s", e.Schema, e.Table))
	if e.ChunkID != "" {
		sb.WriteString(fmt.Sprintf(" chunk=%s", e.ChunkID))
	}
	if e.Phase != "" {
		sb.WriteString(fmt.Sprintf(" phase=%s", e.Phase))
	}
	if len(e.Cursor) > 0 {
		sb.WriteString(fmt.Sprintf(" cursor=%v", e.Cursor))
	}
	if len(e.Start) > 0 {
		sb.WriteString(fmt.Sprintf(" start=%v", e.Start))
	}
	if len(e.End) > 0 {
		sb.WriteString(fmt.Sprintf(" end=%v", e.End))
	}
	return sb.String()
}

// IsReadQueryTimeout 判断错误是否为源端查询超时（支持 fmt.Errorf %w 包装）。
func IsReadQueryTimeout(err error) bool {
	var e *ReadQueryTimeoutError
	return errors.As(err, &e)
}
