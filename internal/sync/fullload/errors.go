package fullload

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

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
