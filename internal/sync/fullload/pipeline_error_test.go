package fullload

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestSelectPipelineError_WriterFailsFirst(t *testing.T) {
	parent := context.Background()
	pipeline, pipelineCancel := context.WithCancelCause(parent)
	defer pipelineCancel(nil)
	writerErr := fmt.Errorf("writer batch failed")
	readerErr := context.Canceled

	got := selectPipelineError(parent, pipeline, readerErr, writerErr)
	if !errors.Is(got, writerErr) && got.Error() != writerErr.Error() {
		t.Fatalf("expected writer error, got %v", got)
	}
}

func TestSelectPipelineError_ReaderFailsFirst(t *testing.T) {
	parent := context.Background()
	pipeline, pipelineCancel := context.WithCancelCause(parent)
	defer pipelineCancel(nil)
	readerErr := fmt.Errorf("reader query failed")
	writerErr := context.Canceled

	got := selectPipelineError(parent, pipeline, readerErr, writerErr)
	if got == nil || got.Error() != readerErr.Error() {
		t.Fatalf("expected reader error, got %v", got)
	}
}

func TestSelectPipelineError_SchemaLockLostCause(t *testing.T) {
	parent, parentCancel := context.WithCancelCause(context.Background())
	defer parentCancel(nil)
	parentCancel(ErrSchemaLockLost)
	pipeline, pipelineCancel := context.WithCancelCause(parent)
	defer pipelineCancel(nil)

	got := selectPipelineError(parent, pipeline, context.Canceled, context.Canceled)
	if !errors.Is(got, ErrSchemaLockLost) {
		t.Fatalf("expected ErrSchemaLockLost, got %v", got)
	}
}

func TestSelectPipelineError_UserPausedCause(t *testing.T) {
	parent := context.Background()
	pipeline, pipelineCancel := context.WithCancelCause(parent)
	pipelineCancel(ErrUserPaused)

	got := selectPipelineError(parent, pipeline, context.Canceled, context.Canceled)
	if !errors.Is(got, ErrUserPaused) {
		t.Fatalf("expected ErrUserPaused, got %v", got)
	}
}

func TestIsReadQueryTimeoutStillWorks(t *testing.T) {
	timeoutErr := &ReadQueryTimeoutError{Schema: "s", Table: "t", Timeout: 0}
	wrapped := fmt.Errorf("wrapped: %w", timeoutErr)
	if !IsReadQueryTimeout(wrapped) {
		t.Fatal("expected IsReadQueryTimeout true for wrapped timeout error")
	}
}
