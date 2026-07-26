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

func TestPreferPipelineStopCause_UserPauseOverridesPlainCancel(t *testing.T) {
	got := preferPipelineStopCause(context.Canceled, func() error { return ErrUserPaused })
	if !errors.Is(got, ErrUserPaused) {
		t.Fatalf("expected ErrUserPaused, got %v", got)
	}
}

func TestPreferPipelineStopCause_DoesNotMaskRealError(t *testing.T) {
	writerErr := errors.New("writer batch failed")
	got := preferPipelineStopCause(writerErr, func() error { return ErrUserPaused })
	if !errors.Is(got, writerErr) {
		t.Fatalf("expected writer error, got %v", got)
	}
}

func TestPreferPipelineStopCause_DoesNotMaskSchemaLockLost(t *testing.T) {
	got := preferPipelineStopCause(ErrSchemaLockLost, func() error { return ErrUserPaused })
	if !errors.Is(got, ErrSchemaLockLost) {
		t.Fatalf("expected ErrSchemaLockLost, got %v", got)
	}
}

func TestCurrentEngineStopCause_PreservesWideStopSemantics(t *testing.T) {
	got := currentEngineStopCause(func() error { return nil }, func() bool { return true })
	if !errors.Is(got, context.Canceled) {
		t.Fatalf("expected generic cancellation for non-user stop, got %v", got)
	}
}

func TestCurrentEngineStopCause_PrefersNamedStop(t *testing.T) {
	got := currentEngineStopCause(func() error { return ErrUserStopped }, func() bool { return true })
	if !errors.Is(got, ErrUserStopped) {
		t.Fatalf("expected ErrUserStopped, got %v", got)
	}
}

func TestIsReadQueryTimeoutStillWorks(t *testing.T) {
	timeoutErr := &ReadQueryTimeoutError{Schema: "s", Table: "t", Timeout: 0}
	wrapped := fmt.Errorf("wrapped: %w", timeoutErr)
	if !IsReadQueryTimeout(wrapped) {
		t.Fatal("expected IsReadQueryTimeout true for wrapped timeout error")
	}
}
