package entity

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateEventSeverity(t *testing.T) {
	require.NoError(t, ValidateEventSeverity(EventSeverityInfo))
	require.NoError(t, ValidateEventSeverity(EventSeverityWarn))
	require.NoError(t, ValidateEventSeverity(EventSeverityError))
	assert.Error(t, ValidateEventSeverity("debug"))
}

func TestValidateEventVisibility(t *testing.T) {
	require.NoError(t, ValidateEventVisibility(EventVisibilityKey))
	require.NoError(t, ValidateEventVisibility(EventVisibilityDiagnostic))
	assert.Error(t, ValidateEventVisibility("PUBLIC"))
}

func TestIsNeverSuppressEventCode(t *testing.T) {
	assert.True(t, IsNeverSuppressEventCode(EventCodeTaskStarted))
	assert.True(t, IsNeverSuppressEventCode(EventCodePhaseP0Captured))
	assert.True(t, IsNeverSuppressEventCode("TABLE_RETRY_EXHAUSTED"))
	assert.False(t, IsNeverSuppressEventCode("QUEUE_BACKPRESSURE_HIGH"))
}

func TestValidateTaskEvent(t *testing.T) {
	ev := &TaskEvent{
		EventID:     "e1",
		TaskID:      "t1",
		ExecutionID: "x1",
		Severity:    EventSeverityWarn,
		Visibility:  EventVisibilityKey,
		Code:        EventCodeTaskFailed,
		Message:     "boom",
	}
	require.NoError(t, ValidateTaskEvent(ev))
	ev.ExecutionID = ""
	assert.Error(t, ValidateTaskEvent(ev))
}
