package queue_to_s3_sink

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

var (
	startTime        = time.Date(2026, time.February, 28, 10, 9, 8, 7, time.UTC)
	rotationInterval = time.Hour
)

func TestDefaultRotator(t *testing.T) {
	firstItem := abstract.MakeRawMessage(
		[]byte("stub"),
		topic,
		startTime,
		topic,
		partition,
		int64(0),
		[]byte("stub"),
	)
	rotator := NewDefaultRotator(rotationInterval)

	require.True(t, rotator.ShouldRotate(&firstItem)) // First ShouldRotate is always true
	require.NoError(t, rotator.UpdateState(&firstItem))
	require.Equal(t, startTime.Add(rotationInterval), rotator.nextRotate)

	secondItem := abstract.MakeRawMessage(
		[]byte("stub"),
		topic,
		startTime.Add(rotationInterval-time.Nanosecond),
		topic,
		partition,
		int64(0),
		[]byte("stub"),
	)
	require.False(t, rotator.ShouldRotate(&secondItem))

	lastItem := abstract.MakeRawMessage(
		[]byte("stub"),
		topic,
		startTime.Add(rotationInterval),
		topic,
		partition,
		int64(0),
		[]byte("stub"),
	)
	require.True(t, rotator.ShouldRotate(&lastItem))
	require.NoError(t, rotator.UpdateState(&lastItem))
	require.Equal(t, startTime.Add(rotationInterval*2), rotator.nextRotate)
}
