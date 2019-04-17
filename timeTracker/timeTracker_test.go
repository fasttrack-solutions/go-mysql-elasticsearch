package ttracker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimeTracker(t *testing.T) {
	tt := New(5)

	tt.Add(time.Second)
	require.Equal(t, time.Millisecond*200, tt.Avg())

	tt.Add(time.Second)
	require.Equal(t, time.Millisecond*400, tt.Avg())

	tt.Add(time.Second)
	require.Equal(t, time.Millisecond*600, tt.Avg())
}

func TestThirdsDiff(t *testing.T) {
	tt := New(6)

	// Test determination of increased latency.
	tt.durations = []time.Duration{
		time.Second * 2,
		time.Second * 2,
		time.Second * 2,
		time.Second,
		time.Second,
		time.Second,
		time.Second * 3,
		time.Second * 3,
		time.Second * 3,
	}

	require.Equal(t, 50, tt.ThirdsDiff())

	// Test determination of decreased latency.
	tt.durations = []time.Duration{
		time.Second * 3,
		time.Second * 3,
		time.Second * 3,
		time.Second,
		time.Second,
		time.Second,
		time.Second * 2,
		time.Second * 2,
		time.Second * 2,
	}

	require.Equal(t, -34, tt.ThirdsDiff())

	// Test unchanged latency.

	tt.durations = []time.Duration{
		time.Second,
		time.Second,
		time.Second,
		time.Second,
		time.Second,
		time.Second,
		time.Second,
		time.Second,
		time.Second,
	}

	require.Equal(t, 0, tt.ThirdsDiff())
}
