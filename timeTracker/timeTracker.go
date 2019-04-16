package ttracker

import (
	"sync"
	"time"
)

// TimeTracker contains time tracker data.
type TimeTracker struct {
	mu        sync.Mutex
	durations []time.Duration
}

// New returns new TimeTracker.
func New(n int) *TimeTracker {
	return &TimeTracker{
		durations: make([]time.Duration, n),
	}
}

// Add adds a new duration record to the time tracker for ater processing.
func (t *TimeTracker) Add(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.durations = append([]time.Duration{d}, t.durations[:len(t.durations)-1]...)
}

// Avg returns an average duration of all records accumulated so far.
func (t *TimeTracker) Avg() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()

	return avg(t.durations)
}

// ThirdsDiff returns the difference of averages in a form of per cents between
// the first and the last thirds of the tracker history.
func (t *TimeTracker) ThirdsDiff() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	thirds := splitSlice(t.durations, 3)
	if len(thirds) != 3 {
		return 0
	}

	oldAvg := avg(thirds[0])
	newAvg := avg(thirds[2])

	q := int(newAvg) * 100 / int(oldAvg)

	if newAvg > oldAvg {
		return q - 100
	}

	return -(100 - q)
}

// GetDurations returns all stored durations.
func (t *TimeTracker) GetDurations() []time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.durations
}

func avg(sl []time.Duration) time.Duration {
	var sum time.Duration
	for _, v := range sl {
		sum += v
	}

	return sum / time.Duration(len(sl))
}

func splitSlice(sl []time.Duration, limit int) (res [][]time.Duration) {
	if len(sl) > limit {
		for i := 0; i < len(sl); i += limit {
			if i+limit > len(sl) {
				res = append(res, sl[i:])
				break
			}
			res = append(res, sl[i:i+limit])
		}

		return
	}

	res = [][]time.Duration{sl}
	return
}
