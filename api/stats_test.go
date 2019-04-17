package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/fasttrack-solutions/go-mysql-elasticsearch/timeTracker"
	"github.com/gavv/httpexpect"
)

func TestGetStats(t *testing.T) {
	// Define test API.
	testAPI := New(0, nil)
	testAPI.defineMux()

	testServer := httptest.NewServer(testAPI.mux)
	defer testServer.Close()

	e := httpexpect.WithConfig(httpexpect.Config{
		BaseURL:  testServer.URL,
		Reporter: httpexpect.NewRequireReporter(t),
	})

	// Test 1.
	tt := ttracker.New(6)
	testAPI.tt = tt

	for i := 0; i < 6; i++ {
		tt.Add(time.Second)
	}

	e.Request(http.MethodGet, "/timetracker").
		Expect().
		Status(http.StatusOK).
		JSON().
		Equal(map[string]interface{}{
			"recentDurations": tt.GetDurations(),
			"averageLatency":  time.Second,
			"thirdsDiff":      0,
		})

	// Test 2.
	tt = ttracker.New(18)
	testAPI.tt = tt

	for i := 0; i < 6; i++ { // fill 3/3
		tt.Add(time.Second * 3)
	}
	for i := 0; i < 6; i++ { // fill 2/3
		tt.Add(time.Second)
	}
	for i := 0; i < 6; i++ { // fill 1/3
		tt.Add(time.Second * 2)
	}

	e.Request(http.MethodGet, "/timetracker").
		Expect().
		Status(http.StatusOK).
		JSON().
		Equal(map[string]interface{}{
			"recentDurations": tt.GetDurations(),
			"averageLatency":  time.Second * 2,
			"thirdsDiff":      50,
		})

	// Test 3.
	tt = ttracker.New(11)
	testAPI.tt = tt

	for i := 0; i < 4; i++ { // fill 3/3
		tt.Add(time.Second * 3)
	}
	for i := 0; i < 3; i++ { // fill 2/3
		tt.Add(time.Second)
	}
	for i := 0; i < 4; i++ { // fill 1/3
		tt.Add(time.Second * 2)
	}

	e.Request(http.MethodGet, "/timetracker").
		Expect().
		Status(http.StatusOK).
		JSON().
		Equal(map[string]interface{}{
			"recentDurations": tt.GetDurations(),
			"averageLatency":  time.Nanosecond * 2090909090,
			"thirdsDiff":      50,
		})
}
