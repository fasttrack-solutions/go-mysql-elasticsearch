package api

import (
	"net/http"
	"time"
)

type statsResp struct {
	RecentDurations []time.Duration `json:"recentDurations"`
	AverageLatency  time.Duration   `json:"averageLatency"`
	ThirdsDiff      int             `json:"thirdsDiff"`
}

func (a *API) statsHandler(w http.ResponseWriter, r *http.Request) {
	resp := statsResp{
		RecentDurations: a.tt.GetDurations(),
		AverageLatency:  a.tt.Avg(),
		ThirdsDiff:      a.tt.ThirdsDiff(),
	}

	respond(resp, http.StatusOK, w)
}
