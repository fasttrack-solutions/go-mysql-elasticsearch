package api

import (
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/go-zoo/bone"
	"gopkg.in/oauth2.v3/server"
)

// TimeTracker defines time tracker interface.
type TimeTracker interface {
	Avg() time.Duration
	ThirdsDiff() int
	GetDurations() []time.Duration
}

// API contains HTTP server's settings.
type API struct {
	port      int
	listener  net.Listener
	mux       *bone.Mux
	oauthSrv  *server.Server
	authUser  string
	authPass  string
	authToken string
	tt        TimeTracker
}

// New returns new API.
func New(port int, tt TimeTracker) *API {
	return &API{
		port: port,
		tt:   tt,
	}
}

func (a *API) defineMux() error {
	a.mux = bone.New()

	a.mux.Get("/timetracker", http.HandlerFunc(a.statsHandler))

	return nil
}

// Start starts the HTTP server.
func (a *API) Start() (err error) {
	err = a.defineMux()
	if err != nil {
		return err
	}

	a.listener, err = net.Listen("tcp", ":"+strconv.Itoa(a.port))
	if err != nil {
		return
	}

	go http.Serve(a.listener, a.mux)

	return
}

// Stop stops the server.
func (a *API) Stop() {
	a.listener.Close()
}
