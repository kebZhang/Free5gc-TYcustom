package sbi

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/free5gc/nef/internal/logger"
	"github.com/free5gc/nef/internal/sbi/processor"
	"github.com/free5gc/nef/pkg/app"
	"github.com/free5gc/nef/pkg/factory"
	logger_util "github.com/free5gc/util/logger"
	"github.com/free5gc/util/metrics"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const (
	CorsConfigMaxAge = 86400
)

type nef interface {
	app.App
	Processor() *processor.Processor
}

type Server struct {
	nef

	httpServer *http.Server
	router     *gin.Engine
}

func NewServer(nef nef, tlsKeyLogPath string) (*Server, error) {
	s := &Server{
		nef: nef,
	}

	s.router = logger_util.NewGinWithLogrus(logger.GinLog)

	s.router.Use(metrics.InboundMetrics())

	endpoints := s.getTrafficInfluenceRoutes()
	group := s.router.Group(factory.TraffInfluResUriPrefix)
	applyRoutes(group, endpoints)

	endpoints = s.getPFDManagementRoutes()
	group = s.router.Group(factory.PfdMngResUriPrefix)
	applyRoutes(group, endpoints)

	endpoints = s.getPFDFRoutes()
	group = s.router.Group(factory.NefPfdMngResUriPrefix)
	applyRoutes(group, endpoints)

	endpoints = s.getOamRoutes()
	group = s.router.Group(factory.NefOamResUriPrefix)
	applyRoutes(group, endpoints)

	endpoints = s.getCallbackRoutes()
	group = s.router.Group(factory.NefCallbackResUriPrefix)
	applyRoutes(group, endpoints)

	s.router.Use(cors.New(cors.Config{
		AllowMethods: []string{"GET", "POST", "OPTIONS", "PUT", "PATCH", "DELETE"},
		AllowHeaders: []string{
			"Origin", "Content-Length", "Content-Type", "User-Agent",
			"Referrer", "Host", "Token", "X-Requested-With",
		},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		AllowAllOrigins:  true,
		MaxAge:           CorsConfigMaxAge,
	}))

	bindAddr := s.Config().SbiBindingAddr()
	logger.SBILog.Infof("Binding addr: [%s]", bindAddr)
	var err error
	if s.httpServer, err = newHttp2ServerWithIdleTimeout(bindAddr, tlsKeyLogPath, s.router); err != nil {
		logger.InitLog.Errorf("Initialize HTTP server failed: %+v", err)
		return nil, err
	}

	return s, nil
}

func (s *Server) Run(wg *sync.WaitGroup) error {
	wg.Add(1)
	go s.startServer(wg)
	return nil
}

func (s *Server) Terminate() {
	const defaultShutdownTimeout time.Duration = 2 * time.Second

	if s.httpServer != nil {
		logger.SBILog.Infof("Stop SBI server (listen on %s)", s.httpServer.Addr)
		toCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
		defer cancel()
		if err := s.httpServer.Shutdown(toCtx); err != nil {
			logger.SBILog.Errorf("Could not close SBI server: %#v", err)
		}
	}
}

func (s *Server) startServer(wg *sync.WaitGroup) {
	defer func() {
		if p := recover(); p != nil {
			// Print stack for panic to log. Fatalf() will let program exit.
			logger.SBILog.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
			s.Terminate()
		}

		wg.Done()
	}()

	logger.SBILog.Infof("Start SBI server (listen on %s)", s.httpServer.Addr)

	var err error

	scheme := s.Config().SbiScheme()
	switch scheme {
	case "http":
		err = s.httpServer.ListenAndServe()
	case "https":
		// TODO: use config file to config path
		err = s.httpServer.ListenAndServeTLS(s.Config().GetCertPemPath(), s.Config().GetCertKeyPath())
	default:
		err = fmt.Errorf("scheme [%s] is not supported", scheme)
	}

	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.SBILog.Errorf("SBI server error: %+v", err)
	}
	logger.SBILog.Warnf("SBI server (listen on %s) stopped", s.httpServer.Addr)
}

// idleTimeoutPeriod replaces the 1ms that free5gc's httpwrapper.NewHttp2Server
// hard-codes into http2.Server.IdleTimeout.
//
// IdleTimeout is how long a connection may sit with no active stream before the
// server sends GOAWAY and closes it. Upstream sets 1ms behind a standing
// "TODO: extends the idle time after re-use openapi client" -- a leftover from
// when free5gc clients did not reuse connections at all and the server needed
// to reap dead ones aggressively. Clients reuse now (accesslog.Client() is a
// singleton), but the 1ms stayed.
//
// Measured with RQ5/UE10 (C6525100g_NF2HTTPconn_0806v2): every NF pair got
// exactly 1.0 requests per socket -- 84 requests over UDM->UDR opened 84 TCP
// connections, conn_reused false every time. The smallest observed gap between
// two consecutive requests was 1.4ms, i.e. only 0.4ms over the limit, which was
// enough to have the connection torn down between them.
//
// 500ms covers the median gap of every pair measured (2.3ms to 199.6ms) with
// room to spare, while still reaping genuinely idle connections far sooner than
// Go's own default of 0 (never).
const idleTimeoutPeriod = 500 * time.Millisecond

// newHttp2ServerWithIdleTimeout mirrors httpwrapper.NewHttp2Server exactly,
// apart from idleTimeoutPeriod above. It is duplicated per NF rather than
// shared because each NF is its own Go module with no common internal package.
//
// Like upstream, h2Server is wired in only through h2c.NewHandler, which covers
// cleartext HTTP/2. Under ListenAndServeTLS the h2c handler is bypassed
// entirely, so this IdleTimeout would not apply there and the connection would
// fall back to whatever ALPN negotiates. Every NF in config/ runs
// "scheme: http", so that path is unused in this deployment. If https is ever
// enabled, this function must also call http2.ConfigureServer(server, h2Server)
// AFTER setting server.TLSConfig -- assigning TLSConfig wholesale would
// otherwise discard the NextProtos that ConfigureServer installs.
func newHttp2ServerWithIdleTimeout(
	bindAddr, preMasterSecretLogPath string, handler http.Handler,
) (*http.Server, error) {
	if handler == nil {
		return nil, errors.New("server needs handler to handle request")
	}

	h2Server := &http2.Server{
		IdleTimeout: idleTimeoutPeriod,
	}
	server := &http.Server{
		Addr:    bindAddr,
		Handler: h2c.NewHandler(handler, h2Server),
	}

	if preMasterSecretLogPath != "" {
		preMasterSecretFile, err := os.OpenFile(
			preMasterSecretLogPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			return nil, fmt.Errorf(
				"create pre-master-secret log [%s] fail: %s", preMasterSecretLogPath, err)
		}
		server.TLSConfig = &tls.Config{KeyLogWriter: preMasterSecretFile}
	}

	return server, nil
}
