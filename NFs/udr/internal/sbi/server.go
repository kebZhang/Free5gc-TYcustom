package sbi

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/openapi/models"
	"github.com/free5gc/udr/internal/accesslog"
	"github.com/free5gc/udr/internal/logger"
	"github.com/free5gc/udr/internal/sbi/processor"
	"github.com/free5gc/udr/internal/util"
	"github.com/free5gc/udr/pkg/app"
	"github.com/free5gc/udr/pkg/factory"
	logger_util "github.com/free5gc/util/logger"
	"github.com/free5gc/util/metrics"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type Server struct {
	UDR

	httpServer *http.Server
	router     *gin.Engine
}

type UDR interface {
	app.App

	Processor() *processor.Processor
}

func NewServer(udr UDR, tlsKeyLogPath string) *Server {
	s := &Server{
		UDR: udr,
	}

	s.router = newRouter(s)
	server, err := bindRouter(udr, s.router, tlsKeyLogPath)
	s.httpServer = server

	if err != nil {
		logger.SBILog.Errorf("bind Router Error: %+v", err)
		panic("Server initialization failed")
	}

	return s
}

func (s *Server) Run(wg *sync.WaitGroup) {
	logger.SBILog.Info("Starting server...")

	wg.Add(1)
	go func() {
		defer wg.Done()

		err := s.serve()
		if err != http.ErrServerClosed {
			logger.SBILog.Panicf("HTTP server setup failed: %+v", err)
		}
		logger.SBILog.Infof("SBI server (listen on %s) stopped", s.httpServer.Addr)
	}()
}

func (s *Server) Shutdown() {
	s.shutdownHttpServer()
}

func (s *Server) shutdownHttpServer() {
	const shutdownTimeout time.Duration = 2 * time.Second

	if s.httpServer == nil {
		return
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	err := s.httpServer.Shutdown(shutdownCtx)
	if err != nil {
		logger.SBILog.Errorf("HTTP server shutdown failed: %+v", err)
	}
}

func bindRouter(udr app.App, router *gin.Engine, tlsKeyLogPath string) (*http.Server, error) {
	sbiConfig := udr.Config().Configuration.Sbi
	bindAddr := fmt.Sprintf("%s:%d", sbiConfig.BindingIPv4, sbiConfig.Port)

	return newHttp2ServerWithIdleTimeout(bindAddr, tlsKeyLogPath, router)
}

func newRouter(s *Server) *gin.Engine {
	router := logger_util.NewGinWithLogrus(logger.GinLog)
	router.Use(metrics.InboundMetrics())
	router.Use(accesslog.InboundLogger())

	dataRepositoryGroup := router.Group(factory.UdrDrResUriPrefix)
	dataRepositoryGroup.Use(func(c *gin.Context) {
		util.NewRouterAuthorizationCheck(models.ServiceName_NUDR_DR).Check(c, s.Context())
	})
	dataRepositoryRoutes := s.getDataRepositoryRoutes()
	AddService(dataRepositoryGroup, dataRepositoryRoutes)

	groupIdGroup := router.Group(factory.UdrGroupIdResUriPrefix)
	groupIdGroup.Use(func(c *gin.Context) {
		util.NewRouterAuthorizationCheck(models.ServiceName_NUDR_GROUP_ID_MAP).Check(c, s.Context())
	})
	groupIdRoutes := s.getGroupIdMap()
	AddService(groupIdGroup, groupIdRoutes)

	imsSDM := router.Group(factory.HSSIsmSDMUriPrefix)
	imsSDM.Use(func(c *gin.Context) {
		util.NewRouterAuthorizationCheck(models.ServiceName_NHSS_IMS_SDM).Check(c, s.Context())
	})
	imsSDMRoutes := s.getImsSDMRoutes()
	AddService(imsSDM, imsSDMRoutes)

	return router
}

func (s *Server) unsecureServe() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server) secureServe() error {
	sbiConfig := s.UDR.Config()

	pemPath := sbiConfig.GetCertPemPath()
	if pemPath == "" {
		pemPath = factory.UdrDefaultCertPemPath
	}

	keyPath := sbiConfig.GetCertKeyPath()
	if keyPath == "" {
		keyPath = factory.UdrDefaultPrivateKeyPath
	}

	return s.httpServer.ListenAndServeTLS(pemPath, keyPath)
}

func (s *Server) serve() error {
	sbiConfig := s.UDR.Config().Configuration.Sbi

	switch sbiConfig.Scheme {
	case "http":
		return s.unsecureServe()
	case "https":
		return s.secureServe()
	default:
		return fmt.Errorf("invalid SBI scheme: %s", sbiConfig.Scheme)
	}
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
