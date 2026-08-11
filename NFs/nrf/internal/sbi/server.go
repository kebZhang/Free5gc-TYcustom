package sbi

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nrf/internal/accesslog"
	"github.com/free5gc/nrf/internal/logger"
	"github.com/free5gc/nrf/internal/sbi/processor"
	"github.com/free5gc/nrf/internal/util"
	"github.com/free5gc/nrf/pkg/app"
	"github.com/free5gc/nrf/pkg/factory"
	"github.com/free5gc/openapi/models"
	logger_util "github.com/free5gc/util/logger"
	"github.com/free5gc/util/metrics"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type ServerNrf interface {
	app.App

	// Consumer() *consumer.Consumer
	Processor() *processor.Processor
}

type Server struct {
	ServerNrf

	httpServer *http.Server
	router     *gin.Engine
}

func NewServer(nrf ServerNrf, tlsKeyLogPath string) (*Server, error) {
	s := &Server{
		ServerNrf: nrf,
		router:    logger_util.NewGinWithLogrus(logger.GinLog),
	}
	s.router.Use(metrics.InboundMetrics())
	s.router.Use(accesslog.InboundLogger())
	cfg := s.Config()
	bindAddr := cfg.GetSbiBindingAddr()
	logger.SBILog.Infof("Binding addr: [%s]", bindAddr)

	s.applyService()

	var err error
	if s.httpServer, err = newHttp2ServerWithIdleTimeout(bindAddr, tlsKeyLogPath, s.router); err != nil {
		logger.InitLog.Errorf("Initialize HTTP server failed: %v", err)
		return nil, err
	}
	return s, nil
}

func (s *Server) GetLocalIp() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		logger.NfmLog.Error(err)
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

func (s *Server) applyService() {
	accesstokenRoutes := s.getAccesstokenRoutes()
	accesstokenGroup := s.router.Group("") // accesstoken service didn't have api prefix
	applyRoutes(accesstokenGroup, accesstokenRoutes)

	bootstrappingRoutes := s.getBootstrappingRoutes()
	bootstrappingGroup := s.router.Group(factory.NrfBootstrappingPrefix)
	applyRoutes(bootstrappingGroup, bootstrappingRoutes)

	discoveryRoutes := s.getNfDiscoveryRoutes()
	discoveryGroup := s.router.Group(factory.NrfDiscResUriPrefix)
	discAuthCheck := util.NewRouterAuthorizationCheck(models.ServiceName_NNRF_DISC)
	discoveryGroup.Use(func(c *gin.Context) {
		discAuthCheck.Check(c, s.Context())
	})
	applyRoutes(discoveryGroup, discoveryRoutes)

	// OAuth2 must exclude NfRegister
	nfRegisterRoute := s.getNfRegisterRoute()
	nfRegisterGroup := s.router.Group(factory.NrfNfmResUriPrefix)
	applyRoutes(nfRegisterGroup, nfRegisterRoute)

	managementRoutes := s.getNfManagementRoute()
	managementGroup := s.router.Group(factory.NrfNfmResUriPrefix)
	managementAuthCheck := util.NewRouterAuthorizationCheck(models.ServiceName_NNRF_NFM)
	managementGroup.Use(func(c *gin.Context) {
		managementAuthCheck.Check(c, s.Context())
	})
	applyRoutes(managementGroup, managementRoutes)
}

func (s *Server) Run(wg *sync.WaitGroup) error {
	wg.Add(1)
	go s.startServer(wg)

	logger.SBILog.Infoln("SBI server started")
	return nil
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

	cfg := s.Config()
	serverScheme := cfg.GetSbiScheme()

	var err error
	switch serverScheme {
	case "http":
		err = s.httpServer.ListenAndServe()
	case "https":
		// TODO: support TLS mutual authentication for OAuth
		err = s.httpServer.ListenAndServeTLS(
			cfg.GetNrfCertPemPath(),
			cfg.GetNrfPrivKeyPath())
	default:
		err = fmt.Errorf("not support this scheme[%s]", serverScheme)
	}

	if err != nil && err != http.ErrServerClosed {
		logger.SBILog.Errorf("SBI server error: %v", err)
	}
	logger.SBILog.Infof("SBI server (listen on %s) stopped", s.httpServer.Addr)
}

func (s *Server) Stop() {
	// server stop
	const defaultShutdownTimeout time.Duration = 2 * time.Second

	toCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer cancel()
	if err := s.httpServer.Shutdown(toCtx); err != nil {
		logger.SBILog.Errorf("Could not close SBI server: %#v", err)
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
