package sbi

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"

	"github.com/free5gc/openapi/models"
	"github.com/free5gc/udm/internal/accesslog"
	"github.com/free5gc/udm/internal/logger"
	"github.com/free5gc/udm/internal/sbi/consumer"
	"github.com/free5gc/udm/internal/sbi/processor"
	"github.com/free5gc/udm/internal/util"
	"github.com/free5gc/udm/pkg/app"
	"github.com/free5gc/udm/pkg/factory"
	logger_util "github.com/free5gc/util/logger"
	"github.com/free5gc/util/metrics"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type ServerUdm interface {
	app.App

	Consumer() *consumer.Consumer
	Processor() *processor.Processor
	CancelContext() context.Context
}

type Server struct {
	ServerUdm

	httpServer *http.Server
	router     *gin.Engine
}

func NewServer(udm ServerUdm, tlsKeyLogPath string) (*Server, error) {
	s := &Server{
		ServerUdm: udm,
	}
	s.router = newRouter(s)

	cfg := s.Config()
	bindAddr := cfg.GetSbiBindingAddr()
	logger.SBILog.Infof("Binding addr: [%s]", bindAddr)
	var err error
	if s.httpServer, err = newHttp2ServerWithIdleTimeout(bindAddr, tlsKeyLogPath, s.router); err != nil {
		logger.InitLog.Errorf("Initialize HTTP server failed: %v", err)
		return nil, err
	}
	s.httpServer.ErrorLog = log.New(logger.SBILog.WriterLevel(logrus.ErrorLevel), "HTTP2: ", 0)

	return s, err
}

func (s *Server) Run(traceCtx context.Context, wg *sync.WaitGroup) error {
	logger.SBILog.Info("Starting server...")

	var err error
	_, s.Context().NfId, err = s.Consumer().RegisterNFInstance(s.CancelContext())
	if err != nil {
		logger.InitLog.Errorf("UDM register to NRF Error[%s]", err.Error())
	}

	wg.Add(1)
	go s.startServer(wg)

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

	logger.SBILog.Infof("Start SBI server (listen on %s)", s.httpServer.Addr)

	var err error
	cfg := s.Config()
	scheme := cfg.GetSbiScheme()
	switch s.Config().GetSbiScheme() {
	case "http":
		err = s.httpServer.ListenAndServe()
	case "https":
		err = s.httpServer.ListenAndServeTLS(
			cfg.GetCertPemPath(),
			cfg.GetCertKeyPath())
	default:
		err = fmt.Errorf("no support this scheme[%s]", scheme)
	}

	if err != nil && err != http.ErrServerClosed {
		logger.SBILog.Errorf("SBI server error: %v", err)
	}
	logger.SBILog.Infof("SBI server (listen on %s) stopped", s.httpServer.Addr)
}

func (s *Server) Shutdown() {
	s.shutdownHttpServer()
}

func (s *Server) Stop() {
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

func newRouter(s *Server) *gin.Engine {
	router := logger_util.NewGinWithLogrus(logger.GinLog)
	router.Use(metrics.InboundMetrics())
	router.Use(accesslog.InboundLogger())

	// EE
	udmEERoutes := s.getEventExposureRoutes()
	udmEEGroup := router.Group(factory.UdmEeResUriPrefix)
	udmEEGroup.Use(func(c *gin.Context) {
		util.NewRouterAuthorizationCheck(models.ServiceName_NUDM_EE).Check(c, s.Context())
	})
	AddService(udmEEGroup, udmEERoutes)

	// Callback
	udmCallBackRoutes := s.getHttpCallBackRoutes()
	udmCallNackGroup := router.Group("")
	AddService(udmCallNackGroup, udmCallBackRoutes)

	// UEAU
	udmUEAURoutes := s.getUEAuthenticationRoutes()
	udmUEAUGroup := router.Group(factory.UdmUeauResUriPrefix)
	udmUEAUGroup.Use(func(c *gin.Context) {
		util.NewRouterAuthorizationCheck(models.ServiceName_NUDM_UEAU).Check(c, s.Context())
	})
	AddService(udmUEAUGroup, udmUEAURoutes)

	ueauTwoLayerPath := "/:supi/:twoLayer"
	udmUEAUGroup.Any(ueauTwoLayerPath, s.UEAUTwoLayerPathHandlerFunc)

	ueauThreeLayerPath := "/:supi/:twoLayer/:thirdLayer"
	udmUEAUGroup.Any(ueauThreeLayerPath, s.UEAUThreeLayerPathHandlerFunc)

	generateAvPath := "/:supi/hss-security-information/:hssAuthType/generate-av"
	udmUEAUGroup.Any(generateAvPath, s.HandleGenerateAv)

	// UECM
	udmUECMRoutes := s.getUEContextManagementRoutes()
	udmUECMGroup := router.Group(factory.UdmUecmResUriPrefix)
	udmUECMGroup.Use(func(c *gin.Context) {
		util.NewRouterAuthorizationCheck(models.ServiceName_NUDM_UECM).Check(c, s.Context())
	})
	AddService(udmUECMGroup, udmUECMRoutes)

	// SDM
	udmSDMRoutes := s.getSubscriberDataManagementRoutes()
	udmSDMGroup := router.Group(factory.UdmSdmResUriPrefix)
	udmSDMGroup.Use(func(c *gin.Context) {
		util.NewRouterAuthorizationCheck(models.ServiceName_NUDM_SDM).Check(c, s.Context())
	})
	AddService(udmSDMGroup, udmSDMRoutes)

	oneLayerPath := "/:supi"
	udmSDMGroup.Any(oneLayerPath, s.OneLayerPathHandlerFunc)

	twoLayerPath := "/:supi/:subscriptionId"
	udmSDMGroup.Any(twoLayerPath, s.TwoLayerPathHandlerFunc)

	threeLayerPath := "/:supi/:subscriptionId/:thirdLayer"
	udmSDMGroup.Any(threeLayerPath, s.ThreeLayerPathHandlerFunc)

	// PP
	udmPPRoutes := s.getParameterProvisionRoutes()
	udmPPGroup := router.Group(factory.UdmPpResUriPrefix)
	udmPPGroup.Use(func(c *gin.Context) {
		util.NewRouterAuthorizationCheck(models.ServiceName_NUDM_PP).Check(c, s.Context())
	})
	AddService(udmPPGroup, udmPPRoutes)

	// MT
	udmMTRoutes := s.getMTRoutes()
	udmMTGroup := router.Group(factory.UdmMtResUrdPrefix)
	udmMTGroup.Use(func(c *gin.Context) {
		util.NewRouterAuthorizationCheck(models.ServiceName_NUDM_MT).Check(c, s.Context())
	})
	AddService(udmMTGroup, udmMTRoutes)

	// NIDDAU
	udmNIDDAURoutes := s.getNIDDAuthenticationRoutes()
	udmNIDDAUGroup := router.Group(factory.UdmNiddauResUriPrefix)
	udmNIDDAUGroup.Use(func(c *gin.Context) {
		util.NewRouterAuthorizationCheck(models.ServiceName_NUDM_NIDDAU).Check(c, s.Context())
	})
	AddService(udmNIDDAUGroup, udmNIDDAURoutes)

	// RSDS
	udmRSDSRoutes := s.getReportSMDeliveryStatusRoutes()
	udmRSDSGroup := router.Group(factory.UdmRsdsResUriPrefix)
	udmRSDSGroup.Use(func(c *gin.Context) {
		util.NewRouterAuthorizationCheck(models.ServiceName_NUDM_RSDS).Check(c, s.Context())
	})
	AddService(udmRSDSGroup, udmRSDSRoutes)

	// SSAU
	udmSSAURoutes := s.getServiceSpecificAuthorizationRoutes()
	udmSSAUGroup := router.Group(factory.UdmSsauResUriPrefix)
	udmSSAUGroup.Use(func(c *gin.Context) {
		util.NewRouterAuthorizationCheck(models.ServiceName_NUDM_SSAU).Check(c, s.Context())
	})
	AddService(udmSSAUGroup, udmSSAURoutes)

	// UEID
	udmUEIDRoutes := s.getUEIDRoutes()
	udmUEIDGroup := router.Group(factory.UdmUeidResUriPrefix)
	udmUEIDGroup.Use(func(c *gin.Context) {
		util.NewRouterAuthorizationCheck(models.ServiceName_NUDM_UEID).Check(c, s.Context())
	})
	AddService(udmUEIDGroup, udmUEIDRoutes)

	return router
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
