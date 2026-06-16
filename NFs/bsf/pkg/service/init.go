package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	bsfContext "github.com/free5gc/bsf/internal/context"
	"github.com/free5gc/bsf/internal/logger"
	businessMetrics "github.com/free5gc/bsf/internal/metrics/business"
	"github.com/free5gc/bsf/internal/sbi"
	"github.com/free5gc/bsf/internal/sbi/consumer"
	"github.com/free5gc/bsf/internal/sbi/processor"
	"github.com/free5gc/bsf/pkg/app"
	"github.com/free5gc/bsf/pkg/factory"
	"github.com/free5gc/util/metrics"
	sbiMetrics "github.com/free5gc/util/metrics/sbi"
	"github.com/free5gc/util/metrics/utils"
)

type BsfAppInterface interface {
	app.App
	consumer.ConsumerBsf
	Consumer() *consumer.Consumer
	Processor() *processor.Processor
}

var BSF BsfAppInterface

type BsfApp struct {
	BsfAppInterface

	cfg           *factory.Config
	bsfCtx        *bsfContext.BSFContext
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	tlsKeyLogPath string

	processor     *processor.Processor
	consumer      *consumer.Consumer
	metricsServer *metrics.Server
	server        *http.Server
}

func NewApp(ctx context.Context, cfg *factory.Config, tlsKeyLogPath string) (*BsfApp, error) {
	bsf := &BsfApp{
		cfg:           cfg,
		tlsKeyLogPath: tlsKeyLogPath,
	}
	bsf.SetLogEnable(cfg.Logger.Enable)
	bsf.SetLogLevel(cfg.Logger.Level)
	bsf.SetReportCaller(cfg.Logger.ReportCaller)

	// Initialize context
	bsf.ctx, bsf.cancel = context.WithCancel(ctx)
	bsfContext.InitBsfContext()
	bsf.bsfCtx = bsfContext.BsfSelf

	// Initialize consumer
	var err error
	if bsf.consumer, err = consumer.NewConsumer(bsf); err != nil {
		return nil, fmt.Errorf("failed to initialize consumer: %w", err)
	}

	// Initialize processor singleton
	if bsf.processor, err = processor.NewProcessor(bsf); err != nil {
		return nil, fmt.Errorf("failed to initialize processor: %w", err)
	}

	// Set BSF context configuration
	bsf.bsfCtx.NrfUri = cfg.Configuration.NrfUri

	// Initialize metrics if enabled
	if cfg.AreMetricsEnabled() {
		sbiMetrics.EnableSbiMetrics()

		features := map[utils.MetricTypeEnabled]bool{utils.SBI: true}
		customMetrics := getCustomMetrics(cfg)

		var metricsErr error
		if bsf.metricsServer, metricsErr = metrics.NewServer(
			getInitMetrics(cfg, features, customMetrics), tlsKeyLogPath, logger.MainLog); metricsErr != nil {
			logger.MainLog.Warnf("Failed to create metrics server: %+v", metricsErr)
		}
	}

	BSF = bsf
	return bsf, nil
}

func getCustomMetrics(cfg *factory.Config) map[utils.MetricTypeEnabled][]prometheus.Collector {
	customMetrics := make(map[utils.MetricTypeEnabled][]prometheus.Collector)

	// Enable business metrics if configured
	if cfg.AreMetricsEnabled() {
		businessMetrics.EnableBindingMetrics()
		businessMetrics.EnableDiscoveryMetrics()

		// Add BSF business metrics
		customMetrics[utils.SBI] = append(
			customMetrics[utils.SBI],
			businessMetrics.GetBindingHandlerMetrics(cfg.GetMetricsNamespace())...)

		// Add discovery metrics
		customMetrics[utils.SBI] = append(
			customMetrics[utils.SBI],
			businessMetrics.GetDiscoveryHandlerMetrics(cfg.GetMetricsNamespace())...)
	}

	return customMetrics
}

func getInitMetrics(
	cfg *factory.Config,
	features map[utils.MetricTypeEnabled]bool,
	customMetrics map[utils.MetricTypeEnabled][]prometheus.Collector,
) metrics.InitMetrics {
	metricsInfo := metrics.Metrics{
		BindingIPv4: cfg.GetMetricsBindingAddr(),
		Scheme:      cfg.GetMetricsScheme(),
		Namespace:   cfg.GetMetricsNamespace(),
		Port:        cfg.GetMetricsPort(),
		Tls: metrics.Tls{
			Key: cfg.GetMetricsCertKeyPath(),
			Pem: cfg.GetMetricsCertPemPath(),
		},
	}

	return metrics.NewInitMetrics(metricsInfo, "bsf", features, customMetrics)
}

func (a *BsfApp) SetLogEnable(enable bool) {
	logger.MainLog.Infof("Log enable is set to [%v]", enable)
	if enable && logger.Log.Out == os.Stderr {
		return
	} else if !enable && logger.Log.Out == io.Discard {
		return
	}

	a.cfg.SetLogEnable(enable)
	if enable {
		logger.Log.SetOutput(os.Stderr)
	} else {
		logger.Log.SetOutput(io.Discard)
	}
}

func (a *BsfApp) SetLogLevel(level string) {
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		logger.MainLog.Warnf("Log level [%s] is invalid", level)
		return
	}

	logger.MainLog.Infof("Log level is set to [%s]", level)
	if lvl == logger.Log.GetLevel() {
		return
	}

	a.cfg.SetLogLevel(level)
	logger.Log.SetLevel(lvl)
}

func (a *BsfApp) SetReportCaller(reportCaller bool) {
	logger.MainLog.Infof("Report Caller is set to [%v]", reportCaller)
	if reportCaller == logger.Log.ReportCaller {
		return
	}

	a.cfg.SetLogReportCaller(reportCaller)
	logger.Log.SetReportCaller(reportCaller)
}

func (a *BsfApp) Start() {
	logger.MainLog.Infoln("BSF started")

	if err := factory.CheckConfigVersion(); err != nil {
		logger.MainLog.Warnf("Config version error: %v", err)
	}

	// Initialize MongoDB connection
	if mongoErr := a.bsfCtx.ConnectMongoDB(a.ctx); mongoErr != nil {
		logger.MainLog.Warnf("MongoDB connection failed: %+v", mongoErr)
	} else {
		// Load existing bindings from MongoDB
		if loadErr := a.bsfCtx.LoadPcfBindingsFromMongoDB(); loadErr != nil {
			logger.MainLog.Warnf("Failed to load PCF bindings from MongoDB: %+v", loadErr)
		}
	}

	// Start cleanup routine for expired and inactive bindings
	a.bsfCtx.StartCleanupRoutine()

	// Start listening for shutdown events
	a.wg.Add(1)
	go a.listenShutdownEvent()

	// Start metrics server if enabled
	if a.cfg.AreMetricsEnabled() && a.metricsServer != nil {
		go func() {
			a.metricsServer.Run(&a.wg)
		}()
		logger.MainLog.Infof("BSF metrics server enabled on %s://%s",
			a.cfg.GetMetricsScheme(), a.cfg.GetMetricsBindingAddr())
	}

	// Register with NRF - moved to consumer
	go func() {
		if err := a.consumer.RegisterWithNRF(a.ctx); err != nil {
			logger.MainLog.Errorf("BSF register to NRF Error: %+v", err)
		} else {
			logger.MainLog.Infof("BSF successfully registered with NRF")
		}
	}()

	// Start SBI server
	router := gin.Default()
	sbi.AddService(router)

	// Add CORS
	router.Use(cors.New(cors.Config{
		AllowMethods: []string{"GET", "POST", "OPTIONS", "PUT", "PATCH", "DELETE"},
		AllowHeaders: []string{
			"Origin", "Content-Length", "Content-Type", "User-Agent",
			"Referrer", "Host", "Token", "X-Requested-With",
		},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		AllowAllOrigins:  false,
		AllowOriginFunc:  func(origin string) bool { return true },
		MaxAge:           86400,
	}))

	bindAddr := fmt.Sprintf("%s:%d", a.cfg.Configuration.Sbi.BindingIPv4, a.cfg.Configuration.Sbi.Port)
	logger.MainLog.Infof("BSF SBI Server started on %s", bindAddr)

	a.server = &http.Server{
		Addr:           bindAddr,
		Handler:        router,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	// Start server and wait for it to finish
	if err := a.runServer(); err != nil {
		logger.MainLog.Fatalf("Run SBI server failed: %+v", err)
	}
	a.WaitRoutineStopped()
}

func (a *BsfApp) runServer() error {
	serverErr := make(chan error, 1)
	go func() {
		var err error
		switch a.cfg.Configuration.Sbi.Scheme {
		case "http":
			err = a.server.ListenAndServe()
		case "https":
			err = a.server.ListenAndServeTLS(
				a.cfg.Configuration.Sbi.Tls.Pem,
				a.cfg.Configuration.Sbi.Tls.Key,
			)
		default:
			err = fmt.Errorf("unsupported scheme: %s", a.cfg.Configuration.Sbi.Scheme)
		}
		// Send error to channel (including ErrServerClosed for testing)
		if err != nil {
			serverErr <- err
		}
	}()

	// Wait for context cancellation or server error
	select {
	case <-a.ctx.Done():
		// Context canceled, normal shutdown
		return nil
	case err := <-serverErr:
		// Server error (could be ErrServerClosed after shutdown)
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}

// Used in BSF planned removal procedure
func (a *BsfApp) Terminate() {
	a.cancel()
}

func (a *BsfApp) Config() *factory.Config {
	return a.cfg
}

func (a *BsfApp) Context() *bsfContext.BSFContext {
	return a.bsfCtx
}

func (a *BsfApp) CancelContext() context.Context {
	return a.ctx
}

func (a *BsfApp) Consumer() *consumer.Consumer {
	return a.consumer
}

func (a *BsfApp) Processor() *processor.Processor {
	return a.processor
}

func (a *BsfApp) listenShutdownEvent() {
	defer func() {
		if p := recover(); p != nil {
			// Print stack for panic to log. Fatalf() will let program exit.
			logger.MainLog.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
		}
		a.wg.Done()
	}()

	<-a.ctx.Done()
	a.terminateProcedure()
}

func (a *BsfApp) CallServerStop() {
	// Stop cleanup routine
	if a.bsfCtx != nil {
		a.bsfCtx.StopCleanupRoutine()
	}

	// Shutdown SBI server if running
	if a.server != nil {
		// Use a separate context with timeout for shutdown
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()

		if err := a.server.Shutdown(shutdownCtx); err != nil {
			logger.MainLog.Errorf("Error shutting down SBI server: %+v", err)
		}
	}

	// Stop metrics server
	if a.metricsServer != nil {
		a.metricsServer.Stop()
		logger.MainLog.Infof("BSF Metrics Server terminated")
	}
}

func (a *BsfApp) WaitRoutineStopped() {
	a.wg.Wait()
	logger.MainLog.Infof("BSF App is terminated")
}

func (a *BsfApp) terminateProcedure() {
	logger.MainLog.Infof("Terminating BSF...")

	// Stop all servers first
	a.CallServerStop()

	// Deregister from NRF using consumer
	if a.consumer != nil {
		if err := a.consumer.DeregisterWithNRF(); err != nil {
			logger.MainLog.Errorf("BSF deregister from NRF Error: %+v", err)
			// Don't return error here as termination should continue
		} else {
			logger.MainLog.Infof("[BSF] Deregister from NRF successfully")
		}
	}

	// Disconnect from MongoDB
	if a.bsfCtx != nil {
		if err := a.bsfCtx.DisconnectMongoDB(); err != nil {
			logger.MainLog.Errorf("Error disconnecting from MongoDB: %+v", err)
		}
	}

	logger.MainLog.Info("BSF termination complete")
}
