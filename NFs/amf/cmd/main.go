package main

import (
	"context"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof handlers on the default mux
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"syscall"

	"github.com/urfave/cli/v2"

	"github.com/free5gc/amf/internal/logger"
	"github.com/free5gc/amf/pkg/factory"
	"github.com/free5gc/amf/pkg/service"
	logger_util "github.com/free5gc/util/logger"
	"github.com/free5gc/util/version"
)

var AMF *service.AmfApp

func main() {
	defer func() {
		if p := recover(); p != nil {
			// Print stack for panic to log. Fatalf() will let program exit.
			logger.MainLog.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
		}
	}()

	// --- Lock-contention profiling (TYcustom, for AMF-local latency debug) ---
	// Turn on mutex + block profiling and expose pprof on :6060. This lets us
	// snapshot /debug/pprof/mutex before/after each RQ run and diff them, to see
	// which lock (e.g. UePool sync.Map) accumulates the most wait time as the
	// request rate rises. Safe to leave on: sampling is cheap and the endpoint is
	// pull-only. Remove/guard for production if the extra port is unwanted.
	runtime.SetMutexProfileFraction(5) // sample ~1/5 of mutex contention events
	runtime.SetBlockProfileRate(10000) // sample a blocking event ~every 10us blocked
	go func() {
		if err := http.ListenAndServe("0.0.0.0:6060", nil); err != nil {
			logger.MainLog.Warnf("pprof server on :6060 exited: %v", err)
		}
	}()

	app := cli.NewApp()
	app.Name = "amf"
	app.Usage = "5G Access and Mobility Management Function (AMF)"
	app.Action = action
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "config",
			Aliases: []string{"c"},
			Usage:   "Load configuration from `FILE`",
		},
		&cli.StringSliceFlag{
			Name:    "log",
			Aliases: []string{"l"},
			Usage:   "Output NF log to `FILE`",
		},
	}
	if err := app.Run(os.Args); err != nil {
		logger.MainLog.Errorf("AMF Run error: %v\n", err)
		return
	}
}

func action(cliCtx *cli.Context) error {
	tlsKeyLogPath, err := initLogFile(cliCtx.StringSlice("log"))
	if err != nil {
		return err
	}

	logger.MainLog.Infoln("AMF version: ", version.GetVersion())

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigCh  // Wait for interrupt signal to gracefully shutdown
		cancel() // Notify each goroutine and wait them stopped
	}()

	cfg, err := factory.ReadConfig(cliCtx.String("config"))
	if err != nil {
		return err
	}
	factory.AmfConfig = cfg

	amf, err := service.NewApp(ctx, cfg, tlsKeyLogPath)
	if err != nil {
		return err
	}
	AMF = amf

	amf.Start()

	return nil
}

func initLogFile(logNfPath []string) (string, error) {
	logTlsKeyPath := ""

	for _, path := range logNfPath {
		if err := logger_util.LogFileHook(logger.Log, path); err != nil {
			return "", err
		}

		if logTlsKeyPath != "" {
			continue
		}

		nfDir, _ := filepath.Split(path)
		tmpDir := filepath.Join(nfDir, "key")
		if err := os.MkdirAll(tmpDir, 0o775); err != nil {
			logger.InitLog.Errorf("Make directory %s failed: %+v", tmpDir, err)
			return "", err
		}
		_, name := filepath.Split(factory.AmfDefaultTLSKeyLogPath)
		logTlsKeyPath = filepath.Join(tmpDir, name)
	}

	return logTlsKeyPath, nil
}
