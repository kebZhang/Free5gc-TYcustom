package main

import (
	"context"
	// "net/http" // [OFF 1026] profiling, see main()
	// _ "net/http/pprof" // [OFF 1026] registers /debug/pprof handlers on the default mux
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/urfave/cli/v2"

	"github.com/free5gc/nrf/internal/logger"
	"github.com/free5gc/nrf/pkg/factory"
	"github.com/free5gc/nrf/pkg/service"
	logger_util "github.com/free5gc/util/logger"
	"github.com/free5gc/util/version"
)

var NRF *service.NrfApp

func main() {
	// TYcustom: no profiling or sampling. The heap-allocation profiler is the
	// one profiler the Go runtime runs by default (MemProfileRate = 512 KiB), so
	// switch it off too, as early as possible per the runtime docs.
	runtime.MemProfileRate = 0

	// --- pprof on :6060 (TYcustom, noGC experiment) ---
	// [OFF 1026] Commented out, not deleted. To turn it back on: uncomment
	// the code lines below and the imports marked [OFF 1026], and delete the
	// runtime.MemProfileRate = 0 line at the top of main.
	//
	// Same listener as amf/ausf/pcf/udm/udr. GoRuntime_NoGC_Readme.md runs every
	// NF with GOGC=off and collects it by hand between runs through
	// GET :6060/debug/pprof/heap?gc=1 (force_gc_allNF.sh), which needs this port.
	// go func() {
	// 	if err := http.ListenAndServe("0.0.0.0:6060", nil); err != nil {
	// 		logger.MainLog.Warnf("pprof server on :6060 exited: %v", err)
	// 	}
	// }()

	app := cli.NewApp()
	app.Name = "nrf"
	app.Usage = "5G Network Repository Function (NRF)"
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
		logger.MainLog.Errorf("NRF Run Error: %v\n", err)
	}
}

func action(cliCtx *cli.Context) error {
	tlsKeyLogPath, err := initLogFile(cliCtx.StringSlice("log"))
	if err != nil {
		return err
	}
	logger.MainLog.Infoln("NRF version: ", version.GetVersion())

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
	factory.NrfConfig = cfg

	nrf, err := service.NewApp(ctx, cfg, tlsKeyLogPath)
	if err != nil {
		return err
	}
	NRF = nrf

	nrf.Start()
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
		_, name := filepath.Split(factory.NrfDefaultTLSKeyLogPath)
		logTlsKeyPath = filepath.Join(tmpDir, name)
	}
	return logTlsKeyPath, nil
}
