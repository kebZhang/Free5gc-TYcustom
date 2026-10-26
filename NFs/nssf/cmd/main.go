package main

import (
	"context"
	// "net/http" // [OFF 1026] profiling, see main()
	// _ "net/http/pprof" // [OFF 1026] registers /debug/pprof handlers on the default mux
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"syscall"

	"github.com/urfave/cli/v2"

	"github.com/free5gc/nssf/internal/logger"
	"github.com/free5gc/nssf/pkg/factory"
	"github.com/free5gc/nssf/pkg/service"
	logger_util "github.com/free5gc/util/logger"
	"github.com/free5gc/util/version"
)

var NSSF *service.NssfApp

func main() {
	// TYcustom: no profiling or sampling. The heap-allocation profiler is the
	// one profiler the Go runtime runs by default (MemProfileRate = 512 KiB), so
	// switch it off too, as early as possible per the runtime docs.
	runtime.MemProfileRate = 0

	defer func() {
		if p := recover(); p != nil {
			// Print stack for panic to log. Fatalf() will let program exit.
			logger.MainLog.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
		}
	}()

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
	app.Name = "nssf"
	app.Usage = "5G Network Slice Selection Function (NSSF)"
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
		logger.MainLog.Errorf("NSSF Run Error: %v\n", err)
	}
}

func action(cliCtx *cli.Context) error {
	tlsKeyLogPath, err := initLogFile(cliCtx.StringSlice("log"))
	if err != nil {
		return err
	}

	logger.MainLog.Infoln("NSSF version: ", version.GetVersion())
	cfg, err := factory.ReadConfig(cliCtx.String("config"))
	if err != nil {
		return err
	}
	factory.NssfConfig = cfg

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigCh  // Wait for interrupt signal to gracefully shutdown
		cancel() // Notify each goroutine and wait them stopped
	}()

	nssf, err := service.NewApp(ctx, cfg, tlsKeyLogPath)
	if err != nil {
		return err
	}
	NSSF = nssf

	nssf.Start()

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
		_, name := filepath.Split(factory.NssfDefaultTLSKeyLogPath)
		logTlsKeyPath = filepath.Join(tmpDir, name)
	}

	return logTlsKeyPath, nil
}
