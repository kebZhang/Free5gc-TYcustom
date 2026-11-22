package main

import (
	"math/rand"
	"net/http"         // [ON 1122] pprof listener on :6060 only, see main()
	_ "net/http/pprof" // [ON 1122] registers /debug/pprof handlers on the default mux
	"os"
	"runtime" // TYcustom: MemProfileRate, see main()
	"runtime/debug"
	"time"

	"github.com/urfave/cli"

	"github.com/free5gc/go-upf/internal/logger"
	upfapp "github.com/free5gc/go-upf/pkg/app"
	"github.com/free5gc/go-upf/pkg/factory"
	logger_util "github.com/free5gc/util/logger"
	"github.com/free5gc/util/version"
)

func main() {
	// TYcustom: no profiling or sampling. The heap-allocation profiler is the
	// one profiler the Go runtime runs by default (MemProfileRate = 512 KiB), so
	// switch it off too, as early as possible per the runtime docs. (Carried
	// over from the v4.2.2 UPF this file replaced on 1122.)
	runtime.MemProfileRate = 0

	defer func() {
		if p := recover(); p != nil {
			// Print stack for panic to log. Fatalf() will let program exit.
			logger.MainLog.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
		}
	}()

	// --- pprof listener on :6060 (TYcustom, manual GC) ---
	// [ON 1122] Same as the control-plane NFs' [ON 1120] block: lets
	// force_gc_allNF.sh run a full GC through GET :6060/debug/pprof/heap?gc=1.
	// Serving the endpoints turns on no sampling. To turn it off: comment out
	// the block below and the two imports marked [ON 1122].
	go func() {
		if err := http.ListenAndServe("0.0.0.0:6060", nil); err != nil {
			logger.MainLog.Warnf("pprof server on :6060 exited: %v", err)
		}
	}()

	app := cli.NewApp()
	app.Name = "upf"
	app.Usage = "5G User Plane Function (UPF)"
	app.Action = action
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config, c",
			Usage: "Load configuration from `FILE`",
		},
		cli.StringSliceFlag{
			Name:  "log, l",
			Usage: "Output NF log to `FILE`",
		},
	}

	// rand.Seed(time.Now().UnixNano()) // rand.Seed has been deprecated
	randSeed := rand.New(rand.NewSource(time.Now().UnixNano()))
	randSeed.Uint64()

	if err := app.Run(os.Args); err != nil {
		logger.MainLog.Errorf("UPF Cli Run Error: %v", err)
	}
}

func action(cliCtx *cli.Context) error {
	err := initLogFile(cliCtx.StringSlice("log"))
	if err != nil {
		return err
	}

	logger.MainLog.Infoln("UPF version: ", version.GetVersion())

	cfg, err := factory.ReadConfig(cliCtx.String("config"))
	if err != nil {
		return err
	}

	upf, err := upfapp.NewApp(cfg)
	if err != nil {
		return err
	}

	if err := upf.Run(); err != nil {
		return err
	}

	return nil
}

func initLogFile(logNfPath []string) error {
	for _, path := range logNfPath {
		if err := logger_util.LogFileHook(logger.Log, path); err != nil {
			return err
		}
	}
	return nil
}
