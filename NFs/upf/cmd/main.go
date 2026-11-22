package main

import (
	"math/rand"
	"net/http"         // [ON 1122] pprof listener on :6060 only, see main()
	_ "net/http/pprof" // [ON 1122] registers /debug/pprof handlers on the default mux
	"os"
	"runtime" // TYcustom: MemProfileRate / block / mutex rates, see main()
	"runtime/debug"
	"time"

	"github.com/urfave/cli"

	"github.com/free5gc/go-upf/internal/logger"
	"github.com/free5gc/go-upf/internal/schedstat" // [profON 1122] /debug/schedstat, see main()
	upfapp "github.com/free5gc/go-upf/pkg/app"
	"github.com/free5gc/go-upf/pkg/factory"
	logger_util "github.com/free5gc/util/logger"
	"github.com/free5gc/util/version"
)

func main() {
	// TYcustom: heap-allocation sampling.
	// [profON 1122] Back on: the line below is commented out, so the runtime
	// keeps its default MemProfileRate (one sample per 512 KiB allocated) and
	// /debug/pprof/allocs and heap have samples. Runs with and without it are
	// not directly comparable: it changes the GC count. To turn it off again,
	// uncomment the line; keep it first in main, as the runtime docs ask.
	// runtime.MemProfileRate = 0

	defer func() {
		if p := recover(); p != nil {
			// Print stack for panic to log. Fatalf() will let program exit.
			logger.MainLog.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
		}
	}()

	// --- Lock-contention + scheduler profiling (TYcustom) ---
	// [profON 1122] Added, same three lines as the control-plane NFs, so that
	// trace_cpu_sche_block_mutex_allNF_gctrace_allocM.sh collects block, mutex
	// and schedstat from the UPF too. block/mutex are cumulative since process
	// start; only the PRE/AFTER difference is meaningful. To turn it off again:
	// comment out the three code lines below and the schedstat import marked
	// [profON 1122], and uncomment runtime.MemProfileRate = 0 at the top of main.
	runtime.SetMutexProfileFraction(5) // sample ~1/5 of mutex contention events
	runtime.SetBlockProfileRate(10000) // sample a blocking event ~every 10us blocked
	schedstat.RegisterSchedStat()      // must precede the ListenAndServe below

	// --- pprof listener on :6060 (TYcustom, manual GC) ---
	// [ON 1122] Same as the control-plane NFs' [ON 1120] block: lets
	// force_gc_allNF.sh run a full GC through GET :6060/debug/pprof/heap?gc=1.
	// Serving the endpoints turns on no sampling by itself; the sampling is the
	// mutex/block rates above and MemProfileRate at the top of main, all on
	// since [profON 1122]. To turn the listener off: comment out the block below
	// and the two imports marked [ON 1122].
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
