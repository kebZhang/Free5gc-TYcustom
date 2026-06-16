package app

import (
	bsf_context "github.com/free5gc/bsf/internal/context"
	"github.com/free5gc/bsf/pkg/factory"
)

type App interface {
	SetLogEnable(enable bool)
	SetLogLevel(level string)
	SetReportCaller(reportCaller bool)

	Start()
	Terminate()

	Context() *bsf_context.BSFContext
	Config() *factory.Config
}
