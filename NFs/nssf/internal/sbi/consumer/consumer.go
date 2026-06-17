package consumer

import (
	"github.com/free5gc/nssf/pkg/app"
	"github.com/free5gc/openapi/nrf/NFManagement"
	"github.com/free5gc/nssf/internal/accesslog"
	sbi_metrics "github.com/free5gc/util/metrics/sbi"
)

type Consumer struct {
	app.NssfApp

	*NrfService
}

func NewConsumer(nssf app.NssfApp) *Consumer {
	configuration := NFManagement.NewConfiguration()
	configuration.SetBasePath(nssf.Context().NrfUri)
	configuration.SetMetrics(sbi_metrics.SbiMetricHook)
	configuration.SetHTTPClient(accesslog.Client())
	nrfService := &NrfService{
		nrfNfMgmtClient: NFManagement.NewAPIClient(configuration),
	}

	return &Consumer{
		NssfApp:    nssf,
		NrfService: nrfService,
	}
}
