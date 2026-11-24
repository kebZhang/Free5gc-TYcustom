package util

import (
	"github.com/free5gc/chf/internal/accesslog" // [CHF port 1124]
	Nchf_ConvergedCharging "github.com/free5gc/openapi/chf/ConvergedCharging"
)

func GetNchfChargingNotificationCallbackClient() *Nchf_ConvergedCharging.APIClient {
	configuration := Nchf_ConvergedCharging.NewConfiguration()
	configuration.SetHTTPClient(accesslog.Client()) // [CHF port 1124]
	client := Nchf_ConvergedCharging.NewAPIClient(configuration)
	return client
}
