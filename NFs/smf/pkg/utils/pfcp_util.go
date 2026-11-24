package utils

import (
	"context"
	"time"

	"github.com/free5gc/pfcp/dnscache" // [DNSCACHE 1124]
	"github.com/free5gc/pfcp/pfcpType" // [DNSCACHE 1124]
	smf_context "github.com/free5gc/smf/internal/context"
	"github.com/free5gc/smf/internal/logger"
	"github.com/free5gc/smf/internal/pfcp"
	"github.com/free5gc/smf/internal/pfcp/udp"
	"github.com/free5gc/smf/pkg/service"
)

func InitPFCPFunc(pCtx context.Context) (func(a *service.SmfApp), func()) {
	smfContext := smf_context.GetSelf()

	pfcpStart := func(a *service.SmfApp) {
		// Initialize PFCP server
		smfContext.PfcpContext, smfContext.PfcpCancelFunc = context.WithCancel(pCtx)

		// [DNSCACHE 1124] start the 1-min DNS cache refresher and resolve every configured
		// name once, so the first PDU sessions never miss (SMFUPF_cacheDNS_PLAN1124.md 3.4).
		// The UPF's self-reported NodeID is prewarmed at association setup (association.go).
		dnscache.Start()
		hosts := []string{smfContext.ExternalAddr, smfContext.ListenAddr}
		for _, upNode := range smfContext.UserPlaneInformation.UPFs {
			if upNode.NodeID.NodeIdType == pfcpType.NodeIdTypeFqdn {
				hosts = append(hosts, upNode.NodeID.FQDN)
			}
			if upNode.UPF == nil {
				continue
			}
			for _, ifs := range [][]*smf_context.UPFInterfaceInfo{upNode.UPF.N3Interfaces, upNode.UPF.N9Interfaces} {
				for _, i := range ifs {
					hosts = append(hosts, i.EndpointFQDN)
				}
			}
		}
		dnscache.Prewarm(hosts...)

		udp.Run(pfcp.Dispatch)

		// Wait for PFCP start
		time.Sleep(1000 * time.Millisecond)

		for _, upNode := range smf_context.GetSelf().UserPlaneInformation.UPFs {
			go a.Processor().ToBeAssociatedWithUPF(smfContext.PfcpContext, upNode.UPF)
		}
	}

	pfcpStop := func() {
		smfContext.PfcpCancelFunc()
		err := udp.Server.Close()
		if err != nil {
			logger.Log.Errorf("udp server close failed %+v", err)
		}
	}

	return pfcpStart, pfcpStop
}
