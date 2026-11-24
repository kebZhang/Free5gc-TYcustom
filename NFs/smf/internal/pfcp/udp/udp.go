package udp

import (
	"errors"
	"net"
	"runtime/debug"
	"strings"
	"sync" // [PFCPlog 1122]
	"time"

	"github.com/free5gc/pfcp"
	"github.com/free5gc/pfcp/dnscache" // [DNSCACHE 1124]
	"github.com/free5gc/pfcp/pfcpUdp"
	smf_context "github.com/free5gc/smf/internal/context"
	"github.com/free5gc/smf/internal/logger"
	"github.com/free5gc/smf/internal/pfcplog" // [PFCPlog 1122]
)

const MaxPfcpUdpDataSize = 1024

var Server *pfcpUdp.PfcpServer

var ServerStartTime time.Time

func Run(dispatch func(*pfcpUdp.Message)) {
	defer func() {
		if p := recover(); p != nil {
			// Print stack for panic to log. Fatalf() will let program exit.
			logger.PfcpLog.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
		}
	}()

	smfContext := smf_context.GetSelf()

	serverIP := smfContext.ListenIP().To4()
	Server = pfcpUdp.NewPfcpServer(serverIP.String())
	Server.StampRecv = pfcplog.Enabled // [PFCPlog 1122] receive stamp in the local pfcp library

	err := Server.Listen()
	if err != nil {
		logger.PfcpLog.Errorf("Failed to listen: %v", err)
	}

	logger.PfcpLog.Infof("Listen on %s", Server.Conn.LocalAddr().String())

	go func(p *pfcpUdp.PfcpServer) {
		defer func() {
			if p := recover(); p != nil {
				// Print stack for panic to log. Fatalf() will let program exit.
				logger.PfcpLog.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
			}
		}()

		for {
			msg, errReadFrom := p.ReadFrom()
			if errReadFrom != nil {
				if errReadFrom == pfcpUdp.ErrReceivedResentRequest {
					logger.PfcpLog.Infoln(errReadFrom)
				} else if strings.Contains(errReadFrom.Error(), "use of closed network connection") {
					continue
				} else {
					logger.PfcpLog.Warnf("Read PFCP error: %v, msg: [%v]", errReadFrom, msg)
					select {
					case <-smfContext.PfcpContext.Done():
						// PFCP is closing
						return
					default:
						continue
					}
				}
				continue
			}

			if msg.PfcpMessage.IsRequest() {
				if pfcplog.Enabled { // [PFCPlog 1122]
					go dispatchLogged(dispatch, msg)
				} else {
					go dispatch(msg)
				}
			}
		}
	}(Server)

	ServerStartTime = time.Now()

	logger.PfcpLog.Infof("Pfcp running... [%v]", ServerStartTime)
}

func SendPfcpResponse(sndMsg *pfcp.Message, addr *net.UDPAddr) {
	if !pfcplog.Enabled { // [PFCPlog 1122]
		Server.WriteResponseTo(sndMsg, addr)
		return
	}
	// [PFCPlog 1122] rx line: pick up what dispatchLogged recorded for the
	// request this answers (same peer, same seq), add the response stamps, and
	// log once the response has been written.
	rspStart := pfcplog.Now()
	h := sndMsg.Header
	var rec pfcplog.Record
	if v, ok := pendingRx.LoadAndDelete(pendingKey{peer: addr.String(), seq: h.SequenceNumber}); ok {
		rec = v.(pfcplog.Record)
	} else {
		// No dispatch record (should not happen): a PFCP response type is its
		// request type + 1 (29.244 Table 7.3-1).
		rec = pfcplog.Record{
			Role: pfcplog.RoleRx, MsgType: uint8(h.MessageType) - 1, Seq: h.SequenceNumber,
			Local: Server.Conn.LocalAddr(), Peer: addr,
		}
	}
	rec.RspStart = rspStart
	rec.RspType = uint8(h.MessageType)
	Server.WriteResponseToTraced(sndMsg, addr, func(written time.Time) {
		rec.RspWritten = pfcplog.Stamp(written)
		pfcplog.Log(rec)
	})
}

func SendPfcpRequest(sndMsg *pfcp.Message, addr *net.UDPAddr) (rsvMsg *pfcpUdp.Message, err error) {
	return SendPfcpRequestUE(sndMsg, addr, "") // [PFCPlog 1122] body moved
}

// SendPfcpRequestUE [PFCPlog 1122] is SendPfcpRequest that also names the UE the
// request is for (SUPI, or "" for node-level messages such as heartbeat and
// association), for the tx line in PFCP_log. With pfcplog off it is exactly the
// old SendPfcpRequest.
//
// [DNSCACHE 1124] results are named (was: (*pfcpUdp.Message, error)) so the deferred check
// below sees the error of every return path.
func SendPfcpRequestUE(sndMsg *pfcp.Message, addr *net.UDPAddr, ueID string) (rsvMsg *pfcpUdp.Message, err error) {
	// [DNSCACHE 1124] like Aether's HandlePfcpSendError (pfcp/message/send.go:464): any failed
	// PFCP request -- retry-out after NumOfResend x 3 s, a write error, or a 0.0.0.0
	// destination -- triggers one background DNS cache refresh (coalesced, see
	// SMFUPF_cacheDNS_PLAN1124.md 3.6). Heartbeat requests come through here too.
	defer func() {
		if err != nil {
			dnscache.RefreshNow()
		}
	}()
	if addr.IP.Equal(net.IPv4zero) {
		return nil, errors.New("no destination IP address is specified")
	}
	if !pfcplog.Enabled {
		return Server.WriteRequestTo(sndMsg, addr)
	}

	// send_start is taken before the library encodes the request; req_written
	// after its first WriteToUDP; rsp_recv when the library's read loop got the
	// response off the socket; done when it is back here. rsp_recv -> done
	// includes the read loop handing the response over an unbuffered channel
	// to this goroutine and this goroutine being scheduled.
	sendStart := pfcplog.Now()
	rsvMsg, tr, err := Server.WriteRequestToTraced(sndMsg, addr)
	done := pfcplog.Now()

	h := sndMsg.Header
	rec := pfcplog.Record{
		Role:       pfcplog.RoleTx,
		MsgType:    uint8(h.MessageType),
		Seq:        h.SequenceNumber,
		SEID:       h.SEID,
		Local:      Server.Conn.LocalAddr(),
		Peer:       addr,
		UEID:       ueID,
		SendStart:  sendStart,
		ReqWritten: pfcplog.Stamp(tr.FirstWrite),
		RspRecv:    pfcplog.Stamp(tr.RspRecv),
		Done:       done,
		Attempts:   uint8(tr.Attempts),
	}
	switch {
	case err == nil:
		if rsvMsg != nil && rsvMsg.PfcpMessage != nil {
			rec.RspType = uint8(rsvMsg.PfcpMessage.Header.MessageType)
		}
	case tr.Attempts >= pfcp.NumOfResend && tr.RspRecv.IsZero():
		rec.Outcome = pfcplog.OutcomeTimeout
	default:
		rec.Outcome = pfcplog.OutcomeError
	}
	pfcplog.Log(rec)
	return rsvMsg, err
}

// [PFCPlog 1122] Requests this SMF receives (in practice only Session Report
// Requests from the UPF) are dispatched on their own goroutine, and the handler
// answers through SendPfcpResponse. pendingRx carries the request's stamps from
// one to the other, keyed by (peer, seq) -- the handlers always answer to
// msg.RemoteAddr with the request's sequence number.
type pendingKey struct {
	peer string
	seq  uint32
}

var pendingRx sync.Map // pendingKey -> pfcplog.Record

func dispatchLogged(dispatch func(*pfcpUdp.Message), msg *pfcpUdp.Message) {
	deq := pfcplog.Now() // req_deq: this dispatch goroutine is running
	h := msg.PfcpMessage.Header
	key := pendingKey{peer: msg.RemoteAddr.String(), seq: h.SequenceNumber}
	pendingRx.Store(key, pfcplog.Record{
		Role:    pfcplog.RoleRx,
		MsgType: uint8(h.MessageType),
		Seq:     h.SequenceNumber,
		SEID:    h.SEID,
		Local:   Server.Conn.LocalAddr(),
		Peer:    msg.RemoteAddr,
		ReqRecv: pfcplog.Stamp(msg.RecvTime),
		ReqDeq:  deq,
	})
	dispatch(msg)
	// Still pending means the handler returned without answering.
	if v, ok := pendingRx.LoadAndDelete(key); ok {
		rec := v.(pfcplog.Record)
		rec.Done = pfcplog.Now()
		rec.Outcome = pfcplog.OutcomeNoRsp
		pfcplog.Log(rec)
	}
}

func ClosePfcp() error {
	smf_context.GetSelf().PfcpCancelFunc()

	closeErr := Server.Close()
	if closeErr != nil {
		logger.PfcpLog.Errorf("Pfcp close err: %+v", closeErr)
	} else {
		logger.PfcpLog.Infof("Pfcp server closed")
	}
	return closeErr
}
