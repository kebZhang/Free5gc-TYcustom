package pfcp

// [PFCPlog 1122] TYcustom: builds the UPF's PFCP_log lines. All three run on the
// PFCP main loop -- the only goroutine that touches rxTrans/txTrans and the
// stamps kept on the transactions -- so they read those fields without locks.
// Each call takes one clock reading (done) and queues one fixed-size record.

import (
	"github.com/wmnsk/go-pfcp/message"

	"github.com/free5gc/go-upf/internal/pfcplog"
)

// logRx: a request from the SMF that the handler ran for. done is taken after
// the handler returned, so rsp_written -> done is what the handler still did
// after the response was on the wire.
func (s *PfcpServer) logRx(msg message.Message, pkt *ReceivePacket, deq int64, rx *RxTransaction, err error) {
	rec := pfcplog.Record{
		Role:       pfcplog.RoleRx,
		MsgType:    msg.MessageType(),
		RspType:    rx.tyRspType,
		Seq:        msg.Sequence(),
		SEID:       msg.SEID(),
		Local:      s.conn.LocalAddr(),
		Peer:       pkt.RemoteAddr,
		ReqRecv:    pkt.RecvTime,
		ReqDeq:     deq,
		RspStart:   rx.tyRspStart,
		RspWritten: rx.tyRspWritten,
		Done:       pfcplog.Now(),
	}
	switch {
	case err != nil:
		rec.Outcome = pfcplog.OutcomeError
	case rx.tyRspWritten == 0:
		rec.Outcome = pfcplog.OutcomeNoRsp
	}
	pfcplog.Log(rec)
}

// logRxDup: a retransmission of a request already seen. The handler does not
// run again; rx.recv resends the cached response if there is one.
func (s *PfcpServer) logRxDup(msg message.Message, pkt *ReceivePacket, deq int64, hadRsp bool, err error) {
	rec := pfcplog.Record{
		Role:    pfcplog.RoleRxDup,
		MsgType: msg.MessageType(),
		Seq:     msg.Sequence(),
		SEID:    msg.SEID(),
		Local:   s.conn.LocalAddr(),
		Peer:    pkt.RemoteAddr,
		ReqRecv: pkt.RecvTime,
		ReqDeq:  deq,
		Done:    pfcplog.Now(),
	}
	switch {
	case err != nil:
		rec.Outcome = pfcplog.OutcomeError
	case !hadRsp:
		rec.Outcome = pfcplog.OutcomeNoRsp
	}
	pfcplog.Log(rec)
}

// logTx: the response to a request this UPF sent (in v3.4.3 only Session
// Report Requests). done is taken after rspDispacher returned.
func (s *PfcpServer) logTx(tx *TxTransaction, rsp message.Message, pkt *ReceivePacket, deq int64, err error) {
	rec := pfcplog.Record{
		Role:       pfcplog.RoleTx,
		RspType:    rsp.MessageType(),
		Attempts:   tx.retransCount + 1,
		Seq:        tx.seq,
		Local:      s.conn.LocalAddr(),
		Peer:       tx.raddr,
		SendStart:  tx.tySendStart,
		ReqWritten: tx.tyReqWritten,
		RspRecv:    pkt.RecvTime,
		RspDeq:     deq,
		Done:       pfcplog.Now(),
	}
	if tx.req != nil {
		rec.MsgType = tx.req.MessageType()
		rec.SEID = tx.req.SEID()
	}
	if err != nil {
		rec.Outcome = pfcplog.OutcomeError
	}
	pfcplog.Log(rec)
}
