// Package pfcplog [PFCPlog 1122] writes one JSON line per PFCP request/response
// exchange between SMF and UPF to /tmp/PFCP_log.txt (override: PFCP_LOG_PATH),
// from this NF's point of view. It is the PFCP counterpart of HTTP_log.
//
// The same file, apart from srcNF, lives in both NFs/smf/internal/pfcplog and
// NFs/upf/internal/pfcplog. It must keep compiling with Go 1.21 (the UPF is
// built with the go1.21.8 toolchain of the official free5gc/upf:v3.4.3 image).
//
// Design, same as accesslog: the hot path only copies a fixed-size Record onto
// a buffered channel and never blocks (a full queue drops the record and counts
// it); one writer goroutine formats the lines and owns the file, so lines never
// interleave. Stamps are carried as UnixNano so a Record holds no time.Time.
//
// Join key between the two sides: direction + seq, NOT addresses. The PFCP
// sequence number is chosen by the side that sends the request and echoed in
// the response, so one exchange gives a "tx" line on the requester and an "rx"
// line on the responder that carry the same seq:
//
//	SMF tx <-> UPF rx   SMF's requests; seq from one SMF-process counter, from 1
//	UPF tx <-> SMF rx   UPF's Session Report Requests; the UPF's own counter, from 0
//
// The two directions are numbered independently and overlap, so the direction
// is part of the key. msg_type and seid (the SEID in the request header, which
// both sides log) are a cross-check, not the key.
//
// local/peer cannot be used to join: in the Gradiant chart both NFs listen on
// 0.0.0.0:8805, so local is "0.0.0.0:8805" on both sides, and the SMF reaches
// the UPF through the Service name free5gc-upf-pfcp, so the SMF's peer is that
// Service's address while the UPF's peer is the SMF pod's own address.
//
// seq is unique only within one SMF process: it restarts at 1 when the SMF
// restarts, while the UPF's append-only log may still hold lines from the
// previous SMF process. Truncate both PFCP_log files before each run.
// A retransmitted request is one tx line on the SMF (attempts > 1) but one rx
// line plus rx_dup line(s) on the UPF.
//
// Line kinds ("role"):
//
//	tx      this NF sent the request and waited for the response
//	        send_start  entered the send path, before the request is encoded
//	        req_written the first write of the request returned (kernel has it)
//	        rsp_recv    the receive goroutine's ReadFrom returned the response
//	        rsp_deq     UPF only: its single main loop took the response off rcvCh
//	        done        the requesting code has the response (or gave up)
//	        attempts    writes of the request, 1 = never retransmitted
//	rx      this NF received the request and answered it
//	        req_recv    the receive goroutine's ReadFrom returned the request
//	        req_deq     UPF: its main loop took the request off rcvCh;
//	                    SMF: the dispatch goroutine for it started
//	        rsp_start   the response send path was entered (handler built it)
//	        rsp_written the first write of the response returned
//	        done        UPF only: the handler returned to the main loop
//	rx_dup  UPF only: a retransmitted request, answered from the cached
//	        response without running the handler again (req_recv, req_deq, done)
//
// A zero stamp is written as "". outcome: ok | timeout | error | no_rsp.
package pfcplog

import (
	"bufio"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

// srcNF is the NF this binary runs as.
const srcNF = "UPF"

// Enabled is the compile-time switch. With it false, Log returns at once, no
// goroutine is started and no file is opened; call sites also test it so they
// take no clock readings.
const Enabled = true // [PFCPlog 1122] set to false to turn the PFCP log off

const (
	// 8192 records is several seconds of PFCP at the highest rates we run
	// (two exchanges per PDU session); kept small because the buffer is
	// allocated up front and counts toward this process's heap.
	queueCapacity    = 1 << 13
	writerBufferSize = 1 << 18
	flushInterval    = 200 * time.Millisecond

	envPath     = "PFCP_LOG_PATH"
	defaultPath = "/tmp/PFCP_log.txt"
)

// Role is the kind of line.
type Role uint8

const (
	RoleTx Role = iota + 1
	RoleRx
	RoleRxDup
)

// Outcome of the exchange as seen by this NF.
type Outcome uint8

const (
	OutcomeOK      Outcome = iota
	OutcomeTimeout         // tx: no response after every retransmission
	OutcomeError           // tx: send or receive failed; rx: handler/dispatch error
	OutcomeNoRsp           // rx: the handler returned without sending a response; rx_dup: nothing cached to resend
)

// Record is one line. Times are UnixNano, 0 = not reached.
type Record struct {
	Role     Role
	Outcome  Outcome
	MsgType  uint8 // request message type (29.244 Table 7.3-1)
	RspType  uint8 // response message type, 0 if none
	Attempts uint8
	Seq      uint32
	SEID     uint64 // SEID in the request header (0 if absent)
	Local    net.Addr
	Peer     net.Addr
	UEID     string // SMF only, "" when the message is not tied to a UE

	SendStart, ReqWritten, RspRecv, RspDeq int64 // tx
	ReqRecv, ReqDeq, RspStart, RspWritten  int64 // rx
	Done                                   int64
}

var (
	queue   chan Record
	dropped atomic.Uint64
)

func init() {
	if !Enabled {
		return
	}
	queue = make(chan Record, queueCapacity)
	go writerLoop()
}

// Now returns the stamp format used in Record.
func Now() int64 { return time.Now().UnixNano() }

// Stamp converts a time.Time (e.g. one handed back by the pfcp library) to the
// Record format; the zero Time stays 0.
func Stamp(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// Log queues r without blocking. If the queue is full the record is dropped and
// counted, so PFCP handling is never stalled by logging.
func Log(r Record) {
	if !Enabled {
		return
	}
	select {
	case queue <- r:
	default:
		dropped.Add(1)
	}
}

// Dropped reports how many records were lost to a full queue.
func Dropped() uint64 { return dropped.Load() }

func writerLoop() {
	path := os.Getenv(envPath)
	if path == "" {
		path = defaultPath
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		// Logging must never take the NF down: keep draining, write nothing.
		for range queue { //nolint:revive
		}
		return
	}
	w := bufio.NewWriterSize(f, writerBufferSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	line := make([]byte, 0, 640)
	for {
		select {
		case r := <-queue:
			line = appendRecord(line[:0], &r)
			_, _ = w.Write(line)
			for n := len(queue); n > 0; n-- {
				r = <-queue
				line = appendRecord(line[:0], &r)
				_, _ = w.Write(line)
			}
		case <-ticker.C:
			_ = w.Flush()
		}
	}
}

func appendRecord(b []byte, r *Record) []byte {
	b = append(b, `{"nf":"`...)
	b = append(b, srcNF...)
	b = append(b, `","role":"`...)
	b = append(b, roleName(r.Role)...)
	b = append(b, `","msg":"`...)
	b = append(b, msgName(r.MsgType)...)
	b = append(b, `","msg_type":`...)
	b = strconv.AppendUint(b, uint64(r.MsgType), 10)
	b = append(b, `,"seq":`...)
	b = strconv.AppendUint(b, uint64(r.Seq), 10)
	b = append(b, `,"seid":`...)
	b = strconv.AppendUint(b, r.SEID, 10)
	b = appendStr(b, "local", addrString(r.Local))
	b = appendStr(b, "peer", addrString(r.Peer))
	b = appendStr(b, "ue_id", r.UEID)
	switch r.Role {
	case RoleTx:
		b = appendTime(b, "send_start", r.SendStart)
		b = appendTime(b, "req_written", r.ReqWritten)
		b = appendTime(b, "rsp_recv", r.RspRecv)
		b = appendTime(b, "rsp_deq", r.RspDeq)
		b = appendTime(b, "done", r.Done)
		b = append(b, `,"attempts":`...)
		b = strconv.AppendUint(b, uint64(r.Attempts), 10)
	case RoleRx:
		b = appendTime(b, "req_recv", r.ReqRecv)
		b = appendTime(b, "req_deq", r.ReqDeq)
		b = appendTime(b, "rsp_start", r.RspStart)
		b = appendTime(b, "rsp_written", r.RspWritten)
		b = appendTime(b, "done", r.Done)
	case RoleRxDup:
		b = appendTime(b, "req_recv", r.ReqRecv)
		b = appendTime(b, "req_deq", r.ReqDeq)
		b = appendTime(b, "done", r.Done)
	}
	b = append(b, `,"rsp_msg_type":`...)
	b = strconv.AppendUint(b, uint64(r.RspType), 10)
	b = append(b, `,"outcome":"`...)
	b = append(b, outcomeName(r.Outcome)...)
	return append(b, "\"}\n"...)
}

// appendTime writes key as RFC3339Nano UTC, the same rendering as HTTP_log.
func appendTime(b []byte, key string, ns int64) []byte {
	b = append(b, `,"`...)
	b = append(b, key...)
	b = append(b, `":"`...)
	if ns != 0 {
		b = time.Unix(0, ns).UTC().AppendFormat(b, time.RFC3339Nano)
	}
	return append(b, '"')
}

func appendStr(b []byte, key, s string) []byte {
	b = append(b, `,"`...)
	b = append(b, key...)
	b = append(b, `":`...)
	return strconv.AppendQuote(b, s)
}

func addrString(a net.Addr) string {
	if a == nil {
		return ""
	}
	return a.String()
}

func roleName(r Role) string {
	switch r {
	case RoleTx:
		return "tx"
	case RoleRx:
		return "rx"
	case RoleRxDup:
		return "rx_dup"
	}
	return "unknown"
}

func outcomeName(o Outcome) string {
	switch o {
	case OutcomeOK:
		return "ok"
	case OutcomeTimeout:
		return "timeout"
	case OutcomeError:
		return "error"
	case OutcomeNoRsp:
		return "no_rsp"
	}
	return "unknown"
}

// msgName follows 3GPP TS 29.244 Table 7.3-1 (same codes as free5gc/pfcp and
// wmnsk/go-pfcp).
func msgName(t uint8) string {
	switch t {
	case 1:
		return "HeartbeatRequest"
	case 2:
		return "HeartbeatResponse"
	case 3:
		return "PFDManagementRequest"
	case 4:
		return "PFDManagementResponse"
	case 5:
		return "AssociationSetupRequest"
	case 6:
		return "AssociationSetupResponse"
	case 7:
		return "AssociationUpdateRequest"
	case 8:
		return "AssociationUpdateResponse"
	case 9:
		return "AssociationReleaseRequest"
	case 10:
		return "AssociationReleaseResponse"
	case 11:
		return "VersionNotSupportedResponse"
	case 12:
		return "NodeReportRequest"
	case 13:
		return "NodeReportResponse"
	case 14:
		return "SessionSetDeletionRequest"
	case 15:
		return "SessionSetDeletionResponse"
	case 50:
		return "SessionEstablishmentRequest"
	case 51:
		return "SessionEstablishmentResponse"
	case 52:
		return "SessionModificationRequest"
	case 53:
		return "SessionModificationResponse"
	case 54:
		return "SessionDeletionRequest"
	case 55:
		return "SessionDeletionResponse"
	case 56:
		return "SessionReportRequest"
	case 57:
		return "SessionReportResponse"
	}
	return "Unknown"
}
