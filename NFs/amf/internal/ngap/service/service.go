package service

import (
	"encoding/hex"
	"io"
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/free5gc/amf/internal/logger"
	ngap_internal "github.com/free5gc/amf/internal/ngap"
	"github.com/free5gc/amf/internal/recvtime"
	"github.com/free5gc/amf/pkg/factory"
	"github.com/free5gc/ngap"
	"github.com/free5gc/sctp"
)

type NGAPHandler struct {
	HandleMessage         func(conn net.Conn, msg []byte)
	HandleNotification    func(conn net.Conn, notification sctp.Notification)
	HandleConnectionError func(conn net.Conn)
}

const (
	notimeout   int    = -1
	readBufSize uint32 = 262144
)

// set default read timeout to 2 seconds
var readTimeout syscall.Timeval = syscall.Timeval{Sec: 2, Usec: 0}

var (
	sctpListener *sctp.SCTPListener
	connections  sync.Map
	// activeConns tracks the number of live SCTP associations. It is the signal
	// used to auto-detect the dGNB scenario (>= 2 associations). Maintained next
	// to connections so it stays O(1) on the dispatch hot path.
	activeConns int32
)

func NewSctpConfig(cfg *factory.Sctp) *sctp.SocketConfig {
	sctpConfig := &sctp.SocketConfig{
		InitMsg: sctp.InitMsg{
			NumOstreams:    uint16(cfg.NumOstreams),
			MaxInstreams:   uint16(cfg.MaxInstreams),
			MaxAttempts:    uint16(cfg.MaxAttempts),
			MaxInitTimeout: uint16(cfg.MaxInitTimeout),
		},
		RtoInfo:   &sctp.RtoInfo{SrtoAssocID: 0, SrtoInitial: 500, SrtoMax: 1500, StroMin: 100},
		AssocInfo: &sctp.AssocInfo{AsocMaxRxt: 4},
	}
	return sctpConfig
}

func Run(addresses []string, port int, handler NGAPHandler, sctpConfig *sctp.SocketConfig) {
	ips := []net.IPAddr{}

	for _, addr := range addresses {
		if netAddr, err := net.ResolveIPAddr("ip", addr); err != nil {
			logger.NgapLog.Errorf("Error resolving address '%s': %v\n", addr, err)
		} else {
			logger.NgapLog.Debugf("Resolved address '%s' to %s\n", addr, netAddr)
			ips = append(ips, *netAddr)
		}
	}

	addr := &sctp.SCTPAddr{
		IPAddrs: ips,
		Port:    port,
	}

	go listenAndServe(addr, handler, sctpConfig)
}

func listenAndServe(addr *sctp.SCTPAddr, handler NGAPHandler, sctpConfig *sctp.SocketConfig) {
	defer func() {
		if p := recover(); p != nil {
			// Print stack for panic to log. Fatalf() will let program exit.
			logger.NgapLog.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
		}
	}()

	if sctpConfig == nil {
		logger.NgapLog.Errorf("Error sctp SocketConfig is nil")
		return
	}

	if listener, err := sctpConfig.Listen("sctp", addr); err != nil {
		logger.NgapLog.Errorf("Failed to listen: %+v", err)
		return
	} else {
		sctpListener = listener
	}

	logger.NgapLog.Infof("Listen on %s", sctpListener.Addr())

	for {
		newConn, err := sctpListener.AcceptSCTP(notimeout)
		if err != nil {
			switch err {
			case syscall.EINTR, syscall.EAGAIN:
				logger.NgapLog.Debugf("AcceptSCTP: %+v", err)
			default:
				logger.NgapLog.Errorf("Failed to accept: %+v", err)
			}
			continue
		}

		var info *sctp.SndRcvInfo
		if infoTmp, errGetDefaultSentParam := newConn.GetDefaultSentParam(); errGetDefaultSentParam != nil {
			logger.NgapLog.Errorf("Get default sent param error: %+v, accept failed", errGetDefaultSentParam)
			if errGetDefaultSentParam = newConn.Close(); errGetDefaultSentParam != nil {
				logger.NgapLog.Errorf("Close error: %+v", errGetDefaultSentParam)
			}
			continue
		} else {
			info = infoTmp
			logger.NgapLog.Debugf("Get default sent param[value: %+v]", info)
		}

		info.PPID = ngap.PPID
		if errSetDefaultSentParam := newConn.SetDefaultSentParam(info); errSetDefaultSentParam != nil {
			logger.NgapLog.Errorf("Set default sent param error: %+v, accept failed", errSetDefaultSentParam)
			if errSetDefaultSentParam = newConn.Close(); errSetDefaultSentParam != nil {
				logger.NgapLog.Errorf("Close error: %+v", errSetDefaultSentParam)
			}
			continue
		} else {
			logger.NgapLog.Debugf("Set default sent param[value: %+v]", info)
		}

		events := sctp.SCTP_EVENT_DATA_IO | sctp.SCTP_EVENT_SHUTDOWN | sctp.SCTP_EVENT_ASSOCIATION
		if errSubscribeEvents := newConn.SubscribeEvents(events); errSubscribeEvents != nil {
			logger.NgapLog.Errorf("Failed to accept: %+v", errSubscribeEvents)
			if errSubscribeEvents = newConn.Close(); errSubscribeEvents != nil {
				logger.NgapLog.Errorf("Close error: %+v", errSubscribeEvents)
			}
			continue
		} else {
			logger.NgapLog.Debugln("Subscribe SCTP event[DATA_IO, SHUTDOWN_EVENT, ASSOCIATION_CHANGE]")
		}

		if errSetReadBuffer := newConn.SetReadBuffer(int(readBufSize)); errSetReadBuffer != nil {
			logger.NgapLog.Errorf("Set read buffer error: %+v, accept failed", errSetReadBuffer)
			if errSetReadBuffer = newConn.Close(); errSetReadBuffer != nil {
				logger.NgapLog.Errorf("Close error: %+v", errSetReadBuffer)
			}
			continue
		} else {
			logger.NgapLog.Debugf("Set read buffer to %d bytes", readBufSize)
		}

		if errSetReadTimeout := newConn.SetReadTimeout(readTimeout); errSetReadTimeout != nil {
			logger.NgapLog.Errorf("Set read timeout error: %+v, accept failed", errSetReadTimeout)
			if errSetReadTimeout = newConn.Close(); errSetReadTimeout != nil {
				logger.NgapLog.Errorf("Close error: %+v", errSetReadTimeout)
			}
			continue
		} else {
			logger.NgapLog.Debugf("Set read timeout: %+v", readTimeout)
		}
		logger.NgapLog.Infof("[AMF] SCTP Accept from: %+v", newConn.RemoteAddr())
		connections.Store(newConn, newConn)

		// Detect dGNB the moment associations are established, before any NGAP
		// message (including NGSetup) arrives: once >= 2 SCTP associations are
		// accepted, the scheduler latches into per-association routing. This does
		// not depend on any UE registration being sent.
		nowActive := atomic.AddInt32(&activeConns, 1)
		if scheduler, errSched := ngap_internal.GetScheduler(); errSched == nil {
			scheduler.NotifyActiveConns(int(nowActive))
		}

		go handleConnection(newConn, readBufSize, handler)
	}
}

func Stop() {
	logger.NgapLog.Infof("Close SCTP server...")
	if err := sctpListener.Close(); err != nil {
		logger.NgapLog.Error(err)
		logger.NgapLog.Infof("SCTP server may not close normally.")
	}

	connections.Range(func(key, value interface{}) bool {
		conn := value.(net.Conn)
		if err := conn.Close(); err != nil {
			logger.NgapLog.Error(err)
		}
		return true
	})

	logger.NgapLog.Infof("SCTP server closed")
}

func handleConnection(conn *sctp.SCTPConn, bufsize uint32, handler NGAPHandler) {
	defer func() {
		if p := recover(); p != nil {
			// Print stack for panic to log. Fatalf() will let program exit.
			logger.NgapLog.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
		}

		// if AMF call Stop(), then conn.Close() will return EBADF because conn has been closed inside Stop()
		if err := conn.Close(); err != nil && err != syscall.EBADF {
			logger.NgapLog.Errorf("close connection error: %+v", err)
		}
		connections.Delete(conn)
		atomic.AddInt32(&activeConns, -1)

		// Drop this SCTP association's worker mapping so it does not leak.
		// (No-op in non-dGNB mode where the map is unused.)
		if scheduler, err := ngap_internal.GetScheduler(); err == nil {
			scheduler.ReleaseConn(conn)
		}
	}()

	for {
		buf := make([]byte, bufsize)

		n, info, notification, err := conn.SCTPRead(buf)
		// Capture the SCTP-read time as early as possible (memory only). It is
		// carried with the message and only written to AMF_log later, once the
		// NAS layer has confirmed the message type. Never falls on the I/O path.
		recvTime := time.Now()
		if err != nil {
			switch err {
			case io.EOF, io.ErrUnexpectedEOF:
				logger.NgapLog.Debugln("Read EOF from client")
				handler.HandleConnectionError(conn)
				return
			case syscall.EAGAIN:
				logger.NgapLog.Debugln("SCTP read timeout")
				continue
			case syscall.EINTR:
				logger.NgapLog.Debugf("SCTPRead: %+v", err)
				continue
			default:
				logger.NgapLog.Errorf(
					"Handle connection[addr: %+v] error: %+v",
					conn.RemoteAddr(),
					err,
				)
				handler.HandleConnectionError(conn)
				return
			}
		}

		if notification != nil {
			if handler.HandleNotification != nil {
				handler.HandleNotification(conn, notification)
			} else {
				logger.NgapLog.Warnf("Received sctp notification[type 0x%x] but not handled", notification.Type())
			}
		} else {
			if info == nil || info.PPID != ngap.PPID {
				logger.NgapLog.Warnln("Received SCTP PPID != 60, discard this packet")
				continue
			}

			logger.NgapLog.Tracef("Read %d bytes", n)
			logger.NgapLog.Tracef("Packet content:\n%+v", hex.Dump(buf[:n]))

			// Dispatch message through worker pool for parallel processing
			dispatchToWorkerPool(conn, buf[:n], recvTime, handler)
		}
	}
}

// dispatchToWorkerPool routes the NGAP message to the worker pool. The
// scheduler auto-selects its routing strategy:
//   - non-dGNB (single SCTP association): hash by the extracted UE ID, so
//     different UEs of the same gNB run in parallel (the original behaviour).
//   - dGNB (>= 2 associations, latched): pin by SCTP association, so each gNB's
//     messages stay ordered on one worker and the (non-unique) RAN-UE-NGAP-ID
//     is not used for routing.
//
// The dGNB scenario is detected at SCTP accept time (see listenAndServe), so by
// the time any NGAP message arrives the mode is already latched; this function
// only reads the latched decision and never flips it mid-association.
func dispatchToWorkerPool(conn net.Conn, msg []byte, recvTime time.Time, handler NGAPHandler) {
	scheduler, err := ngap_internal.GetScheduler()
	if err != nil {
		// Fallback to direct handling if scheduler is not initialized
		logger.NgapLog.Warnf("Scheduler not initialized, falling back to sequential processing: %v", err)
		// Stash the read time for the (synchronous) handler on this goroutine so
		// the NAS layer can attach it to AMF_log, matching the worker path.
		recvtime.Set(recvTime)
		handler.HandleMessage(conn, msg)
		recvtime.Clear()
		return
	}

	// Extract the UE ID; only used by the non-dGNB (hash-by-UE) routing branch.
	// For non-UE messages (e.g. NGSetupRequest) or on failure, fall back to 0
	// so such connection-level messages share a fixed worker, as before.
	ueID, found := ngap_internal.ExtractUEID(msg)
	if !found {
		ueID = 0
	}

	task := ngap_internal.Task{
		UEID:     ueID,
		Conn:     conn,
		Message:  msg,
		RecvTime: recvTime,
	}

	// Attempt to dispatch to worker pool
	if !scheduler.DispatchTask(task) {
		logger.NgapLog.Warnf("Drop packet from %v (UE ID %d, Scheduler is shutting down)",
			conn.RemoteAddr(), ueID)
	}
}
