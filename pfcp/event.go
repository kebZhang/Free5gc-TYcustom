package pfcp

import (
	"net"
	"time" // [PFCPlog 1122]
)

type ReceiveEvent struct {
	Type       ReceiveEventType
	RemoteAddr *net.UDPAddr
	RcvMsg     *Message
	// RecvTime [PFCPlog 1122] TYcustom: when ReadFromUDP returned the datagram
	// that carried RcvMsg (pfcpUdp.PfcpServer.ReadFrom). Zero unless the server
	// has StampRecv set. Lets the goroutine waiting on a request learn when its
	// response reached this process, before the hand-off through EventChannel.
	RecvTime time.Time
}

type ReceiveEventType uint8

const (
	ReceiveEventTypeResendRequest ReceiveEventType = iota
	ReceiveEventTypeValidResponse
)
