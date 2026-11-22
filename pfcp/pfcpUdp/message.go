package pfcpUdp

import (
	"net"
	"time" // [PFCPlog 1122]

	"github.com/free5gc/pfcp"
)

type Message struct {
	RemoteAddr  *net.UDPAddr
	PfcpMessage *pfcp.Message
	// RecvTime [PFCPlog 1122] TYcustom: when ReadFromUDP returned this datagram.
	// Set by PfcpServer.ReadFrom only when the server has StampRecv; zero
	// otherwise, and zero on messages this process built itself.
	RecvTime time.Time
}

func NewMessage(remoteAddr *net.UDPAddr, pfcpMessage *pfcp.Message) (msg *Message) {
	return &Message{
		RemoteAddr:  remoteAddr,
		PfcpMessage: pfcpMessage,
	}
}

func (m *Message) MessageType() pfcp.MessageType {
	return m.PfcpMessage.Header.MessageType
}
