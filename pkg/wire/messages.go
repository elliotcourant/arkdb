package wire

import (
	"fmt"
	"github.com/elliotcourant/buffers"
)

type (
	clientMessageType = byte
	serverMessageType = byte
)

// Client Message Types
const (
	appendEntriesRequest   clientMessageType = 'a'
	requestVoteRequest     clientMessageType = 'v'
	installSnapshotRequest clientMessageType = 'i'
	discoveryRequest       clientMessageType = 'd'
	handshakeRequest       clientMessageType = 'h'
)

const (
	appendEntriesResponse   serverMessageType = 'A'
	requestVoteResponse     serverMessageType = 'V'
	installSnapshotResponse serverMessageType = 'I'
	discoveryResponse       serverMessageType = 'D'
	handshakeResponse       serverMessageType = 'H'
	errorResponse           serverMessageType = 'E'
)

type Message interface {
	Encode() []byte
	Decode(src []byte) error
}

type ClientMessage interface {
	Message
	Client()
}

type RaftClientMessage interface {
	ClientMessage
	Raft()
}

type ServerMessage interface {
	Message
	Server()
}

type RaftServerMessage interface {
	ServerMessage
	Raft()
}

type ClientWire interface {
	Send(msg ClientMessage) error
	Receive() (ServerMessage, error)
	HandshakeRaft() error
	HandshakeRpc() error
}

type ServerWire interface {
	Send(msg ServerMessage) error
	Receive() (ClientMessage, error)
}

type RaftClientWire interface {
	Send(msg RaftClientMessage) error
	Receive() (RaftServerMessage, error)
}

type RaftServerWire interface {
	Send(msg RaftServerMessage) error
	Receive() (RaftClientMessage, error)
}

func writeWireMessage(msg Message) []byte {
	buf := buffers.NewBytesBuffer()
	switch msg.(type) {
	case *AppendEntriesRequest:
		buf.AppendByte(appendEntriesRequest)
	case *RequestVoteRequest:
		buf.AppendByte(requestVoteRequest)
	case *InstallSnapshotRequest:
		buf.AppendByte(installSnapshotRequest)
	case *DiscoveryRequest:
		buf.AppendByte(discoveryRequest)
	case *HandshakeRequest:
		buf.AppendByte(handshakeRequest)

	case *AppendEntriesResponse:
		buf.AppendByte(appendEntriesResponse)
	case *RequestVoteResponse:
		buf.AppendByte(requestVoteResponse)
	case *InstallSnapshotResponse:
		buf.AppendByte(installSnapshotResponse)
	case *DiscoveryResponse:
		buf.AppendByte(discoveryResponse)
	case *HandshakeResponse:
		buf.AppendByte(handshakeResponse)
	case *ErrorResponse:
		buf.AppendByte(errorResponse)
	default:
		panic(fmt.Sprintf("unrecognized message type for wire [%T]", msg))
	}

	buf.Append(msg.Encode()...)
	return buf.Bytes()
}
