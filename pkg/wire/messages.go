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
	appendEntriesRequest    clientMessageType = 'a'
	requestVoteRequest      clientMessageType = 'v'
	installSnapshotRequest  clientMessageType = 'i'
	discoveryRequest        clientMessageType = 'd'
	handshakeRequest        clientMessageType = 'h'
	applyTransactionRequest clientMessageType = 't'
)

const (
	appendEntriesResponse    serverMessageType = 'A'
	requestVoteResponse      serverMessageType = 'V'
	installSnapshotResponse  serverMessageType = 'I'
	discoveryResponse        serverMessageType = 'D'
	handshakeResponse        serverMessageType = 'H'
	applyTransactionResponse serverMessageType = 'T'

	errorResponse serverMessageType = 'E'
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

type RpcClientMessage interface {
	ClientMessage
	RPC()
}

type ServerMessage interface {
	Message
	Server()
}

type RaftServerMessage interface {
	ServerMessage
	Raft()
}

type RpcServerMessage interface {
	ServerMessage
	RPC()
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

type RpcClientWire interface {
	Send(msg RpcClientMessage) error
	Receive() (RpcServerMessage, error)
}

type RpcServerWire interface {
	Send(msg RpcServerMessage) error
	Receive() (RpcClientMessage, error)
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
	case *ApplyTransactionRequest:
		buf.AppendByte(applyTransactionRequest)

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
	case *ApplyTransactionResponse:
		buf.AppendByte(applyTransactionResponse)

	case *ErrorResponse:
		buf.AppendByte(errorResponse)
	default:
		panic(fmt.Sprintf("unrecognized message type for wire [%T]", msg))
	}

	buf.Append(msg.Encode()...)
	return buf.Bytes()
}
