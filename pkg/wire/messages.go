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
)

const (
	appendEntriesResponse   serverMessageType = 'A'
	requestVoteResponse     serverMessageType = 'V'
	installSnapshotResponse serverMessageType = 'I'
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

type ServerMessage interface {
	Message
	Server()
}

type ClientWire interface {
	Send(msg ClientMessage) error
	Receive() (ServerMessage, error)
}

type ServerWire interface {
	Send(msg ServerMessage) error
	Receive() (ClientMessage, error)
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

	case *AppendEntriesResponse:
		buf.AppendByte(appendEntriesResponse)
	case *RequestVoteResponse:
		buf.AppendByte(requestVoteResponse)
	case *InstallSnapshotResponse:
		buf.AppendByte(installSnapshotResponse)
	case *ErrorResponse:
		buf.AppendByte(errorResponse)
	default:
		panic(fmt.Sprintf("unrecognized message type for wire [%T]", msg))
	}

	buf.Append(msg.Encode()...)
	return buf.Bytes()
}
