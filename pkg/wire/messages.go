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
	appendEntriesRequest clientMessageType = 'a'
)

const (
	appendEntriesResponse serverMessageType = 'A'
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
	default:
		panic(fmt.Sprintf("unrecognized message type for wire [%T]", msg))
	}

	buf.Append(msg.Encode()...)
	return buf.Bytes()
}
