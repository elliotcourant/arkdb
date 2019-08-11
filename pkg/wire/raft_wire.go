package wire

import (
	"fmt"
	"github.com/elliotcourant/buffers"
	"github.com/jackc/pgx/chunkreader"
	"io"
)

func NewRaftClientWire(r io.ReadCloser, w io.WriteCloser) (RaftClientWire, error) {
	wr := NewClientWire(r, w)

	err := wr.HandshakeRaft()
	if err != nil {
		return nil, err
	}

	return &raftClientWire{
		ClientWire: wr,
	}, nil
}

func NewRaftServerWire(r io.ReadCloser, w io.WriteCloser) RaftServerWire {
	return &raftServerWire{
		ServerWire: NewServerWire(r, w),
	}
}

type raftClientWire struct {
	ClientWire
}

func (r *raftClientWire) Send(msg RaftClientMessage) error {
	return r.ClientWire.Send(msg)
}

func (r *raftClientWire) Receive() (RaftServerMessage, error) {
	msg, err := r.ClientWire.Receive()
	if err != nil {
		return nil, err
	}
	raftMsg, ok := msg.(RaftServerMessage)
	if !ok {
		return nil, fmt.Errorf("expected raft server message, received [%T]", msg)
	}
	return raftMsg, nil
}

type raftServerWire struct {
	ServerWire
}

func (r *raftServerWire) Send(msg RaftServerMessage) error {
	return r.ServerWire.Send(msg)
}

func (r *raftServerWire) Receive() (RaftClientMessage, error) {
	msg, err := r.ServerWire.Receive()
	if err != nil {
		return nil, err
	}
	raftMsg, ok := msg.(RaftClientMessage)
	if !ok {
		return nil, fmt.Errorf("expected raft client message, received [%T]", msg)
	}
	return raftMsg, nil
}

func NewClientWire(r io.ReadCloser, w io.WriteCloser) ClientWire {
	cr := chunkreader.NewChunkReader(r)
	return &clientWire{
		cr: cr,
		r:  r,
		w:  w,
	}
}

func NewServerWire(r io.ReadCloser, w io.WriteCloser) ServerWire {
	cr := chunkreader.NewChunkReader(r)
	return &serverWire{
		cr: cr,
		r:  r,
		w:  w,
	}
}

type serverWire struct {
	cr         *chunkreader.ChunkReader
	r          io.ReadCloser
	w          io.WriteCloser
	bodyLen    int32
	msgType    clientMessageType
	partialMsg bool
}

func (r *serverWire) Send(msg ServerMessage) error {
	// time.Sleep(time.Millisecond * 1)
	_, err := r.w.Write(writeWireMessage(msg))
	return err
}

func (r *serverWire) Receive() (ClientMessage, error) {
	if !r.partialMsg {
		header, err := r.cr.Next(5)
		if err != nil {
			return nil, err
		}

		buf := buffers.NewBytesReader(header)
		r.msgType = buf.NextUint8()
		r.bodyLen = buf.NextInt32()
	}

	msgBody, err := r.cr.Next(int(r.bodyLen))
	if err != nil {
		return nil, err
	}

	var msg ClientMessage
	switch r.msgType {
	case appendEntriesRequest:
		msg = &AppendEntriesRequest{}
	case requestVoteRequest:
		msg = &RequestVoteRequest{}
	case installSnapshotRequest:
		msg = &InstallSnapshotRequest{}

	case handshakeRequest:
		msg = &HandshakeRequest{}

	case discoveryRequest:
		msg = &DiscoveryRequest{}

	default:
		return nil, fmt.Errorf("failed to handle client message of with header [%s]", string(r.msgType))
	}

	r.partialMsg = false

	err = msg.Decode(msgBody)

	return msg, err
}

type clientWire struct {
	cr         *chunkreader.ChunkReader
	r          io.ReadCloser
	w          io.WriteCloser
	bodyLen    int32
	msgType    serverMessageType
	partialMsg bool
}

func (r *clientWire) HandshakeRaft() error {
	return r.handshake(RaftIntention)
}

func (r *clientWire) HandshakeRpc() error {
	return r.handshake(RpcIntention)
}

func (r *clientWire) handshake(intention HandshakeIntention) error {
	if err := r.Send(&HandshakeRequest{
		Intention: intention,
	}); err != nil {
		return err
	}

	for {
		receivedMsg, err := r.Receive()
		if err != nil {
			return err
		}

		switch msg := receivedMsg.(type) {
		case *HandshakeResponse:
			return nil
		case *ErrorResponse:
			return msg.Error
		default:
			return fmt.Errorf("expected handshake response received [%T]", msg)
		}
	}
}

func (r *clientWire) Send(msg ClientMessage) error {
	// time.Sleep(time.Millisecond * 1)
	_, err := r.w.Write(writeWireMessage(msg))
	return err
}

func (r *clientWire) Receive() (ServerMessage, error) {
	if !r.partialMsg {
		header, err := r.cr.Next(5)
		if err != nil {
			return nil, err
		}

		buf := buffers.NewBytesReader(header)
		r.msgType = buf.NextUint8()
		r.bodyLen = buf.NextInt32()
	}

	msgBody, err := r.cr.Next(int(r.bodyLen))
	if err != nil {
		return nil, err
	}

	var msg ServerMessage
	switch r.msgType {
	case appendEntriesResponse:
		msg = &AppendEntriesResponse{}
	case requestVoteResponse:
		msg = &RequestVoteResponse{}
	case installSnapshotResponse:
		msg = &InstallSnapshotResponse{}
	case errorResponse:
		msg = &ErrorResponse{}

	case handshakeResponse:
		msg = &HandshakeResponse{}

	case discoveryResponse:
		msg = &DiscoveryResponse{}

	default:
		return nil, fmt.Errorf("failed to handle server message of with header [%s]", string(r.msgType))
	}

	r.partialMsg = false
	err = msg.Decode(msgBody)

	return msg, err
}
