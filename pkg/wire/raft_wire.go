package wire

import (
	"fmt"
	"github.com/elliotcourant/buffers"
	"github.com/jackc/pgx/chunkreader"
	"io"
	"time"
)

func NewClientWire(r io.ReadCloser, w io.WriteCloser) ClientWire {
	cr := chunkreader.NewChunkReader(r)
	return &raftClientWire{
		cr: cr,
		r:  r,
		w:  w,
	}
}

func NewServerWire(r io.ReadCloser, w io.WriteCloser) ServerWire {
	cr := chunkreader.NewChunkReader(r)
	return &raftServerWire{
		cr: cr,
		r:  r,
		w:  w,
	}
}

type raftServerWire struct {
	cr         *chunkreader.ChunkReader
	r          io.ReadCloser
	w          io.WriteCloser
	bodyLen    int32
	msgType    clientMessageType
	partialMsg bool
}

func (r *raftServerWire) Send(msg ServerMessage) error {
	time.Sleep(time.Millisecond * 1)
	_, err := r.w.Write(writeWireMessage(msg))
	return err
}

func (r *raftServerWire) Receive() (ClientMessage, error) {
	if !r.partialMsg {
		header, err := r.cr.Next(5)
		if err != nil {
			return nil, err
		}

		buf := buffers.NewBytesReader(header)
		r.msgType = buf.NextUint8()
		r.bodyLen = buf.NextInt32()
	}

	var msg ClientMessage
	switch r.msgType {
	case appendEntriesRequest:
		msg = &AppendEntriesRequest{}
	case requestVoteRequest:
		msg = &RequestVoteRequest{}
	case installSnapshotRequest:
		msg = &InstallSnapshotRequest{}
	default:
		return nil, fmt.Errorf("failed to handle client message of with header [%s]", string(r.msgType))
	}

	msgBody, err := r.cr.Next(int(r.bodyLen))
	if err != nil {
		return nil, err
	}

	r.partialMsg = false

	err = msg.Decode(msgBody)

	return msg, err
}

type raftClientWire struct {
	cr         *chunkreader.ChunkReader
	r          io.ReadCloser
	w          io.WriteCloser
	bodyLen    int32
	msgType    serverMessageType
	partialMsg bool
}

func (r *raftClientWire) Send(msg ClientMessage) error {
	time.Sleep(time.Millisecond * 1)
	_, err := r.w.Write(writeWireMessage(msg))
	return err
}

func (r *raftClientWire) Receive() (ServerMessage, error) {
	if !r.partialMsg {
		header, err := r.cr.Next(5)
		if err != nil {
			return nil, err
		}

		buf := buffers.NewBytesReader(header)
		r.msgType = buf.NextUint8()
		r.bodyLen = buf.NextInt32()
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
	default:
		return nil, fmt.Errorf("failed to handle server message of with header [%s]", string(r.msgType))
	}

	msgBody, err := r.cr.Next(int(r.bodyLen))
	if err != nil {
		return nil, err
	}

	r.partialMsg = false
	err = msg.Decode(msgBody)

	return msg, err
}
