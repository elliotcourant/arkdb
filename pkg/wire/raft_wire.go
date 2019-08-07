package wire

import (
	"encoding/binary"
	"fmt"
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
	return nil
}

type raftClientWire struct {
	cr         *chunkreader.ChunkReader
	r          io.ReadCloser
	w          io.WriteCloser
	bodyLen    int
	msgType    serverMessageType
	partialMsg bool
}

func (r *raftClientWire) Send(msg ClientMessage) error {
	_, err := r.w.Write(writeWireMessage(msg))
	return err
}

func (r *raftClientWire) Receive() (ServerMessage, error) {
	if !r.partialMsg {
		header, err := r.cr.Next(5)
		if err != nil {
			return nil, err
		}

		r.msgType = header[0]
		r.bodyLen = int(binary.BigEndian.Uint32(header[1:])) - 4
		r.partialMsg = true
	}

	var msg ServerMessage
	switch r.msgType {
	case appendEntriesResponse:
		msg = nil
	default:
		return nil, fmt.Errorf("failed to handle server message of with header [%s]", string(r.msgType))
	}

	msgBody, err := r.cr.Next(r.bodyLen)
	if err != nil {
		return nil, err
	}

	r.partialMsg = false

	err = msg.Decode(msgBody)

	return msg, err
}

type raftWire struct {
	cr *chunkreader.ChunkReader
	w  io.Writer

	bodyLen    int
	msgType    messageType
	partialMsg bool
}

func NewRaftServ(r io.Reader, w io.Writer) (RaftWire, error) {
	cr := chunkreader.NewChunkReader(r)
	return &raftWire{cr: cr, w: w}, nil
}

func (b *raftWire) Send(msg RaftMessage) error {
	time.Sleep(1 * time.Millisecond)
	_, err := b.w.Write(msg.Encode())
	return err
}

func (b *raftWire) Receive() (RaftMessage, error) {
	if !b.partialMsg {
		header, err := b.cr.Next(5)
		if err != nil {
			return nil, err
		}

		b.msgType = header[0]
		b.bodyLen = int(binary.BigEndian.Uint32(header[1:])) - 4
		b.partialMsg = true
	}

	var msg RaftMessage
	switch b.msgType {

	// Append entries
	case raftAppendEntriesRequest:
		msg = &AppendEntriesRequest{}
	case RaftAppendEntriesResponse:
		msg = &AppendEntriesResponse{}
	// Request vote
	case RaftRequestVoteRequest:
		msg = &RequestVoteRequest{}
	case RaftRequestVoteResponse:
		msg = &RequestVoteResponse{}

	// Install snapshot
	case RaftInstallSnapshotRequest:
		msg = &InstallSnapshotRequest{}
	case RaftInstallSnapshotResponse:
		msg = &InstallSnapshotResponse{}

	case PgErrorResponse:
		msg = &ErrorResponse{}

	default:
		return nil, fmt.Errorf("unknown raft message type: %c", b.msgType)
	}

	msgBody, err := b.cr.Next(b.bodyLen)
	if err != nil {
		return nil, err
	}

	b.partialMsg = false

	err = msg.Decode(msgBody)

	return msg, err
}
