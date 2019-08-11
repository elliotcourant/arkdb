package wire

import (
	"github.com/elliotcourant/buffers"
	"github.com/hashicorp/raft"
)

type RequestVoteResponse struct {
	raft.RequestVoteResponse
	Error error
}

func (RequestVoteResponse) Server() {}

func (i *RequestVoteResponse) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendInt32(int32(i.ProtocolVersion))
	buf.AppendUint64(i.Term)
	buf.Append(i.Peers...)
	buf.AppendBool(i.Granted)
	return buf.Bytes()
}

func (i *RequestVoteResponse) Decode(src []byte) error {
	*i = RequestVoteResponse{}
	buf := buffers.NewBytesReader(src)
	i.ProtocolVersion = raft.ProtocolVersion(buf.NextInt32())
	i.Term = buf.NextUint64()
	i.Peers = buf.NextBytes()
	i.Granted = buf.NextBool()
	return nil
}
