package wire

import (
	"github.com/elliotcourant/buffers"
	"github.com/hashicorp/raft"
)

type RequestVoteRequest struct {
	raft.RequestVoteRequest
}

func (RequestVoteRequest) Client() {}

func (RequestVoteRequest) Raft() {}

func (i *RequestVoteRequest) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendInt32(int32(i.ProtocolVersion))
	buf.AppendUint64(i.Term)
	buf.Append(i.Candidate...)
	buf.AppendUint64(i.LastLogIndex)
	buf.AppendUint64(i.LastLogTerm)
	buf.AppendBool(i.LeadershipTransfer)
	return buf.Bytes()
}

func (i *RequestVoteRequest) Decode(src []byte) error {
	*i = RequestVoteRequest{}
	buf := buffers.NewBytesReader(src)
	i.ProtocolVersion = raft.ProtocolVersion(buf.NextInt32())
	i.Term = buf.NextUint64()
	i.Candidate = buf.NextBytes()
	i.LastLogIndex = buf.NextUint64()
	i.LastLogTerm = buf.NextUint64()
	i.LeadershipTransfer = buf.NextBool()
	return nil
}
