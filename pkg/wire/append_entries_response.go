package wire

import (
	"github.com/elliotcourant/buffers"
	"github.com/hashicorp/raft"
)

type AppendEntriesResponse struct {
	raft.AppendEntriesResponse
	Error error
}

func (AppendEntriesResponse) Server() {}

func (AppendEntriesResponse) Raft() {}

func (i *AppendEntriesResponse) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendInt32(int32(i.ProtocolVersion))
	buf.AppendUint64(i.Term)
	buf.AppendUint64(i.LastLog)
	buf.AppendBool(i.Success)
	buf.AppendBool(i.NoRetryBackoff)
	buf.AppendError(i.Error)
	return buf.Bytes()
}

func (i *AppendEntriesResponse) Decode(src []byte) error {
	*i = AppendEntriesResponse{}
	buf := buffers.NewBytesReader(src)
	i.ProtocolVersion = raft.ProtocolVersion(buf.NextInt32())
	i.Term = buf.NextUint64()
	i.LastLog = buf.NextUint64()
	i.Success = buf.NextBool()
	i.NoRetryBackoff = buf.NextBool()
	i.Error = buf.NextError()
	return nil
}
