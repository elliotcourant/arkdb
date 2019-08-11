package wire

import (
	"github.com/elliotcourant/buffers"
	"github.com/hashicorp/raft"
)

type InstallSnapshotResponse struct {
	raft.InstallSnapshotResponse
	Error error
}

func (InstallSnapshotResponse) Server() {}

func (InstallSnapshotResponse) Raft() {}

func (i *InstallSnapshotResponse) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendInt32(int32(i.ProtocolVersion))
	buf.AppendUint64(i.Term)
	buf.AppendBool(i.Success)
	buf.AppendError(i.Error)
	return buf.Bytes()
}

func (i *InstallSnapshotResponse) Decode(src []byte) error {
	*i = InstallSnapshotResponse{}
	buf := buffers.NewBytesReader(src)
	i.ProtocolVersion = raft.ProtocolVersion(buf.NextInt32())
	i.Term = buf.NextUint64()
	i.Success = buf.NextBool()
	i.Error = buf.NextError()
	return nil
}
