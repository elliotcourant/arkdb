package wire

import (
	"bytes"
	"github.com/elliotcourant/buffers"
	"github.com/hashicorp/raft"
	"io"
)

type InstallSnapshotRequest struct {
	raft.InstallSnapshotRequest
	Snapshot []byte
}

func (i *InstallSnapshotRequest) Reader() io.Reader {
	return bytes.NewReader(i.Snapshot)
}

func (InstallSnapshotRequest) Client() {}

func (InstallSnapshotRequest) Raft() {}

func (i *InstallSnapshotRequest) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendInt32(int32(i.ProtocolVersion))
	buf.AppendInt32(int32(i.SnapshotVersion))
	buf.AppendUint64(i.Term)
	buf.Append(i.Leader...)
	buf.AppendUint64(i.LastLogIndex)
	buf.AppendUint64(i.LastLogTerm)
	buf.Append(i.Peers...)
	buf.Append(i.Configuration...)
	buf.AppendUint64(i.ConfigurationIndex)
	buf.AppendInt64(i.Size)
	buf.Append(i.Snapshot...)
	return buf.Bytes()
}

func (i *InstallSnapshotRequest) Decode(src []byte) error {
	*i = InstallSnapshotRequest{}
	buf := buffers.NewBytesReader(src)
	i.ProtocolVersion = raft.ProtocolVersion(buf.NextInt32())
	i.SnapshotVersion = raft.SnapshotVersion(buf.NextInt32())
	i.Term = buf.NextUint64()
	i.Leader = buf.NextBytes()
	i.LastLogIndex = buf.NextUint64()
	i.LastLogTerm = buf.NextUint64()
	i.Peers = buf.NextBytes()
	i.Configuration = buf.NextBytes()
	i.ConfigurationIndex = buf.NextUint64()
	i.Size = buf.NextInt64()
	i.Snapshot = buf.NextBytes()
	return nil
}
