package wire

import (
	"github.com/hashicorp/raft"
)

type InstallSnapshotRequest struct {
	raft.InstallSnapshotRequest
	Snapshot []byte
}

func (InstallSnapshotRequest) Client() {}

func (i *InstallSnapshotRequest) Encode() []byte {
	return nil
}

func (i *InstallSnapshotRequest) Decode(src []byte) error {
	return nil
}
