package wire

import (
	"github.com/hashicorp/raft"
)

type InstallSnapshotResponse struct {
	raft.InstallSnapshotResponse
	Error error
}

func (InstallSnapshotResponse) Server() {}

func (i *InstallSnapshotResponse) Encode() []byte {
	return nil
}

func (i *InstallSnapshotResponse) Decode(src []byte) error {
	return nil
}
