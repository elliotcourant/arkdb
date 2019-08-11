package wire

import (
	"github.com/elliotcourant/buffers"
	"github.com/hashicorp/raft"
)

type DiscoveryRequest struct {
	NodeID raft.ServerID
}

func (DiscoveryRequest) Client() {}

func (i *DiscoveryRequest) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendString(string(i.NodeID))
	return buf.Bytes()
}

func (i *DiscoveryRequest) Decode(src []byte) error {
	*i = DiscoveryRequest{}
	buf := buffers.NewBytesReader(src)
	i.NodeID = raft.ServerID(buf.NextString())
	return nil
}
