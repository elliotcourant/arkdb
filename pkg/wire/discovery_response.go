package wire

import (
	"github.com/elliotcourant/buffers"
	"github.com/hashicorp/raft"
)

type DiscoveryResponse struct {
	NodeID    raft.ServerID
	IsNewNode bool
	Peers     []raft.ServerAddress
	Leader    raft.ServerAddress
}

func (DiscoveryResponse) Server() {}

func (DiscoveryResponse) RPC() {}

func (i *DiscoveryResponse) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendString(string(i.NodeID))
	buf.AppendBool(i.IsNewNode)
	if i.Peers == nil {
		buf.AppendInt32(-1)
	} else {
		buf.AppendInt32(int32(len(i.Peers)))
		for _, peer := range i.Peers {
			buf.AppendString(string(peer))
		}
	}
	buf.AppendString(string(i.Leader))
	return buf.Bytes()
}

func (i *DiscoveryResponse) Decode(src []byte) error {
	*i = DiscoveryResponse{}
	buf := buffers.NewBytesReader(src)
	i.NodeID = raft.ServerID(buf.NextString())
	i.IsNewNode = buf.NextBool()
	length := buf.NextInt32()
	if length == -1 {
		i.Peers = nil
	} else {
		i.Peers = make([]raft.ServerAddress, length)
		for x := 0; int32(x) < length; x++ {
			i.Peers[x] = raft.ServerAddress(buf.NextString())
		}
	}
	i.Leader = raft.ServerAddress(buf.NextString())
	return nil
}
