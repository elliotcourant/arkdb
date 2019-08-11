package wire

import (
	"github.com/elliotcourant/arkdb/pkg/storage"
	"github.com/elliotcourant/buffers"
	"github.com/hashicorp/raft"
)

type ApplyTransactionRequest struct {
	NodeID      raft.ServerID
	Transaction *storage.Transaction
}

func (ApplyTransactionRequest) Client() {}

func (ApplyTransactionRequest) RPC() {}

func (i *ApplyTransactionRequest) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendString(string(i.NodeID))
	buf.Append(i.Transaction.Encode()...)
	return buf.Bytes()
}

func (i *ApplyTransactionRequest) Decode(src []byte) error {
	*i = ApplyTransactionRequest{
		Transaction: &storage.Transaction{},
	}
	buf := buffers.NewBytesReader(src)
	i.NodeID = raft.ServerID(buf.NextString())
	return i.Transaction.Decode(buf.NextBytes())
}
