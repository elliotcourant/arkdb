package wire

import (
	"github.com/elliotcourant/buffers"
)

type NextObjectIdRequest struct {
	ObjectPath []byte
}

func (NextObjectIdRequest) Client() {}

func (NextObjectIdRequest) RPC() {}

func (i *NextObjectIdRequest) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.Append(i.ObjectPath...)
	return buf.Bytes()
}

func (i *NextObjectIdRequest) Decode(src []byte) error {
	*i = NextObjectIdRequest{}
	buf := buffers.NewBytesReader(src)
	i.ObjectPath = buf.NextBytes()
	return nil
}
