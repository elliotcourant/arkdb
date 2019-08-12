package wire

import (
	"github.com/elliotcourant/buffers"
)

type NextObjectIdResponse struct {
	Identity uint8
}

func (NextObjectIdResponse) Server() {}

func (NextObjectIdResponse) RPC() {}

func (i *NextObjectIdResponse) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint8(i.Identity)
	return buf.Bytes()
}

func (i *NextObjectIdResponse) Decode(src []byte) error {
	*i = NextObjectIdResponse{}
	buf := buffers.NewBytesReader(src)
	i.Identity = buf.NextUint8()
	return nil
}
