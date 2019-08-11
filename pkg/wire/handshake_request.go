package wire

import (
	"github.com/elliotcourant/buffers"
)

type HandshakeIntention = int32

const (
	RaftIntention HandshakeIntention = 1 << iota
	RpcIntention
)

type HandshakeRequest struct {
	Intention HandshakeIntention
}

func (HandshakeRequest) Client() {}

func (i *HandshakeRequest) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendInt32(i.Intention)
	return buf.Bytes()
}

func (i *HandshakeRequest) Decode(src []byte) error {
	*i = HandshakeRequest{}
	buf := buffers.NewBytesReader(src)
	i.Intention = buf.NextInt32()
	return nil
}
