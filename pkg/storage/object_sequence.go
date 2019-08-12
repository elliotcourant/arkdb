package storage

import (
	"github.com/elliotcourant/buffers"
)

type ObjectSequence struct {
	Key  []byte
	Next uint8
}

func (i ObjectSequence) Path() []byte {
	return append([]byte{MetaPrefix_ObjectSequence}, i.Key...)
}

func (i ObjectSequence) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint8(i.Next)
	return buf.Bytes()
}

func (i *ObjectSequence) Decode(src []byte) error {
	*i = ObjectSequence{}
	buf := buffers.NewBytesReader(src)
	i.Next = buf.NextUint8()
	return nil
}
