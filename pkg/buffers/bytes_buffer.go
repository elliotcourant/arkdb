package buffers

import (
	"encoding/binary"
)

const (
	Uint8Size  = 1
	Uint16Size = 2
	Uint32Size = 4
	Uint64Size = 8
)

type BytesBuffer interface {
	Append(bytes ...byte)
	AppendUint16(item uint16)
	AppendUint32(item uint32)
	AppendUint64(item uint64)
	AppendInt16(item int16)
	AppendInt32(item int32)
	AppendInt64(item int64)
	AppendBool(item bool)
	AppendNil32()
	Bytes() []byte
}

func NewBytesBuffer() BytesBuffer {
	return &bytesBuffer{
		buf: make([]byte, 0),
	}
}

type bytesBuffer struct {
	buf []byte
}

func (b *bytesBuffer) Append(bytes ...byte) {
	b.buf = append(b.buf, bytes...)
}

func (b *bytesBuffer) AppendUint16(item uint16) {
	wp := len(b.buf)
	b.buf = append(b.buf, 0, 0)
	binary.BigEndian.PutUint16(b.buf[wp:], item)
}

func (b *bytesBuffer) AppendUint32(item uint32) {
	wp := len(b.buf)
	b.buf = append(b.buf, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(b.buf[wp:], item)
}

func (b *bytesBuffer) AppendUint64(item uint64) {
	wp := len(b.buf)
	b.buf = append(b.buf, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(b.buf[wp:], item)
}

func (b *bytesBuffer) AppendInt16(item int16) {
	b.AppendUint16(uint16(item))
}

func (b *bytesBuffer) AppendInt32(item int32) {
	b.AppendUint32(uint32(item))
}

func (b *bytesBuffer) AppendInt64(item int64) {
	b.AppendUint64(uint64(item))
}

func (b *bytesBuffer) AppendBool(item bool) {
	switch item {
	case true:
		b.AppendUint16(1)
	default:
		b.AppendUint16(0)
	}
}

func (b *bytesBuffer) AppendNil32() {
	b.AppendInt32(-1)
}

func (b *bytesBuffer) Bytes() []byte {
	return b.buf
}

func NewAllocatedBytesBuffer(size int) BytesBuffer {
	return &allocatedBuffer{
		buf:    make([]byte, size),
		offset: 1,
	}
}

type allocatedBuffer struct {
	buf    []byte
	offset int
}

func (b *allocatedBuffer) Append(bytes ...byte) {
	l := len(bytes)
	copy(b.buf[b.offset-1:b.offset-1+l], bytes)
	b.offset += l
}

func (b *allocatedBuffer) set(s, v []byte, l int) {
	for i := 0; i < l; i++ {
		s[i] = v[i]
	}
}

func (b *allocatedBuffer) AppendUint16(item uint16) {
	binary.BigEndian.PutUint16(b.buf[b.offset-1:b.offset-1+Uint16Size], item)
	b.offset += Uint16Size
}

func (b *allocatedBuffer) AppendUint32(item uint32) {
	binary.BigEndian.PutUint32(b.buf[b.offset-1:b.offset-1+Uint32Size], item)
	b.offset += Uint32Size
}

func (b *allocatedBuffer) AppendUint64(item uint64) {
	binary.BigEndian.PutUint64(b.buf[b.offset-1:b.offset-1+Uint64Size], item)
	b.offset += Uint64Size
}

func (b *allocatedBuffer) AppendInt16(item int16) {
	b.AppendUint16(uint16(item))
}

func (b *allocatedBuffer) AppendInt32(item int32) {
	b.AppendUint32(uint32(item))
}

func (b *allocatedBuffer) AppendInt64(item int64) {
	b.AppendUint64(uint64(item))
}

func (b *allocatedBuffer) AppendBool(item bool) {
	switch item {
	case true:
		b.AppendUint16(1)
	default:
		b.AppendUint16(0)
	}
}

func (b *allocatedBuffer) AppendNil32() {
	b.AppendInt32(-1)
}

func (b *allocatedBuffer) Bytes() []byte {
	return b.buf
}
