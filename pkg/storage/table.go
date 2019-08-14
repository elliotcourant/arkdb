package storage

import (
	"github.com/elliotcourant/buffers"
)

type Table struct {
	TableID   uint8
	TableName string
}

func (i Table) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint8(i.TableID)
	buf.AppendString(i.TableName)
	return buf.Bytes()
}

func (i *Table) Decode(src []byte) error {
	*i = Table{}
	buf := buffers.NewBytesReader(src)
	i.TableID = buf.NextUint8()
	i.TableName = buf.NextString()
	return nil
}

func (i Table) Path() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendByte(MetaPrefix_Table)
	buf.AppendString(i.TableName)
	return buf.Bytes()
}

func (i Table) ObjectIdPrefix() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendByte(MetaPrefix_Table)
	return buf.Bytes()
}

func (i Table) Prefix() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendByte(MetaPrefix_Table)
	buf.AppendString(i.TableName)
	return buf.Bytes()
}
