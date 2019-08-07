package storage

import (
	"github.com/elliotcourant/buffers"
)

type Database struct {
	DatabaseID   uint8
	DatabaseName string
}

func (d Database) Path() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint8(MetaPrefix_Database)
	buf.AppendString(d.DatabaseName)
	buf.AppendUint8(d.DatabaseID)
	return buf.Bytes()
}
