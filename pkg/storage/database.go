package storage

import (
	"github.com/elliotcourant/arkdb/pkg/buffers"
)

type Database struct {
	DatabaseID   uint8
	DatabaseName string
}

func (d Database) Path() []byte {
	l := len(d.DatabaseName)
	buf := buffers.NewAllocatedBytesBuffer(3 + l)
	buf.Append(MetaPrefix_Database, uint8(l))
	buf.AppendString(d.DatabaseName)
	buf.Append(d.DatabaseID)
	return buf.Bytes()
}
