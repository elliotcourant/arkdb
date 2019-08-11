package wire

import (
	"github.com/elliotcourant/buffers"
)

type ErrorResponse struct {
	Error error
}

func (ErrorResponse) Server() {}

func (i *ErrorResponse) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendError(i.Error)
	return buf.Bytes()
}

func (i *ErrorResponse) Decode(src []byte) error {
	*i = ErrorResponse{}
	buf := buffers.NewBytesReader(src)
	i.Error = buf.NextError()
	return nil
}
