package wire

type ErrorResponse struct {
	Error error
}

func (ErrorResponse) Server() {}

func (i *ErrorResponse) Encode() []byte {
	return nil
}

func (i *ErrorResponse) Decode(src []byte) error {
	return nil
}
