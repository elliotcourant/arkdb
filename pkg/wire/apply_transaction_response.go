package wire

type ApplyTransactionResponse struct{}

func (ApplyTransactionResponse) Server() {}

func (ApplyTransactionResponse) RPC() {}

func (ApplyTransactionResponse) Encode() []byte {
	return make([]byte, 0)
}

func (ApplyTransactionResponse) Decode(src []byte) error {
	return nil
}
