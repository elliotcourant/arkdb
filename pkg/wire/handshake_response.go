package wire

type HandshakeResponse struct{}

func (HandshakeResponse) Server() {}

func (HandshakeResponse) Encode() []byte {
	return make([]byte, 0)
}

func (HandshakeResponse) Decode(src []byte) error {
	return nil
}
