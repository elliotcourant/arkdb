package wire

import (
	"github.com/hashicorp/raft"
)

type RequestVoteResponse struct {
	raft.RequestVoteResponse
	Error error
}

func (RequestVoteResponse) Server() {}

func (i *RequestVoteResponse) Encode() []byte {
	return nil
}

func (i *RequestVoteResponse) Decode(src []byte) error {
	return nil
}
