package wire

import (
	"github.com/hashicorp/raft"
)

type RequestVoteRequest struct {
	raft.RequestVoteRequest
}

func (RequestVoteRequest) Client() {}

func (i *RequestVoteRequest) Encode() []byte {
	return nil
}

func (i *RequestVoteRequest) Decode(src []byte) error {
	return nil
}
