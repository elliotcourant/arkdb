package transport

import (
	"github.com/hashicorp/raft"
	"net"
	"time"
)

type Transport interface {
	Accept() (net.Conn, error)
	Close() error
	Addr() net.Addr
	Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error)
}
