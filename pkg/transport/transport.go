package transport

import (
	"net"
)

type Transport interface {
	Accept() (net.Conn, error)
	Close() error
	Addr() net.Addr
}
