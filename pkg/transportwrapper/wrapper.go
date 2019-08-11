package transportwrapper

import (
	"github.com/elliotcourant/arkdb/pkg/transport"
	"github.com/elliotcourant/timber"
	"github.com/hashicorp/raft"
	"net"
	"time"
)

type TransportWrapper interface {
	NormalTransport() net.Listener
	ForwardToRaft(net.Conn, error)
	ForwardToRpc(net.Conn, error)
	RaftTransport() transport.Transport
	RpcTransport() transport.Transport
	Port() int
	Addr() net.Addr
	Close()
	SetNodeID(id raft.ServerID)
}

type accept struct {
	conn net.Conn
	err  error
}
type transportWrapperItem struct {
	listener      transport.Transport
	acceptChannel chan accept
	logger        timber.Logger

	closeCallback func()
}

func (t *transportWrapperItem) SendAccept(conn net.Conn, err error) {
	t.acceptChannel <- accept{conn, err}
}

func (t *transportWrapperItem) Accept() (net.Conn, error) {
	a := <-t.acceptChannel
	return a.conn, a.err
}

func (t *transportWrapperItem) Close() error {
	t.logger.Warningf("closing transport wrapper item")
	// close(t.acceptChannel)
	t.closeCallback()
	return t.listener.Close()
}

func (t *transportWrapperItem) Addr() net.Addr {
	return t.listener.Addr()
}

func (t *transportWrapperItem) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	t.logger.Warningf("dialing [%s] via transport wrapper item", address)
	return t.listener.Dial(address, timeout)
}

type transportWrapper struct {
	transport     transport.Transport
	raftTransport *transportWrapperItem
	rpcTransport  *transportWrapperItem
}

func (wrapper *transportWrapper) SetNodeID(id raft.ServerID) {
	wrapper.raftTransport.logger = wrapper.raftTransport.logger.Prefix(string(id))
	wrapper.rpcTransport.logger = wrapper.rpcTransport.logger.Prefix(string(id))
}

func NewTransportWrapperFromListener(listener net.Listener) TransportWrapper {
	ln := transport.NewTransportFromListener(listener)
	return NewTransportWrapperEx(ln)
}

func NewTransportWrapper(addr string) (TransportWrapper, error) {
	ln, err := transport.NewTransport(addr)
	if err != nil {
		return nil, err
	}
	return NewTransportWrapperEx(ln), nil
}

func NewTransportWrapperEx(listener transport.Transport) TransportWrapper {
	wrapper := &transportWrapper{
		transport: listener,
		raftTransport: &transportWrapperItem{
			acceptChannel: make(chan accept, 0),
			logger:        timber.New(),
		},
		rpcTransport: &transportWrapperItem{
			acceptChannel: make(chan accept, 0),
			logger:        timber.New(),
		},
	}

	{
		wrapper.raftTransport.closeCallback = wrapper.closeCallback
		wrapper.raftTransport.listener = wrapper.transport
	}

	{
		wrapper.rpcTransport.closeCallback = wrapper.closeCallback
		wrapper.rpcTransport.listener = wrapper.transport
	}

	return wrapper
}

func (wrapper *transportWrapper) ForwardToRaft(conn net.Conn, err error) {
	wrapper.raftTransport.SendAccept(conn, err)
}

func (wrapper *transportWrapper) ForwardToRpc(conn net.Conn, err error) {
	wrapper.rpcTransport.SendAccept(conn, err)
}

func (wrapper *transportWrapper) closeCallback() {
	wrapper.rpcTransport.logger.Verbosef("received close callback")
}

func (wrapper *transportWrapper) RaftTransport() transport.Transport {
	return wrapper.raftTransport
}

func (wrapper *transportWrapper) RpcTransport() transport.Transport {
	return wrapper.rpcTransport
}

func (wrapper *transportWrapper) NormalTransport() net.Listener {
	return wrapper.transport
}

func (wrapper *transportWrapper) Port() int {
	addr, _ := net.ResolveTCPAddr("tcp", wrapper.Addr().String())
	return addr.Port
}

func (wrapper *transportWrapper) Addr() net.Addr {
	return wrapper.transport.Addr()
}

func (wrapper *transportWrapper) Close() {
	wrapper.raftTransport.Close()
}
