package distribution

import (
	"fmt"
	"github.com/elliotcourant/arkdb/pkg/transport"
	"github.com/elliotcourant/arkdb/pkg/transportwrapper"
	"github.com/elliotcourant/arkdb/pkg/wire"
	"github.com/elliotcourant/timber"
	"net"
)

type masterServer struct {
	boat   *boat
	logger timber.Logger
	ln     transportwrapper.TransportWrapper
}

func (i *masterServer) runMasterServer() {
	go func(i *masterServer) {
		t := i.ln.NormalTransport()
		for {
			conn, err := t.Accept()
			if err != nil {
				i.logger.Errorf("failed to accept connection: %v", err)
			}

			go func(i *masterServer, conn net.Conn) {
				if err := i.handleMasterConn(conn); err != nil {
					i.logger.Errorf("failed to handle connection: %v", err)
				}
			}(i, conn)
		}
	}(i)
}

func (i *masterServer) handleMasterConn(conn net.Conn) error {
	w := wire.NewServerWire(conn, conn)

	receivedMsg, err := w.Receive()
	if err != nil {
		return err
	}

	switch msg := receivedMsg.(type) {
	case *wire.HandshakeRequest:
		switch msg.Intention {
		case wire.RaftIntention:
			if err := w.Send(&wire.HandshakeResponse{}); err != nil {
				return err
			}
			i.ln.ForwardToRaft(conn, nil)
		case wire.RpcIntention:
			if err := w.Send(&wire.HandshakeResponse{}); err != nil {
				return err
			}
			i.ln.ForwardToRpc(conn, nil)
		default:
			e := fmt.Errorf("invalid intention received [%d]", msg.Intention)
			if err := w.Send(&wire.ErrorResponse{
				Error: e,
			}); err != nil {
				return err
			}
			return e
		}
	default:
		return fmt.Errorf("invalid startup message received [%T]", msg)
	}

	return nil
}

type boatServer struct {
	boat   *boat
	logger timber.Logger
	ln     transport.Transport
}

func (i *boatServer) runBoatServer() {
	go func(i *boatServer) {
		for {
			conn, err := i.ln.Accept()
			if err != nil {
				i.logger.Errorf("failed to accept connection: %v", err)
			}

			go func(i *boatServer, conn net.Conn) {
				if err := i.handleConn(conn); err != nil {
					i.logger.Errorf("failed to handle connection: %v", err)
				}
			}(i, conn)
		}
	}(i)
}

func (i *boatServer) handleConn(conn net.Conn) error {
	return nil
}
