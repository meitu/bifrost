package poll

import (
	"net"
)

// Listener only TCP mode links are supported
type Listener struct {
	net.Listener
	fd int
}

func NewListener(ln net.Listener) (Listener, error) {
	fd, err := SocketFD(ln)
	if err != nil {
		return Listener{}, err
	}
	return Listener{
		Listener: ln,
		fd:       fd,
	}, nil
}

// Close closes the listener.
// Any blocked Accept operations will be unblocked and return errors.
func (ln *Listener) Close() error {
	/*
		if err := syscall.Close(ln.fd); err != nil {
			return err
		}
	*/
	return ln.Listener.Close()
}

// FD return the listener fd
func (ln *Listener) FD() int {
	return ln.fd
}

func (ln *Listener) Accept() (conn *Conn, err error) {
	con, err := ln.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return NewConn(con)
}
