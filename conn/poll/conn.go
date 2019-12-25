package poll

import (
	"net"
)

type Conn struct {
	net.Conn
	fd int
}

func NewConn(conn net.Conn) (*Conn, error) {
	fd, err := SocketFD(conn)
	if err != nil {
		return nil, err
	}
	return &Conn{
		fd:   fd,
		Conn: conn,
	}, nil
}

// FD return the scoket fd
func (c *Conn) FD() int {
	return c.fd
}

// Close closes the Connection.
func (c *Conn) Close() error {
	/*
		if err := syscall.Close(c.fd); err != nil {
			return err
		}
	*/
	return c.Conn.Close()
}
