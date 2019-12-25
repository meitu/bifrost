package poll

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	clis    = make([]net.Conn, 0)
	message = []byte("hello world")
)

func TestConn(t *testing.T) {
	clis = make([]net.Conn, 0)
	addr := ":1345"
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}

	lln, err := NewListener(ln)
	if err != nil {
		panic(err)
	}
	go func() {
		conn, err := lln.Accept()
		if err != nil {
			panic(err)
		}
		conn.Write(message)
		time.Sleep(time.Second)

		assert.NotEqual(t, 0, lln.FD())
		assert.NoError(t, conn.Close())
		assert.NoError(t, lln.Close())
	}()
	ping(t, addr, 1)
	read(t, 1)

	time.Sleep(time.Second)
}

func ping(t *testing.T, addr string, count int) {
	for i := 0; i < count; i++ {
		cli, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			panic(err)
		}
		clis = append(clis, cli)
	}
}

func write(t *testing.T) {
	for _, cli := range clis {
		_, err := cli.Write(message)
		if err != nil {
			panic(err)
		}
	}
}

func read(t *testing.T, count int) {
	for i := 0; i < count; i++ {
		for _, cli := range clis {
			recv := make([]byte, len(message))
			//fmt.Println("cli recv")
			_, err := cli.Read(recv)
			if err != nil {
				panic(err)
			}
			//fmt.Println("cli recv", string(recv))
			assert.Equal(t, recv, message)
		}
	}
}

func closeall(t *testing.T) {
	for _, cli := range clis {
		cli.Close()
	}
}
