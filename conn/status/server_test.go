package status

import (
	"net"
	"testing"
	"time"

	"github.com/meitu/bifrost/conn/conf"
	"github.com/stretchr/testify/assert"
)

var config = &conf.Monitor{
	Listen: "localhost:1245",
}

func MockServer(t *testing.T) *Server {
	server, err := NewServer(config)
	assert.NoError(t, err)
	return server
}

func TestStop(t *testing.T) {
	server := MockServer(t)
	lis, err := net.Listen("tcp", config.Listen)
	assert.NoError(t, err)

	go func() {
		time.Sleep(time.Second)
		assert.NoError(t, server.Stop())
	}()

	err = server.Serve(lis)
	assert.Error(t, err)
}

func TestGracefulStop(t *testing.T) {
	server := MockServer(t)
	lis, err := net.Listen("tcp", config.Listen)
	assert.NoError(t, err)

	go func() {
		time.Sleep(time.Second)
		assert.NoError(t, server.GracefulStop())
	}()
	err = server.Serve(lis)
	assert.Error(t, err)
}
