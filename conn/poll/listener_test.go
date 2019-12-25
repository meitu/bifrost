package poll

import (
	"crypto/tls"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListen(t *testing.T) {
	ln, err := net.Listen("tcp", ":1341")
	if err != nil {
		panic(err)
	}

	lis := tls.NewListener(ln, nil)

	lln, err := NewListener(lis)
	if err != nil {
		panic(err)
	}

	assert.NotEqual(t, 0, lln.FD())
	assert.NoError(t, lln.Close())
}
