package clients

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClients(t *testing.T) {
	clis := NewClients()
	var count int
	clis.Register("key", "value")
	clis.Exist("key")
	clis.Get("key")
	clis.Scan(func(cli interface{}) error { count++; return nil })
	clis.Deregister("key")
	assert.Equal(t, count, 1)
}
