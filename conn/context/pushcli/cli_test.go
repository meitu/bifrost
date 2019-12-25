package pushcli

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPushcli(t *testing.T) {
	//全局公用一个
	cli := NewMockPushCli(t, etcdAddrs)
	for !cli.Ready() {
		time.Sleep(time.Microsecond)
	}
	addr := fmt.Sprintf("[\"%s\"]", cli.ServerAddr)
	assert.Equal(t, addr, cli.String())
	cli.Close()
	assert.Equal(t, ErrNoEndpoints.Error(), cli.String())
}
