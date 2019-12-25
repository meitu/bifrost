package callback

import (
	"fmt"
	"testing"
	"time"

	cb "github.com/meitu/bifrost/misc/fake/callback"
	"github.com/stretchr/testify/assert"
)

func TestCallbackDiscovery(t *testing.T) {
	fnames := "OnConnect"
	c1, err := cb.NewCallbackServer(etcdAddrs, service, "s1", fnames)
	assert.NoError(t, err)
	go func() {
		c1.Start("localhost:17746")
	}()
	time.Sleep(time.Second)
	fmt.Println(mockCallback.String())
	time.Sleep(time.Second)
	c1.Stop()
	time.Sleep(time.Second)
	fmt.Println(mockCallback.String())
}
