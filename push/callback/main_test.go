package callback

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/coreos/etcd/integration"
	"github.com/coreos/pkg/capnslog"
)

var (
	etcdAddrs    []string
	mockCallback Callback
)

func TestMain(m *testing.M) {
	t := &testing.T{}
	capnslog.SetFormatter(capnslog.NewDefaultFormatter(ioutil.Discard))
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1, ClientTLS: nil})
	etcdAddrs = clus.RandClient().Endpoints()
	mockCallback = NewMockCallback(etcdAddrs)
	time.Sleep(time.Millisecond * 100)
	v := m.Run()
	clus.Terminate(t)
	os.Exit(v)
}
