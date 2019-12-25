package mock

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/coreos/etcd/integration"
	"github.com/coreos/pkg/capnslog"
)

var (
	etcdAddrs []string
	mock      *MockPushd
)

func TestMain(m *testing.M) {
	t := &testing.T{}

	capnslog.SetFormatter(capnslog.NewDefaultFormatter(ioutil.Discard))
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1, ClientTLS: nil})
	etcdAddrs = clus.RandClient().Endpoints()
	mock = NewMockPushd(t, etcdAddrs, SetAuth("bifrost"))
	mock.Start(t)
	time.Sleep(time.Second)
	v := m.Run()
	mock.Stop(t)
	clus.Terminate(t)
	os.Exit(v)
}
