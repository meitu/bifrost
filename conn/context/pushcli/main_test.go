package pushcli

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/coreos/etcd/integration"
	"github.com/coreos/pkg/capnslog"
)

var etcdAddrs []string

// var pushcli *PushCli

func TestMain(m *testing.M) {
	t := &testing.T{}
	capnslog.SetFormatter(capnslog.NewDefaultFormatter(ioutil.Discard))
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1, ClientTLS: nil})
	etcdAddrs = clus.RandClient().Endpoints()
	// pushcli = NewMockPushCli(t, etcdAddrs)
	v := m.Run()
	clus.Terminate(t)
	os.Exit(v)
}
