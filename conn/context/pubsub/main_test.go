package pubsub

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/coreos/etcd/integration"
	"github.com/coreos/pkg/capnslog"
	"github.com/meitu/bifrost/conn/context/pushcli"
)

var etcdAddrs []string

var ctx context.Context
var pcli *pushcli.PushCli

func TestMain(m *testing.M) {

	t := &testing.T{}
	capnslog.SetFormatter(capnslog.NewDefaultFormatter(ioutil.Discard))
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1, ClientTLS: nil})
	etcdAddrs = clus.RandClient().Endpoints()
	cli := pushcli.NewMockPushCli(t, etcdAddrs)
	// pushcli.SetGlobalMockPushCli(t, etcdAddrs)

	ctx = context.Background()
	pcli = cli.PushCli

	for !cli.Ready() {
		time.Sleep(time.Microsecond)
	}
	v := m.Run()
	cli.Close()
	clus.Terminate(t)
	os.Exit(v)
}
