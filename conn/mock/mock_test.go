package mock

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/coreos/etcd/integration"
	"github.com/coreos/pkg/capnslog"
)

func TestMock(t *testing.T) {
	capnslog.SetFormatter(capnslog.NewDefaultFormatter(ioutil.Discard))
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	etcdAddrs := clus.RandClient().Endpoints()
	mock := NewMockConnd(t, etcdAddrs)
	mock.Start(t)
	time.Sleep(time.Millisecond * 100)
	mock.Stop(t)
	clus.Terminate(t)
}
