package mock

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/coreos/etcd/integration"
	"github.com/coreos/pkg/capnslog"
	connmock "github.com/meitu/bifrost/conn/mock"
	pushmock "github.com/meitu/bifrost/push/mock"
)

var (
	mqttAddr        []string
	publishAddr     string
	etcdAddrs       []string
	callbackService string
)

func TestMain(m *testing.M) {
	t := &testing.T{}
	capnslog.SetFormatter(capnslog.NewDefaultFormatter(ioutil.Discard))
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1, ClientTLS: nil})
	etcdAddrs = clus.RandClient().Endpoints()
	connm := connmock.NewMockConnd(t, etcdAddrs, connmock.SetAuth("bifrost"))
	mqttAddr = connm.MQTTAddr()

	pushm := pushmock.NewMockPushd(t, etcdAddrs, pushmock.SetServerAddr("127.0.0.1:3245"), pushmock.SetAuth("bifrost"))
	publishAddr = pushm.ServerAddr()
	callbackService = pushm.CallbackService()
	srv := NewMockBifrost(t, pushm, connm)
	srv.Serve(t)
	time.Sleep(time.Second * 3)
	v := m.Run()
	srv.Stop(t)
	clus.Terminate(t)
	os.Exit(v)
}
