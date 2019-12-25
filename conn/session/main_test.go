package session

import (
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/coreos/etcd/integration"
	"github.com/coreos/pkg/capnslog"
	"github.com/distributedio/configo"
	"github.com/meitu/bifrost/conn/conf"
	"github.com/meitu/bifrost/conn/context"
	"github.com/meitu/bifrost/conn/context/clients"
	"github.com/meitu/bifrost/conn/context/pubsub"
	"github.com/meitu/bifrost/conn/context/pushcli"
	"github.com/meitu/bifrost/conn/poll"
)

var (
	etcdAddrs     []string
	pubs          *pubsub.Pubsub
	ss            *Session
	SessionConfig conf.Session

	mctx *context.MqttSrvContext
	sctx *context.SessionContext
)

func TestMain(m *testing.M) {
	path := os.Getenv("GOPATH")
	path = path + "/src/github.com/meitu/bifrost/conn/conf/connd.toml"
	var config conf.Connd
	if err := configo.Load(path, &config); err != nil {
		panic(err)
	}
	config.Etcd = conf.Etcd{Cluster: etcdAddrs}
	config.Validate()

	t := &testing.T{}
	capnslog.SetFormatter(capnslog.NewDefaultFormatter(ioutil.Discard))
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1, ClientTLS: nil})
	etcdAddrs = clus.RandClient().Endpoints()
	cli := pushcli.NewMockPushCli(t, etcdAddrs)
	pubs = pubsub.NewPubsub(&config.Pubsub, context.Background(), cli.PushCli)
	for !cli.Ready() {
		time.Sleep(time.Millisecond * 10)
	}
	bctx := &context.BaseContext{
		Context: context.Background(),
		PushCli: cli.PushCli,
		Clients: clients.NewClients(),
		Pubsub:  pubs,
	}

	epoll, _ := poll.OpenPoll()
	mctx = &context.MqttSrvContext{
		BaseContext: bctx,

		Conf:     &config.MqttServer.Session,
		GrpcAddr: "grpcAddr",
		Poll:     epoll,
	}

	conn := &poll.Conn{Conn: &mockConn{}}

	sctx, _ = context.NewSessionContext(mctx, conn)

	config.MqttServer.Session.Auth = "bifrost"
	SessionConfig = config.MqttServer.Session

	ss = NewSession(mctx, conn)
	// cpu := packets.NewConnectPacket()
	// cpu.ProtocolName = "MQTT"
	// cpu.ProtocolVersion = 4
	// cpu.CleanSession = true
	// cpu.ClientIdentifier = "c"

	// ss = NewSession(mctx, &mockConn{})
	// ss.handleConnect(cpu)

	v := m.Run()
	clus.Terminate(t)
	// cli.Close()
	os.Exit(v)
}

type mockAddr struct {
}

func (n *mockAddr) Network() string {
	return ""
}

func (n *mockAddr) String() string {
	return "mockaddr"
}

type mockConn struct {
}

func (ms *mockConn) Read(b []byte) (n int, err error) {
	return 0, nil
}

func (ms *mockConn) Write(b []byte) (n int, err error) {
	return 0, nil

}

func (ms *mockConn) Close() error {
	return nil
}

func (ms *mockConn) LocalAddr() net.Addr {
	return &mockAddr{}
}

func (ms *mockConn) RemoteAddr() net.Addr {
	return &mockAddr{}
}

func (ms *mockConn) SetDeadline(t time.Time) error {
	return nil
}

func (ms *mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (ms *mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}
