package pushcli

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/distributedio/configo"
	"github.com/meitu/bifrost/conn/conf"
	"github.com/meitu/bifrost/push/mock"
	"github.com/stretchr/testify/assert"
)

// MockPushCli it is used of unit testing
// provide a complete pushd service
type MockPushCli struct {
	t       *testing.T
	pushsrv *mock.MockPushd
	*PushCli
	ServerAddr string
}

// NewMockPushCli new a pushcli object
func NewMockPushCli(t *testing.T, etcdAddrs []string) *MockPushCli {
	cli := &MockPushCli{}
	port := rand.Intn(65535)
	if port < 10000 {
		port += 10000
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cli.pushsrv = mock.NewMockPushd(t, etcdAddrs, mock.SetAuth("bifrost"), mock.SetServerAddr(addr))
	cli.pushsrv.Start(t)
	time.Sleep(time.Second)

	path := os.Getenv("GOPATH")
	path = path + "/src/github.com/meitu/bifrost/conn/conf/connd.toml"
	var config conf.Connd
	if err := configo.Load(path, &config); err != nil {
		fmt.Printf("load Config file failed , %s\n", err)
		os.Exit(1)
	}
	config.Etcd = conf.Etcd{Cluster: etcdAddrs}
	if err := config.Validate(); err != nil {
		panic(err)
	}

	pcli, err := NewClient(&config.Push)
	assert.NoError(t, err)
	cli.PushCli = pcli
	cli.t = t
	cli.ServerAddr = addr
	return cli
}

// Close close mockcli
func (cli *MockPushCli) Close() {
	cli.PushCli.Close()
	cli.pushsrv.Stop(cli.t)
}
