package mock

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"sync"
	"testing"

	"github.com/distributedio/configo"
	"github.com/meitu/bifrost/commons/log"
	"github.com/meitu/bifrost/push"
	"github.com/meitu/bifrost/push/conf"
	smock "github.com/meitu/bifrost/push/store/mock"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

var path string

type MockPushd struct {
	server *push.Server
	wg     sync.WaitGroup

	Config *conf.Pushd
}

type Opt func(conf *conf.Pushd)

func SetAuth(auth string) Opt {
	return func(c *conf.Pushd) {
		c.Server.Publish.Auth = auth
	}
}

func SetServerAddr(addr string) Opt {
	return func(c *conf.Pushd) {
		c.Server.Listen = addr
	}
}

func SetServerConfig(path string) {
	path = path
}

func NewMockPushd(t *testing.T, etcdAddrs []string, opts ...Opt) *MockPushd {
	//默认mock没有日志
	initLogger()
	if path == "" {
		path = os.Getenv("GOPATH")
		path = path + "/src/github.com/meitu/bifrost/push/conf/pushd.toml"
	}

	var config conf.Pushd
	if err := configo.Load(path, &config); err != nil {
		fmt.Printf("load config file failed , %s\n", err)
		os.Exit(1)
	}

	for _, o := range opts {
		o(&config)
	}

	config.Server.Etcd.Cluster = etcdAddrs
	config.Server.Store.Tikv.Addrs = smock.MockAddr
	config.ValidateConf()

	server, err := push.NewServer(&config.Server)
	assert.NoError(t, err)
	return &MockPushd{
		server: server,
		Config: &config,
	}
}

func (p *MockPushd) Start(t *testing.T) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		lis, err := net.Listen("tcp", p.Config.Server.Listen)
		assert.NoError(t, err)
		p.server.Serve(lis)
	}()
}

func (p *MockPushd) Stop(t *testing.T) {
	err := p.server.Stop()
	assert.NoError(t, err)
	p.wg.Wait()
}

func initLogger() {
	log.SetGlobalLogger(zap.NewNop())
	logrus.SetOutput(ioutil.Discard)
}

func (p *MockPushd) ServerAddr() string {
	return p.Config.Server.Listen
}

func (p *MockPushd) CallbackService() string {
	return p.Config.Server.Callback.Service
}
