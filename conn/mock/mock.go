package mock

import (
	"fmt"
	"net"
	"os"
	"sync"
	"testing"

	"github.com/distributedio/configo"
	"github.com/meitu/bifrost/commons/log"
	"github.com/meitu/bifrost/conn"
	"github.com/meitu/bifrost/conn/conf"
	"github.com/meitu/bifrost/conn/context"
	"github.com/meitu/bifrost/conn/context/clients"
	"github.com/meitu/bifrost/conn/context/pubsub"
	"github.com/meitu/bifrost/conn/context/pushcli"
	"github.com/meitu/bifrost/conn/poll"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

var path string

type MockConnd struct {
	Config *conf.Connd
	wg     sync.WaitGroup
	mqtts  *conn.MQTTServer
	grpcs  *conn.GrpcServer
}

type Opt func(conf *conf.Connd)

func SetAuth(auth string) Opt {
	return func(c *conf.Connd) {
		c.MqttServer.Session.Auth = auth
	}
}

func SetServerAddr(addr string) Opt {
	return func(c *conf.Connd) {
		// c.Tcp = addr
	}
}

func NewMockConnd(t *testing.T, etcdAddrs []string, opts ...Opt) *MockConnd {
	initLogger()

	if path == "" {
		path = os.Getenv("GOPATH")
		path = path + "/src/github.com/meitu/bifrost/conn/conf/connd.toml"
	}

	var config conf.Connd
	if err := configo.Load(path, &config); err != nil {
		fmt.Printf("load Config file failed , %s\n", err)
		os.Exit(1)
	}

	for _, o := range opts {
		o(&config)
	}
	if config.Tcp.Listen == "" && config.Tls.Listen == "" && config.Wss.Listen == "" && config.Ws.Listen == "" {
		config.Tcp.Type = "tcp"
		config.Tcp.Listen = "0.0.0.0:1883"

	}
	config.Etcd = conf.Etcd{Cluster: etcdAddrs}
	config.Validate()

	cli, err := pushcli.NewClient(&config.Push)
	assert.NoError(t, err)

	ctx := context.Background()
	pubs := pubsub.NewPubsub(&config.Pubsub, ctx, cli)

	bctx := &context.BaseContext{
		Context: ctx,
		PushCli: cli,
		Clients: clients.NewClients(),
		Pubsub:  pubs,
	}

	poll, err := poll.OpenPoll()
	if err != nil {
		panic(err)
	}

	mctx := &context.MqttSrvContext{
		BaseContext: bctx,
		GrpcAddr:    config.Grpc.Listen,
		Conf:        &config.MqttServer.Session,
		Poll:        poll,
		Connections: make(map[int]interface{}),
		Clock:       &sync.RWMutex{},
	}

	// radar is not inited
	mqttsrv := conn.NewMQTTServer(mctx)
	grpcsrv := conn.NewGrpcServer(&context.GrpcSrvContext{BaseContext: bctx})

	return &MockConnd{
		mqtts:  mqttsrv,
		grpcs:  grpcsrv,
		Config: &config,
	}
}

func (c *MockConnd) Start(t *testing.T) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		lis, err := net.Listen("tcp", c.Config.Grpc.Listen)
		assert.NoError(t, err)
		c.grpcs.Serve(lis)
	}()

	for _, lis := range c.Config.Listen {
		c.wg.Add(1)
		go func(addr string) {
			defer c.wg.Done()
			//TODO 支持tls和ws /wss 方式启动
			lis, err := net.Listen("tcp", addr)
			assert.NoError(t, err)
			c.mqtts.Serve(lis)
		}(lis.Listen)
	}
}

func (c *MockConnd) Stop(t *testing.T) {
	err := c.grpcs.Stop()
	assert.NoError(t, err)
	err = c.mqtts.Stop()
	assert.NoError(t, err)
	c.wg.Wait()
}

func (c *MockConnd) MQTTAddr() []string {
	var addrs []string
	for _, lis := range c.Config.Listen {
		switch lis.Type {
		case "tcp":
			addrs = append(addrs, fmt.Sprintf("tcp://%s", lis.Listen))
		case "tls":
			addrs = append(addrs, fmt.Sprintf("tls://%s", lis.Listen))
		case "ws":
			addrs = append(addrs, fmt.Sprintf("ws://%s/mqtt", lis.Listen))
		case "wss":
			addrs = append(addrs, fmt.Sprintf("wss://%s/mqtt", lis.Listen))
		}
	}
	return addrs
}

func initLogger() {
	log.SetGlobalLogger(zap.NewNop())
}
