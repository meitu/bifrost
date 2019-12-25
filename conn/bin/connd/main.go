package main

import (
	"crypto/tls"
	"errors"
	"expvar"
	"flag"
	"io"
	"net"
	_ "net/http/pprof"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/net/trace"

	"github.com/distributedio/configo"
	"github.com/distributedio/continuous"
	"github.com/meitu/bifrost/commons/httpauth"
	"github.com/meitu/bifrost/commons/log"
	"github.com/meitu/bifrost/conn"
	"github.com/meitu/bifrost/conn/conf"
	"github.com/meitu/bifrost/conn/context"
	"github.com/meitu/bifrost/conn/context/clients"
	"github.com/meitu/bifrost/conn/context/pubsub"
	"github.com/meitu/bifrost/conn/context/pushcli"
	"github.com/meitu/bifrost/conn/poll"
	"github.com/meitu/bifrost/conn/status"
	"github.com/meitu/bifrost/conn/websocket"
	"github.com/meitu/bifrost/version"
)

var (
	configFile = flag.String("c", "./conf/connd.toml", "specify the config file")
	v          = flag.Bool("v", false, "print version and exit")
)

func main() {
	flag.Parse()
	if *v {
		log.Info("connd of version", zap.Stringer("version", version.GetVersion()))
		return
	}
	// load config file.
	config := &conf.Connd{}
	if err := configo.Load(*configFile, config); err != nil {
		log.Fatal("load config file failed", zap.Error(err))
	}
	if err := config.Validate(); err != nil {
		log.Fatal("config is validate", zap.Error(err))
	}
	printInfo(config)

	writer, err := Logger(&config.Logger)
	if err != nil {
		log.Fatal("create logger failed", zap.Error(err))
	}

	cli, err := pushcli.NewClient(&config.Push)
	if err != nil {
		log.Fatal("create pushcli failed", zap.Error(err))
	}
	defer cli.Close()

	ctx := context.Background()

	pubs := pubsub.NewPubsub(&config.Pubsub, ctx, cli)

	baseCtx := &context.BaseContext{
		Context: ctx,
		Clients: clients.NewClients(),
		PushCli: cli,
		Pubsub:  pubs,
	}

	cont := continuous.New(continuous.LoggerOutput(writer), continuous.PidFile(config.PIDFileName))

	grpcsrv := conn.NewGrpcServer(&context.GrpcSrvContext{BaseContext: baseCtx})

	status, err := status.NewServer(&config.Monitor)
	if err != nil {
		log.Fatal("monitor server start failed", zap.Error(err))
	}

	//customer the auth about the net trace
	trace.AuthRequest = httpauth.AuthRequest

	// register grcp service
	cont.AddServer(grpcsrv, &continuous.ListenOn{
		Network: "tcp",
		Address: config.Grpc.Listen,
	})

	// start mqttsrv listen
	for _, c := range config.Listen {
		poll, err := poll.OpenPoll()
		if err != nil {
			log.Fatal("create poll failed", zap.Error(err))
		}
		mqttCtx := &context.MqttSrvContext{
			BaseContext: baseCtx,

			GrpcAddr:    config.Grpc.Listen,
			Conf:        &config.MqttServer.Session,
			Poll:        poll,
			Connections: make(map[int]interface{}),
			Clock:       &sync.RWMutex{},
		}

		mqttsrv := conn.NewMQTTServer(mqttCtx)
		if err := MqttListen(c, mqttsrv, cont); err != nil {
			log.Fatal("mqtt listen failed", zap.Error(err))
		}
	}

	cont.AddServer(status, &continuous.ListenOn{
		Network: "tcp",
		Address: config.Monitor.Listen,
	})

	//TODO 动态上报注册信息
	expvar.Publish("conf", config)
	expvar.Publish("version", version.GetVersion())

	if err := ready(cli); err != nil {
		log.Fatal("mqttsrv start failed", zap.Error(err))
	}

	if err := cont.Serve(); err != nil {
		log.Fatal("serve start failed", zap.Error(err))
	}
	log.Info("stoping connd")
}

func MqttListen(c *conf.ListenerConfig, mqttsrv *conn.MQTTServer, cont *continuous.Cont) error {
	wsUpgrader := func(lis net.Listener) net.Listener {
		// Use /mqtt as the serve path, maybe it should be configurable later
		return websocket.ListenWebsocket(lis, "/mqtt", log.GlobalLogger())
	}
	switch c.Type {
	case "tcp":
		cont.AddServer(mqttsrv, &continuous.ListenOn{
			Network: "tcp",
			Address: c.Listen,
		})
	case "tls":
		tlsCfg, err := loadTLSConfig(c.Pem, c.Key, c.TicketKey)
		if err != nil {
			log.Error("failed to load tls config", zap.Error(err))
			return err
		}
		cont.AddServer(mqttsrv, &continuous.ListenOn{
			Network: "tcp",
			Address: c.Listen,
		}, continuous.TLSConfig(tlsCfg))
	case "ws":
		cont.AddServer(mqttsrv, &continuous.ListenOn{
			Network: "tcp",
			Address: c.Listen,
		}, continuous.ListenerUpgrader(wsUpgrader))
	case "wss":
		tlsCfg, err := loadTLSConfig(c.Pem, c.Key, c.TicketKey)
		if err != nil {
			log.Error("failed to load tls config", zap.Error(err))
			return err
		}
		cont.AddServer(mqttsrv, &continuous.ListenOn{
			Network: "tcp",
			Address: c.Listen,
		}, continuous.TLSConfig(tlsCfg), continuous.ListenerUpgrader(wsUpgrader))
	default:
		return errors.New("this type does not exist in config")
	}
	return nil
}

func printInfo(config *conf.Connd) {
	log.Info("version", zap.Stringer("version", version.GetVersion()))
	log.Info("config message", zap.Stringer("config", config))
}

func Logger(config *conf.Logger) (io.Writer, error) {
	writer, err := log.Writer(config.DebugLog.Path,
		config.DebugLog.TimeRotate,
		config.DebugLog.Compress)
	if err != nil {
		log.Error("create debug writer failed", zap.Error(err))
		return nil, err
	}

	debug, err := log.Logger(writer,
		config.DebugLog.Level,
		config.DebugLog.Name,
		config.DebugLog.Format,
		"/connd/log/debug",
	)
	if err != nil {
		log.Error("create debug logger failed", zap.Error(err))
		return nil, err
	}

	log.SetGlobalLogger(debug)
	return writer, nil
}

func loadTLSConfig(certFile string, keyFile string, ticket string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	var buf [32]byte
	copy(buf[:], ticket)

	tlsCfg := &tls.Config{}
	tlsCfg.Certificates = append(tlsCfg.Certificates, cert)
	tlsCfg.SetSessionTicketKeys([][32]byte{buf})
	return tlsCfg, nil
}

func ready(cli *pushcli.PushCli) error {
	var count int
	for {
		if cli.Ready() {
			log.Info("mqttsrv start work")
			return nil
		}
		time.Sleep(time.Second)
		count++
		if count == 60 {
			return errors.New("there is no the endpoint of push service")
		}
	}
}
