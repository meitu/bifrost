package main

import (
	"expvar"
	"flag"
	"io"

	_ "net/http/pprof"

	"github.com/distributedio/configo"
	"github.com/distributedio/continuous"
	"github.com/facebookgo/grace/gracenet"
	"github.com/meitu/bifrost/commons/httpauth"
	"github.com/meitu/bifrost/commons/log"
	"github.com/meitu/bifrost/push"
	"github.com/meitu/bifrost/push/conf"
	"github.com/meitu/bifrost/push/status"
	"github.com/meitu/bifrost/version"
	"go.uber.org/zap"
	"golang.org/x/net/trace"
)

var (
	gnet = &gracenet.Net{}
)

func main() {
	//customer the auth about the net trace
	trace.AuthRequest = httpauth.AuthRequest
	var v bool
	var path string
	flag.BoolVar(&v, "v", false, "print version and exit")
	flag.StringVar(&path, "c", "conf/pushd.toml", "specify the config file")
	flag.Parse()

	if v {
		log.Info("pushd version", zap.Stringer("version", version.GetVersion()))
		return
	}
	expvar.Publish("version", version.GetVersion())

	config := &conf.Pushd{}
	if err := configo.Load(path, config); err != nil {
		log.Fatal("unmarshal config file failed", zap.Error(err))
	}
	if err := config.ValidateConf(); err != nil {
		log.Fatal("config is invalid", zap.Error(err))
	}

	expvar.Publish("conf", config)

	log.Info("configuration", zap.Stringer("config", config))

	//share etcd with other moduls.
	writer, err := Logger(&config.Logger)
	if err != nil {
		log.Fatal("create logger failed", zap.Error(err))
	}
	cont := continuous.New(continuous.LoggerOutput(writer), continuous.PidFile(config.PIDFileName))

	server, err := push.NewServer(&config.Server)
	if err != nil {
		log.Fatal("create pushd server failed", zap.Error(err))
	}

	monitor, err := status.NewServer(&config.Status)
	if err != nil {
		log.Fatal("create status server failed", zap.Error(err))
	}

	//注册热启动
	cont.AddServer(server, &continuous.ListenOn{Network: "tcp", Address: config.Server.Listen})
	cont.AddServer(monitor, &continuous.ListenOn{Network: "tcp", Address: config.Status.Listen})

	if err := cont.Serve(); err != nil {
		log.Fatal("serve failed", zap.Error(err))
	}
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
		"/pushd/log/debug",
	)
	if err != nil {
		log.Error("create debug logger failed", zap.Error(err))
		return nil, err
	}

	log.SetGlobalLogger(debug)
	return writer, nil
}
