package conncli

import (
	"errors"
	"time"

	"github.com/meitu/bifrost/commons/log"
	conn "github.com/meitu/bifrost/grpc/conn"
	"github.com/meitu/bifrost/push/conf"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	ErrChannelFull = errors.New("channel full")
)

type client struct {
	addr       string
	cli        conn.ConnServiceClient
	connection *grpc.ClientConn
	notifyChan chan *conn.NotifyReq
	ctx        context.Context
	cancel     context.CancelFunc
	cfg        *conf.Client
}

func newClient(addr string, cfg *conf.Client) (*client, error) {
	// create grpc connection
	connection, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	cli := conn.NewConnServiceClient(connection)
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		addr:       addr,
		connection: connection,
		cli:        cli,
		notifyChan: make(chan *conn.NotifyReq, cfg.NotifyChanSize),
		ctx:        ctx,
		cancel:     cancel,
		cfg:        cfg,
	}
	for i := 0; i < cfg.WorkerProcesses; i++ {
		go c.work()
	}
	return c, nil
}

func (cli *client) work() {
	for {
		select {
		case <-cli.ctx.Done():
			return
		case req := <-cli.notifyChan:
			cli.dealReq(req)
		}
	}
}

func (cli *client) notify(ctx context.Context, req *conn.NotifyReq) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case cli.notifyChan <- req:
		return nil
	default:
		return ErrChannelFull
	}
}

func (cli *client) dealReq(req *conn.NotifyReq) error {
	if env := log.Check(zap.InfoLevel, "notify client"); env != nil {
		env.Write(zap.String("traceid", req.TraceID),
			zap.String("addr", cli.addr),
			zap.String("service", req.Service),
			zap.String("topic", req.Topic),
			zap.Reflect("index", req.Index))
	}

	for i := 0; i < cli.cfg.RetryTimes; i++ {
		tCtx, cancel := context.WithTimeout(cli.ctx, cli.cfg.DefaultTimeout)
		start := time.Now()
		_, err := cli.cli.Notify(tCtx, req)
		cost := time.Since(start)
		cancel()

		if err != nil {
			log.Error("notify connd failed",
				zap.String("traceid", req.TraceID),
				zap.String("addr", cli.addr),
				zap.String("service", req.Service),
				zap.String("topic", req.Topic),
				zap.Reflect("index", req.Index),
				zap.Duration("cost", cost),
				zap.Error(err))
			continue
		}

		if cost > cli.cfg.Slowlog {
			log.Warn("notify slow",
				zap.String("traceid", req.TraceID),
				zap.String("addr", cli.addr),
				zap.String("service", req.Service),
				zap.String("topic", req.Topic),
				zap.Reflect("index", req.Index),
				zap.Duration("cost", cost),
				zap.Duration("threshold", cli.cfg.Slowlog))

		}
		return nil
	}
	return nil
}

func (cli *client) disconnect(ctx context.Context, req *conn.DisconnectReq) error {
	start := time.Now()
	_, err := cli.cli.Disconnect(ctx, req)
	cost := time.Since(start)
	if err != nil {
		log.Error("disconnect failed",
			zap.String("clientid", req.ClientID),
			zap.String("traceid", req.TraceID),
			zap.Int64("connid", req.ConnectionID),
			zap.String("addr", cli.addr),
			zap.Duration("cost", cost),
			zap.Error(err))
	}
	if cost > cli.cfg.Slowlog {
		log.Warn("disconnect slow",
			zap.String("clientid", req.ClientID),
			zap.String("traceid", req.TraceID),
			zap.Int64("connid", req.ConnectionID),
			zap.String("addr", cli.addr),
			zap.Duration("cost", cost),
			zap.Duration("threshold", cli.cfg.Slowlog))
	}
	return nil
}

func (c *client) close() error {
	c.cancel()
	return c.connection.Close()
}
