package conncli

import (
	"errors"
	"sync"

	"github.com/meitu/bifrost/commons/log"
	"github.com/meitu/bifrost/commons/trace"
	conn "github.com/meitu/bifrost/grpc/conn"
	"github.com/meitu/bifrost/push/conf"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"

	"golang.org/x/net/context"
)

const (
	TraceID = "traceid"
)

type connd struct {
	clients map[string]*client
	rwlock  sync.RWMutex
	cfg     *conf.Client
}

func NewConnd(config *conf.Connd) (Connd, error) {
	c := &connd{
		clients: make(map[string]*client),
		cfg:     &config.Client,
	}

	return c, nil
}

func (c *connd) Close() error {
	c.rwlock.Lock()
	for addr, cli := range c.clients {
		cli.close()
		delete(c.clients, addr)
	}
	c.rwlock.Unlock()
	return nil
}

func (c *connd) getClient(addr string) (cli *client, err error) {
	c.rwlock.RLock()
	cli, ok := c.clients[addr]
	c.rwlock.RUnlock()

	if !ok {
		if env := log.Check(zap.DebugLevel, "create conn client"); env != nil {
			env.Write(zap.String("addr", addr))
		}
		c.rwlock.Lock()
		defer c.rwlock.Unlock()
		// double check
		cli, ok = c.clients[addr]
		if ok {
			return cli, nil
		}
		cli, err = newClient(addr, c.cfg)
		if err != nil {
			return nil, err
		}
		c.clients[addr] = cli
	}

	return cli, err
}

func (c *connd) Disconnect(ctx context.Context, id, service, addr string, connid int64) error {
	if env := log.Check(zap.DebugLevel, "disconnect client"); env != nil {
		env.Write(zap.String("addr", addr),
			zap.String("id", id),
			zap.Int64("connid", connid))
	}

	req := &conn.DisconnectReq{
		Service:      service,
		ClientID:     id,
		ConnectionID: connid,
		TraceID:      traceid(ctx),
	}

	cli, err := c.getClient(addr)
	if err != nil {
		return err
	}

	err = cli.disconnect(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (c *connd) Notify(ctx context.Context, service string, addrs []string, topic string, index []byte, noneDowngrade bool) error {
	req := &conn.NotifyReq{
		Topic:         topic,
		Index:         index,
		NoneDowngrade: noneDowngrade,
		TraceID:       traceid(ctx),
		Service:       service,
	}
	var errbuf buffer.Buffer
	for _, addr := range addrs {
		cli, err := c.getClient(addr)
		if err != nil {
			errbuf.AppendString(err.Error())
			continue
		}
		if err := cli.notify(ctx, req); err != nil {
			errbuf.AppendString(err.Error())
		}
	}
	if errbuf.Len() != 0 {
		return errors.New(errbuf.String())
	}
	return nil
}

func traceid(ctx context.Context) string {
	traceid, err := trace.GetTraceID(ctx)
	if err != nil {
		return TraceID
	}
	return traceid
}
