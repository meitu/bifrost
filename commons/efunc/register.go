package efunc

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/twinj/uuid"
)

const (
	ttl     = 4 * time.Second
	data    = "/data"
	version = "1.0"
)

type Client struct {
	cli    *clientv3.Client
	key    string
	value  string
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type ClientData struct {
	ServiceName string   `json:"service-name"`
	Funcs       []string `json:"callback"`
}

func NewClient(c clientv3.Config, srvName string, node *ClientData) (*Client, error) {
	if c.DialTimeout == 0 {
		c.DialTimeout = time.Second
	}
	cli, err := clientv3.New(c)
	if err != nil {
		return nil, err
	}

	key := data + "/" + srvName + "/" + version + "/" + uuid.NewV4().String()

	v, err := json.Marshal(node)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		cli:    cli,
		ctx:    ctx,
		cancel: cancel,
		key:    key,
		value:  string(v),
	}, nil
}

func (n *Client) Register() error {
	if err := n.updateClient(); err != nil {
		return err
	}

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		ticker := time.NewTicker(ttl / 2)
		for {
			select {
			case <-ticker.C:
				if err := n.updateClient(); err != nil {
					// TODO log
					return
				}
			case <-n.ctx.Done():
				ticker.Stop()
				if _, err := n.cli.Delete(context.Background(), n.key); err != nil {
					// TODO log
					return
				}
				// TODO log
				return
			}
		}
	}()
	return nil
}

func (n *Client) updateClient() error {
	_, err := n.cli.Get(n.ctx, n.key)
	if err != nil && err != rpctypes.ErrKeyNotFound {
		return err
	}

	resp, err := n.cli.Grant(n.ctx, int64(ttl))
	if err != nil {
		return err
	}

	if _, err := n.cli.Put(n.ctx, n.key, n.value, clientv3.WithLease(resp.ID)); err != nil {
		return err
	}
	return nil
}

func (n *Client) Deregister() error {
	n.cancel()
	n.wg.Wait()
	return nil
}
