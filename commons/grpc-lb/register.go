package lb

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
)

const (
	ttl = 4 * time.Second
)

type Node struct {
	cli    *clientv3.Client
	key    string
	value  string
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type NodeData struct {
	Key  string `json:"-"`
	Addr string `json:"addr"`
}

func NewNode(c clientv3.Config, srvName, addr string) (*Node, error) {
	if c.DialTimeout == 0 {
		c.DialTimeout = time.Second
	}
	cli, err := clientv3.New(c)
	if err != nil {
		return nil, err
	}

	key := "/" + scheme + "/" + srvName + "/" + version + "/" + addr

	v, err := json.Marshal(NodeData{Addr: addr, Key: key})
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Node{
		cli:    cli,
		ctx:    ctx,
		cancel: cancel,
		key:    key,
		value:  string(v),
	}, nil
}

func (n *Node) Register() error {
	if err := n.updateNode(); err != nil {
		return err
	}

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		ticker := time.NewTicker(ttl / 2)
		for {
			select {
			case <-ticker.C:
				if err := n.updateNode(); err != nil {
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

func (n *Node) updateNode() error {
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

func (n *Node) Deregister() error {
	n.cancel()
	n.wg.Wait()
	return nil
}
