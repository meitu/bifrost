package lb

import (
	"context"
	"encoding/json"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"google.golang.org/grpc/resolver"
)

type Watcher struct {
	key    string
	cli    *clientv3.Client
	ctx    context.Context
	cancel context.CancelFunc
}

func NewWatcher(key string, cc clientv3.Config) (*Watcher, error) {
	cli, err := clientv3.New(cc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Watcher{
		key:    key,
		cli:    cli,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func (w *Watcher) Watch(addrs []resolver.Address, cc resolver.ClientConn) {
	rch := w.cli.Watch(w.ctx, w.key, clientv3.WithPrefix())
	for wresp := range rch {
		if wresp.Err() != nil {
			return
		}
		for _, ev := range wresp.Events {
			switch ev.Type {
			case mvccpb.PUT:
				node := &NodeData{}
				err := json.Unmarshal([]byte(ev.Kv.Value), node)
				// TODO log
				if err != nil {
					continue
				}
				w.update(node, addrs, cc)
			case mvccpb.DELETE:
				node := &NodeData{}
				err := json.Unmarshal([]byte(ev.Kv.Value), node)
				if err != nil {
					// TODO log
					continue
				}
				w.remove(node, addrs, cc)
			}
		}
	}
}

func (w *Watcher) update(node *NodeData, nodes []resolver.Address, cc resolver.ClientConn) {
	for _, n := range nodes {
		if n.Addr == node.Addr {
			return
		}
	}
	nodes = append(nodes, resolver.Address{Addr: node.Addr})
	cc.UpdateState(resolver.State{Addresses: nodes})
}

func (w *Watcher) remove(node *NodeData, nodes []resolver.Address, cc resolver.ClientConn) {
	for i, n := range nodes {
		if n.Addr == node.Addr {
			nodes[i] = nodes[len(nodes)-1]
			nodes = nodes[:len(nodes)-1]
			cc.UpdateState(resolver.State{Addresses: nodes})
			return
		}
	}
}

func (w *Watcher) GetAllNodes() (ret []resolver.Address) {
	resp, err := w.cli.Get(w.ctx, w.key, clientv3.WithPrefix())
	if err != nil {
		// TODO log
		return []resolver.Address{}
	}
	nodes := parseNodes(resp)
	for _, node := range nodes {
		ret = append(ret, resolver.Address{
			Addr: node.Addr,
		})
	}
	return ret
}

func (w *Watcher) Close() {
	w.cancel()
	w.cli.Close()
}

func parseNodes(resp *clientv3.GetResponse) (nodes []NodeData) {
	if resp == nil || resp.Kvs == nil {
		return []NodeData{}
	}

	for i := range resp.Kvs {
		if v := resp.Kvs[i].Value; v != nil {
			node := NodeData{}
			err := json.Unmarshal(v, &node)
			if err != nil {
				// TODO log
				continue
			}
			node.Key = string(resp.Kvs[i].Key)
			nodes = append(nodes, node)
		}
	}
	return nodes
}
