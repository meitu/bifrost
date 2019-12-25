package efunc

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
)

type Watcher struct {
	key string
	cli *clientv3.Client
}

func NewWatcher(c clientv3.Config, srvName string) (*Watcher, error) {
	if c.DialTimeout == 0 {
		c.DialTimeout = time.Second
	}
	cli, err := clientv3.New(c)
	if err != nil {
		return nil, err
	}
	return &Watcher{
		key: data + "/" + srvName + "/" + version + "/",
		cli: cli,
	}, nil
}

func (w *Watcher) Watch(ctx context.Context) clientv3.WatchChan {
	return w.cli.Watch(ctx, w.key, clientv3.WithPrefix(), clientv3.WithPrevKV())
}

func (w *Watcher) Close() {
	w.cli.Close()
}
