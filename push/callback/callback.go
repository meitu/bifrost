package callback

import (
	"context"
	"encoding/json"

	"github.com/coreos/etcd/clientv3"
	"github.com/meitu/bifrost/commons/efunc"
	"github.com/meitu/bifrost/push/conf"
)

type callback struct {
	record *Record
	ctx    context.Context
	cancel context.CancelFunc
	cli    *efunc.Watcher
}

func NewCallback(config *conf.Callback, ctx context.Context) (Callback, error) {
	ecfg := clientv3.Config{
		Endpoints: config.Etcd.Cluster,
		Username:  config.Etcd.Username,
		Password:  config.Etcd.Password,
	}
	cli, err := efunc.NewWatcher(ecfg, config.Service)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	cb := &callback{
		record: newRecord(),
		ctx:    ctx,
		cancel: cancel,
		cli:    cli,
	}

	go cb.watch(config)
	return cb, nil
}

type callbackNode struct {
	Name  string   `json:"service"`
	Nodes []string `json:"nodes"`
}

func (cb *callback) String() string {
	if cb.record == nil {
		return ""
	}
	mapclis := cb.GetClientAddr()
	clusters := make([]callbackNode, 0, len(mapclis))
	for k, cli := range mapclis {
		var cls callbackNode
		cls.Name = k
		cls.Nodes = cli.r.ServerAddrs()
		clusters = append(clusters, cls)
	}
	if rawcluster, err := json.Marshal(clusters); err == nil {
		return string(rawcluster)
	}
	return ""
}

func (cb *callback) Close() error {
	cb.cli.Close()
	cb.cancel()
	return nil
}
