package callback

import (
	"encoding/json"
	"sync/atomic"
	"unsafe"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/meitu/bifrost/commons/efunc"
	lb "github.com/meitu/bifrost/commons/grpc-lb"
	"github.com/meitu/bifrost/commons/log"
	pb "github.com/meitu/bifrost/grpc/callback"
	"github.com/meitu/bifrost/push/conf"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
)

type disConn struct {
	cli *grpc.ClientConn
	r   *lb.Resolver
}

type Record struct {
	Client map[string]*disConn

	OnConnect     map[string]pb.OnConnectClient
	OnPublish     map[string]pb.OnPublishClient
	OnOffline     map[string]pb.OnOfflineClient
	OnSubscribe   map[string]pb.OnSubscribeClient
	OnDisconnect  map[string]pb.OnDisconnectClient
	PostSubscribe map[string]pb.PostSubscribeClient
	OnUnsubscribe map[string]pb.OnUnsubscribeClient
	OnACK         map[string]pb.OnACKClient
}

func (cb *callback) watch(config *conf.Callback) {
	rch := cb.cli.Watch(cb.ctx)
	for wresp := range rch {
		if wresp.Err() != nil {
			return
		}

		for _, ev := range wresp.Events {
			cur := &efunc.ClientData{}
			if err := json.Unmarshal(ev.Kv.Value, cur); err != nil {
				continue
			}

			pre := &efunc.ClientData{}
			if ev.PrevKv != nil {
				if err := json.Unmarshal(ev.PrevKv.Value, pre); err != nil {
					continue
				}
			}

			switch ev.Type {
			case mvccpb.PUT:
				cb.processAdd(config, pre, cur)
			case mvccpb.DELETE:
				cb.processReset(config, cur)
			}
		}
	}
}

func (cb *callback) processAdd(config *conf.Callback, prev *efunc.ClientData, cur *efunc.ClientData) {
	if prev.ServiceName != cur.ServiceName && prev.ServiceName != "" {
		// del
		rec := copyRecord(cb.record)
		for _, name := range prev.Funcs {
			removeCallback(rec, name, prev.ServiceName)
		}
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&cb.record)), unsafe.Pointer(rec))
	}
	if cur.ServiceName == "" {
		return
	}
	// add
	if err := cb.parseNode(config, cur); err != nil {
		log.Error("process add signal failed", zap.Error(err))
	}
}

func (cb *callback) processReset(config *conf.Callback, cur *efunc.ClientData) {
	if cur.ServiceName == "" {
		return
	}
	// del
	rec := copyRecord(cb.record)
	for _, name := range cur.Funcs {
		removeCallback(rec, name, cur.ServiceName)
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&cb.record)), unsafe.Pointer(rec))
}

func (cb *callback) parseNode(config *conf.Callback, node *efunc.ClientData) error {
	rec := copyRecord(cb.record)

	if _, ok := rec.Client[node.ServiceName]; !ok {
		cc := clientv3.Config{
			Endpoints: config.Etcd.Cluster,
			Username:  config.Etcd.Username,
			Password:  config.Etcd.Password,
		}
		r := lb.NewResolver(cc, node.ServiceName)
		resolver.Register(r)

		var opts []grpc.DialOption
		opts = append(opts, grpc.WithBalancerName("round_robin"))
		opts = append(opts, grpc.WithInsecure())
		opts = append(opts, grpc.WithBlock())
		conn, err := grpc.Dial(r.URL(), opts...)
		if err != nil {
			log.Error("grpc dail failed", zap.Error(err))
			return err
		}
		rec.Client[node.ServiceName] = &disConn{r: r, cli: conn}
	}

	for i := range node.Funcs {
		addCallback(rec.Client[node.ServiceName], rec, node.Funcs[i], node.ServiceName)
	}

	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&cb.record)), unsafe.Pointer(rec))
	return nil
}

func (cb *callback) GetClientAddr() map[string]*disConn {
	recv := (*Record)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&cb.record))))
	return recv.Client
}

func removeCallback(record *Record, funcName, service string) {
	switch funcName {
	case "OnConnect":
		delete(record.OnConnect, service)
	case "OnDisconnect":
		delete(record.OnDisconnect, service)
	case "OnSubscribe":
		delete(record.OnSubscribe, service)
	case "PostSubscribe":
		delete(record.PostSubscribe, service)
	case "OnUnsubscribe":
		delete(record.OnUnsubscribe, service)
	case "OnPublish":
		delete(record.OnPublish, service)
	case "OnACK":
		delete(record.OnACK, service)
	case "OnOffline":
		delete(record.OnOffline, service)
	default:
		log.Error("unknown callback name", zap.String("funcName", funcName))
	}
}

func addCallback(cli *disConn, record *Record, funcName, service string) {
	switch funcName {
	case "OnConnect":
		record.OnConnect[service] = pb.NewOnConnectClient(cli.cli)
	case "OnDisconnect":
		record.OnDisconnect[service] = pb.NewOnDisconnectClient(cli.cli)
	case "OnSubscribe":
		record.OnSubscribe[service] = pb.NewOnSubscribeClient(cli.cli)
	case "PostSubscribe":
		record.PostSubscribe[service] = pb.NewPostSubscribeClient(cli.cli)
	case "OnPublish":
		record.OnPublish[service] = pb.NewOnPublishClient(cli.cli)
	case "OnUnsubscribe":
		record.OnUnsubscribe[service] = pb.NewOnUnsubscribeClient(cli.cli)
	case "OnACK":
		record.OnACK[service] = pb.NewOnACKClient(cli.cli)
	case "OnOffline":
		record.OnOffline[service] = pb.NewOnOfflineClient(cli.cli)
	default:
		log.Error("unknown callback name", zap.String("funcName", funcName))
		return
	}
}

func newRecord() *Record {
	return &Record{
		Client:        make(map[string]*disConn),
		OnConnect:     make(map[string]pb.OnConnectClient),
		OnPublish:     make(map[string]pb.OnPublishClient),
		OnOffline:     make(map[string]pb.OnOfflineClient),
		OnSubscribe:   make(map[string]pb.OnSubscribeClient),
		OnDisconnect:  make(map[string]pb.OnDisconnectClient),
		PostSubscribe: make(map[string]pb.PostSubscribeClient),
		OnUnsubscribe: make(map[string]pb.OnUnsubscribeClient),
		OnACK:         make(map[string]pb.OnACKClient),
	}
}

func copyRecord(r *Record) *Record {
	rec := newRecord()
	for k, v := range r.Client {
		rec.Client[k] = v
	}
	for k, v := range r.OnConnect {
		rec.OnConnect[k] = v
	}
	for k, v := range r.OnPublish {
		rec.OnPublish[k] = v
	}
	for k, v := range r.OnOffline {
		rec.OnOffline[k] = v
	}
	for k, v := range r.OnSubscribe {
		rec.OnSubscribe[k] = v
	}
	for k, v := range r.OnDisconnect {
		rec.OnDisconnect[k] = v
	}
	for k, v := range r.PostSubscribe {
		rec.PostSubscribe[k] = v
	}
	for k, v := range r.OnUnsubscribe {
		rec.OnUnsubscribe[k] = v
	}
	for k, v := range r.OnACK {
		rec.OnACK[k] = v
	}
	return rec
}
