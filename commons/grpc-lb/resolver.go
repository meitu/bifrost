package lb

import (
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"

	"google.golang.org/grpc/resolver"
)

const (
	scheme    = "bifrost"
	version   = "v1.0"
	authority = "authority"
)

type Resolver struct {
	scheme  string
	url     string
	cc      clientv3.Config
	wg      sync.WaitGroup
	watcher *Watcher
	cli     *clientv3.Client
}

func NewResolver(c clientv3.Config, srvName string) *Resolver {
	if c.DialTimeout == 0 {
		c.DialTimeout = time.Second
	}
	return &Resolver{
		cc:     c,
		scheme: scheme,
		url:    scheme + "://" + authority + "/" + srvName + "/" + version,
	}
}

func (r *Resolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOption) (resolver.Resolver, error) {
	var err error
	r.watcher, err = NewWatcher("/"+target.Scheme+"/"+target.Endpoint+"/", r.cc)
	if err != nil {
		return nil, err
	}
	r.start(cc)
	return r, nil
}

func (r *Resolver) Scheme() string {
	return r.scheme
}

func (r *Resolver) URL() string {
	return r.url
}

// ResolveNow will be called by gRPC to try to resolve the target name
// again. It's just a hint, resolver can ignore this if it's not necessary.
func (r *Resolver) ResolveNow(o resolver.ResolveNowOption) {
	//TODO
}

// Close closes the resolver.
func (r *Resolver) Close() {
	r.watcher.Close()
	r.wg.Wait()
}

func (r *Resolver) ServerAddrs() []string {
	nodes := r.watcher.GetAllNodes()
	var addrs []string
	for _, n := range nodes {
		addrs = append(addrs, n.Addr)
	}
	return addrs
}

func (r *Resolver) start(cc resolver.ClientConn) {
	addrs := r.watcher.GetAllNodes()
	cc.UpdateState(resolver.State{Addresses: addrs})

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.watcher.Watch(addrs, cc)
	}()
}
