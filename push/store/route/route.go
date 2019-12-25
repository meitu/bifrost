package route

import (
	"errors"

	util "github.com/meitu/bifrost/push/store/redis_util"
	"github.com/pingcap/tidb/kv"
)

var (
	ErrInvalidIterator  = errors.New("invalid iterator")
	ErrUnexpectedDelete = errors.New("unexpected delete")
)

type RouteOps interface {
	Add(ns, topic, address string, version uint64) error
	Remove(ns, topic, address string, version uint64) error
	Exist(ns, topic string) (bool, error)
	Lookup(ns, topic string) ([]string, error)
}

type Option func(r *routeOps)

type routeOps struct {
	txn kv.Transaction
	rdc util.Redic
}

func SetTikvStore(txn kv.Transaction) Option {
	return func(r *routeOps) {
		r.txn = txn
	}
}

func SetRedisStore(rdc util.Redic) Option {
	return func(r *routeOps) {
		r.rdc = rdc
	}
}

func NewRouteOps(opts ...Option) RouteOps {
	s := &routeOps{}
	for _, op := range opts {
		op(s)
	}
	if s.txn != nil {
		return NewTiRoute(s.txn)
	}
	return NewRdsRoute(s.rdc)
}
