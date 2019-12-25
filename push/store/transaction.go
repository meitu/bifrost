package store

import (
	"github.com/meitu/bifrost/push/store/pubsub"
	rds_util "github.com/meitu/bifrost/push/store/redis_util"
	"github.com/meitu/bifrost/push/store/route"
	"github.com/meitu/bifrost/push/store/session"
	"github.com/pingcap/tidb/kv"
	"golang.org/x/net/context"
)

type tiTxn struct {
	txn kv.Transaction
	t   pubsub.PubsubOps
	s   session.SessionOps
	r   route.RouteOps
}

func newTiTxn(txn kv.Transaction) Transaction {
	return &tiTxn{
		txn: txn,
		t:   pubsub.NewPubsubOps(pubsub.SetTikvStore(txn)),
		s:   session.NewSessionOps(session.SetTikvStore(txn)),
		r:   route.NewRouteOps(route.SetTikvStore(txn)),
	}
}

func (t *tiTxn) TopicOps() pubsub.PubsubOps {
	return t.t
}

func (t *tiTxn) SessionOps() session.SessionOps {
	return t.s
}

func (t *tiTxn) RouteOps() route.RouteOps {
	return t.r
}

func (t *tiTxn) Rollback() error {
	return t.txn.Rollback()
}

func (t *tiTxn) Commit(ctx context.Context) error {
	return t.txn.Commit(ctx)
}

type rdsTxn struct {
	t pubsub.PubsubOps
	s session.SessionOps
	r route.RouteOps
}

func newRdsTxn(rdc rds_util.Redic, queue rds_util.Queue) Transaction {
	return &rdsTxn{
		t: pubsub.NewPubsubOps(pubsub.SetRedisStore(rdc, queue)),
		s: session.NewSessionOps(session.SetRedisStore(rdc)),
		r: route.NewRouteOps(route.SetRedisStore(rdc)),
	}
}

func (t *rdsTxn) TopicOps() pubsub.PubsubOps {
	return t.t
}

func (t *rdsTxn) SessionOps() session.SessionOps {
	return t.s
}

func (t *rdsTxn) RouteOps() route.RouteOps {
	return t.r
}

func (t *rdsTxn) Rollback() error {
	return nil
}

func (t *rdsTxn) Commit(ctx context.Context) error {
	return nil
}
