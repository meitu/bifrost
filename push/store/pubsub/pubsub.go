package pubsub

import (
	pbMessage "github.com/meitu/bifrost/push/message"
	util "github.com/meitu/bifrost/push/store/redis_util"
	"github.com/pingcap/tidb/kv"
)

type PubsubOps interface {
	Publish(ns, topic string, message *pbMessage.Message) ([]byte, error)
	Last(ns, topic string) ([]byte, error)
	Range(ns, topic string, pos []byte, count int64) ([]byte, []*pbMessage.Message, bool, error)

	// LastMID(ns, topic string) ([]byte, error)

	Drop(ns, topic string) error
	SetRetain(ns, topic string, message *pbMessage.Message) error
	DeleteRetain(ns, topic string) error
	Retain(ns, topic string) (*pbMessage.Message, error)

	Subscribe(ns, topic, client string, online bool) error
	Unsubscribe(ns, topic, client string) error
	HasSubscriber(ns, topic string) (bool, error)
	ListOfflineSubscribers(ns, topic string) ([]string, error)
	ListOnlineSubscribers(ns, topic string) ([]string, error)
}

type Option func(r *pubsubOps)

type pubsubOps struct {
	txn   kv.Transaction
	rdc   util.Redic
	queue util.Queue
}

func SetTikvStore(txn kv.Transaction) Option {
	return func(r *pubsubOps) {
		r.txn = txn
	}
}

func SetRedisStore(rdc util.Redic, queue util.Queue) Option {
	return func(r *pubsubOps) {
		r.rdc = rdc
		r.queue = queue
	}
}

func NewPubsubOps(opts ...Option) PubsubOps {
	s := &pubsubOps{}
	for _, op := range opts {
		op(s)
	}
	if s.txn != nil {
		return NewTiPubsub(s.txn)
	}
	return NewRdsPubsub(s.rdc, s.queue)
}
