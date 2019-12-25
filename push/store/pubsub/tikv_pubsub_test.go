package pubsub

import (
	"context"
	"testing"

	"github.com/meitu/bifrost/push/store/mock"
)

func MockTiPubsub() PubsubOps {
	txn, _ := mock.Store.Begin()
	return NewTiPubsub(txn)
}

func TestTiPubsub(t *testing.T) {
	rt := NewPubsubT(t, MockTiPubsub, func(p PubsubOps) {
		ti := p.(*tiPubsub)
		ti.txn.Commit(context.TODO())
	})
	rt.OperatorRetain("ns", "topic")
	rt.OperatorSubscribe("ns", "topic")
	rt.OperatorOnline("ns", "topic")
	rt.OperatorOffline("ns", "topic")
	rt.OperatorPublish("ns", "topic")
}
