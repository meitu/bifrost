package session

import (
	"context"
	"testing"

	"github.com/meitu/bifrost/push/store/mock"
)

func MockTiSession() SessionOps {
	txn, _ := mock.Store.Begin()
	return NewTiSession(txn)
}

func TestTiPubsub(t *testing.T) {
	rt := NewSessionT(t, MockTiSession, func(p SessionOps) {
		ti := p.(*tiSession)
		ti.txn.Commit(context.TODO())
	})
	rt.OperatorMessageID("ns", "c1")
	rt.OperatorSubscribe("ns", "c1")
	rt.OperatorCursor("ns", "c1")
	rt.OperatorConndAddress("ns", "c1")
	rt.OperatorUnack("ns", "c1")
	rt.OperatorDrop("ns", "c1")
}
