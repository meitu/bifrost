package route

import (
	"context"
	"testing"

	"github.com/meitu/bifrost/push/store/mock"
)

func MockTiRoute() RouteOps {
	txn, _ := mock.Store.Begin()
	return NewTiRoute(txn)
}

//Add
func TestTiRouter(t *testing.T) {
	rt := NewRouteT(t, MockTiRoute, func(p RouteOps) {
		ti := p.(*tiRoute)
		ti.txn.Commit(context.TODO())
	})
	rt.OperatorRoute("ns", "topic")
}
