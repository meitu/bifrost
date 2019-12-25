package route

import (
	"testing"

	"github.com/meitu/bifrost/push/store/mock"
)

func MockRdsRoute() RouteOps {
	return NewRdsRoute(mock.MockRedis())
}

//Add
func ATestRdsRouter(t *testing.T) {
	rt := NewRouteT(t, MockRdsRoute, func(p RouteOps) {})
	rt.OperatorRoute("ns", "topic")
}
