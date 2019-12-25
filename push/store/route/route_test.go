package route

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type routeT struct {
	t          *testing.T
	beginFunc  func() RouteOps
	endFunc    func(RouteOps)
	handleFunc func(RouteOps, func(RouteOps))
}

func NewRouteT(t *testing.T, p func() RouteOps, e func(RouteOps)) *routeT {
	return &routeT{
		t:          t,
		beginFunc:  p,
		handleFunc: func(p RouteOps, handler func(RouteOps)) { handler(p) },
		endFunc:    e,
	}
}

func (rt *routeT) OperatorRoute(ns, topic string) {
	handler := func(r RouteOps) {
		exist, err := r.Exist(ns, topic)
		rt.endFunc(r)
		assert.NoError(rt.t, err)
		assert.False(rt.t, exist)
	}
	rt.handleFunc(rt.beginFunc(), handler)

	handler = func(r RouteOps) {
		err := r.Add(ns, topic, "a1", 1)
		rt.endFunc(r)
		assert.NoError(rt.t, err)
	}
	rt.handleFunc(rt.beginFunc(), handler)

	handler = func(r RouteOps) {
		err := r.Add(ns, topic, "a2", 1)
		assert.NoError(rt.t, err)
		rt.endFunc(r)
	}
	rt.handleFunc(rt.beginFunc(), handler)

	handler = func(r RouteOps) {
		exist, err := r.Exist(ns, topic)
		rt.endFunc(r)
		assert.NoError(rt.t, err)
		assert.True(rt.t, exist)
	}
	rt.handleFunc(rt.beginFunc(), handler)

	handler = func(r RouteOps) {
		raw, err := r.Lookup(ns, topic)
		rt.endFunc(r)
		assert.NoError(rt.t, err)
		assert.Contains(rt.t, raw, "a1")
		assert.Contains(rt.t, raw, "a2")
	}
	rt.handleFunc(rt.beginFunc(), handler)

	handler = func(r RouteOps) {
		err := r.Remove(ns, topic, "a1", 0)
		rt.endFunc(r)
		assert.Equal(rt.t, ErrUnexpectedDelete, err)
	}
	rt.handleFunc(rt.beginFunc(), handler)

	handler = func(r RouteOps) {
		err := r.Remove(ns, topic, "a1", 2)
		rt.endFunc(r)
		assert.NoError(rt.t, err)
	}
	rt.handleFunc(rt.beginFunc(), handler)

	handler = func(r RouteOps) {
		err := r.Remove(ns, topic, "a2", 3)
		rt.endFunc(r)
		assert.NoError(rt.t, err)
	}
	rt.handleFunc(rt.beginFunc(), handler)

	handler = func(r RouteOps) {
		exist, err := r.Exist(ns, topic)
		rt.endFunc(r)
		assert.NoError(rt.t, err)
		assert.False(rt.t, exist)
	}
	rt.handleFunc(rt.beginFunc(), handler)
}
