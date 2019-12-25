package session

import (
	"testing"

	pbMessage "github.com/meitu/bifrost/push/message"
	"github.com/stretchr/testify/assert"
)

type sessionT struct {
	t          *testing.T
	beginFunc  func() SessionOps
	endFunc    func(SessionOps)
	handleFunc func(SessionOps, func(SessionOps))
}

func NewSessionT(t *testing.T, p func() SessionOps, e func(SessionOps)) *sessionT {
	return &sessionT{
		t:          t,
		beginFunc:  p,
		handleFunc: func(p SessionOps, handler func(SessionOps)) { handler(p) },
		endFunc:    e,
	}
}

func (st *sessionT) OperatorMessageID(ns, cid string) {
	handler := func(s SessionOps) {
		mid, err := s.MessageID(ns, cid)
		st.endFunc(s)
		assert.NoError(st.t, err)
		assert.Equal(st.t, mid, int64(0))
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		err := s.SetMessageID(ns, cid, 100)
		st.endFunc(s)
		assert.NoError(st.t, err)
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		mid, err := s.MessageID(ns, cid)
		st.endFunc(s)
		assert.NoError(st.t, err)
		assert.Equal(st.t, mid, int64(100))
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		err := s.DeleteMessageID(ns, cid)
		st.endFunc(s)
		assert.NoError(st.t, err)
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		mid, err := s.MessageID(ns, cid)
		st.endFunc(s)
		assert.NoError(st.t, err)
		assert.Equal(st.t, mid, int64(0))
	}
	st.handleFunc(st.beginFunc(), handler)
}

func (st *sessionT) OperatorSubscribe(ns, cid string) {
	handler := func(s SessionOps) {
		mapinfo, err := s.Subscriptions(ns, cid)
		st.endFunc(s)
		assert.NoError(st.t, err)
		assert.Equal(st.t, len(mapinfo), 0)
	}
	st.handleFunc(st.beginFunc(), handler)

	expect := make(map[string]byte)
	expect["t1"] = 1
	handler = func(s SessionOps) {
		err := s.Subscribe(ns, cid, "t1", 1)
		st.endFunc(s)
		assert.NoError(st.t, err)
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		mapinfo, err := s.Subscriptions(ns, cid)
		st.endFunc(s)
		assert.NoError(st.t, err)
		assert.Equal(st.t, mapinfo, expect)
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		err := s.Unsubscribe(ns, cid, "t1")
		st.endFunc(s)
		assert.NoError(st.t, err)
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		mapinfo, err := s.Subscriptions(ns, cid)
		st.endFunc(s)
		assert.NoError(st.t, err)
		assert.Equal(st.t, len(mapinfo), 0)
	}
	st.handleFunc(st.beginFunc(), handler)
}

func (st *sessionT) OperatorCursor(ns, cid string) {
	handler := func(s SessionOps) {
		cursor, err := s.Cursor(ns, cid, "t1")
		st.endFunc(s)
		assert.Equal(st.t, err, ErrInvalidRecord)
		assert.Equal(st.t, cursor, []byte(nil))
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		err := s.SetCursor(ns, cid, "t1", []byte("cursor"))
		st.endFunc(s)
		assert.NoError(st.t, err)
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		cursor, err := s.Cursor(ns, cid, "t1")
		st.endFunc(s)
		assert.NoError(st.t, err)
		assert.Equal(st.t, cursor, []byte("cursor"))
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		err := s.DeleteCursor(ns, cid, "t1")
		st.endFunc(s)
		assert.NoError(st.t, err)
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		cursor, err := s.Cursor(ns, cid, "t1")
		st.endFunc(s)
		assert.Equal(st.t, err, ErrInvalidRecord)
		assert.Equal(st.t, cursor, []byte(nil))
	}
	st.handleFunc(st.beginFunc(), handler)
}

func (st *sessionT) OperatorUnack(ns, cid string) {
	msgs := []*pbMessage.Message{
		&pbMessage.Message{BizID: []byte("1"), MessageID: 1, Index: []byte("1000000000000001"), Topic: "t1"},
		&pbMessage.Message{BizID: []byte("2"), MessageID: 2, Index: []byte("1000000000000002"), Topic: "t1"},
		&pbMessage.Message{BizID: []byte("3"), MessageID: 3, Index: []byte("1000000000000003"), Topic: "t1"},
		&pbMessage.Message{BizID: []byte("4"), MessageID: 4, Index: []byte("1000000000000004"), Topic: "t1"}}

	var (
		pos      []byte
		err      error
		complete bool
		expectM  []*pbMessage.Message
	)

	handler := func(s SessionOps) {
		_, expectM, complete, err := s.Unacks(ns, cid, nil, 100)
		st.endFunc(s)
		assert.NoError(st.t, err)
		assert.True(st.t, complete)
		assert.Equal(st.t, msgs[3:], expectM)
	}

	handler = func(s SessionOps) {
		err := s.Unack(ns, cid, msgs)
		st.endFunc(s)
		assert.NoError(st.t, err)
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		pos, expectM, complete, err = s.Unacks(ns, cid, nil, 1)
		st.endFunc(s)
		assert.NoError(st.t, err)
		assert.False(st.t, complete)
		assert.Equal(st.t, msgs[:1], expectM)
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		pos, expectM, complete, err = s.Unacks(ns, cid, pos, 3)
		st.endFunc(s)
		assert.NoError(st.t, err)
		assert.False(st.t, complete)
		assert.Equal(st.t, msgs[1:], expectM)
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		_, expectM, complete, err := s.Unacks(ns, cid, pos, 1)
		st.endFunc(s)
		assert.NoError(st.t, err)
		assert.True(st.t, complete)
		assert.Equal(st.t, msgs[4:], expectM)
	}
	st.handleFunc(st.beginFunc(), handler)

	for _, m := range msgs {
		handler = func(s SessionOps) {
			biz, err := s.Ack(ns, cid, m.MessageID)
			st.endFunc(s)
			assert.NoError(st.t, err)
			assert.Equal(st.t, biz, m.BizID)
		}
		st.handleFunc(st.beginFunc(), handler)
	}

	handler = func(s SessionOps) {
		_, expectM, complete, err := s.Unacks(ns, cid, nil, 1)
		st.endFunc(s)
		assert.NoError(st.t, err)
		assert.True(st.t, complete)
		assert.Equal(st.t, msgs[4:], expectM)
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		err := s.DropUnack(ns, cid)
		st.endFunc(s)
		assert.NoError(st.t, err)
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		_, expectM, complete, err := s.Unacks(ns, cid, nil, 1)
		st.endFunc(s)
		assert.NoError(st.t, err)
		assert.True(st.t, complete)
		assert.Equal(st.t, msgs[4:], expectM)
	}
	st.handleFunc(st.beginFunc(), handler)
}

func (st *sessionT) OperatorConndAddress(ns, cid string) {
	var (
		err     error
		info1   *ClientInfo
		info2   *ClientInfo
		info3   = &ClientInfo{ConnectionID: 1}
		nilInfo *ClientInfo
	)

	handler := func(s SessionOps) {
		err := s.DeleteConndAddress(ns, cid, "string", 0)
		st.endFunc(s)
		assert.NoError(st.t, err)
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		info1, err = s.UpdateConndAddress(ns, cid, "string", 1)
		st.endFunc(s)
		assert.NoError(st.t, err)
		assert.Equal(st.t, info1, nilInfo)
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		info2, err = s.UpdateConndAddress(ns, cid, "string", 2)
		st.endFunc(s)
		assert.NoError(st.t, err)
		assert.Equal(st.t, info2.ConnectionID, info3.ConnectionID)
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		err := s.DeleteConndAddress(ns, cid, "string", 1)
		st.endFunc(s)
		assert.NoError(st.t, err)
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		err := s.DeleteConndAddress(ns, cid, "string", 2)
		st.endFunc(s)
		assert.NoError(st.t, err)
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		err := s.DeleteConndAddress(ns, cid, "string", 2)
		st.endFunc(s)
		assert.NoError(st.t, err)
	}
	st.handleFunc(st.beginFunc(), handler)
}

func (st *sessionT) OperatorDrop(ns, cid string) {
	handler := func(s SessionOps) {
		err := s.SetMessageID(ns, cid, 100)
		st.endFunc(s)
		assert.NoError(st.t, err)
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		err := s.Subscribe(ns, cid, "t1", 1)
		st.endFunc(s)
		assert.NoError(st.t, err)
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		err := s.Drop(ns, cid)
		st.endFunc(s)
		assert.NoError(st.t, err)
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		mid, err := s.MessageID(ns, cid)
		st.endFunc(s)
		assert.NoError(st.t, err)
		assert.Equal(st.t, mid, int64(0))
	}
	st.handleFunc(st.beginFunc(), handler)

	handler = func(s SessionOps) {
		mapinfo, err := s.Subscriptions(ns, cid)
		st.endFunc(s)
		assert.NoError(st.t, err)
		assert.Equal(st.t, len(mapinfo), 0)
	}
	st.handleFunc(st.beginFunc(), handler)
}
