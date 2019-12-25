package pubsub

import (
	"bytes"
	"testing"

	pbMessage "github.com/meitu/bifrost/push/message"
	"github.com/stretchr/testify/assert"
)

type pubsubT struct {
	t          *testing.T
	beginFunc  func() PubsubOps
	endFunc    func(PubsubOps)
	handleFunc func(PubsubOps, func(PubsubOps))
}

func NewPubsubT(t *testing.T, p func() PubsubOps, e func(PubsubOps)) *pubsubT {
	return &pubsubT{
		t:          t,
		beginFunc:  p,
		handleFunc: func(p PubsubOps, handler func(PubsubOps)) { handler(p) },
		endFunc:    e,
	}
}

// TODO
func (pub *pubsubT) OperatorPublish(ns, topic string) {
	var (
		begin, end []byte
		err        error
		off        []byte
		comlete    bool
		messages   []*pbMessage.Message
	)
	handler := func(p PubsubOps) {
		begin, err = p.Last(ns, topic)
		pub.endFunc(p)
		assert.NoError(pub.t, err)
	}
	pub.handleFunc(pub.beginFunc(), handler)

	// send three messages
	handler = func(p PubsubOps) {
		end, err = p.Publish(ns, topic, &pbMessage.Message{Topic: "t1"})
		pub.endFunc(p)
		assert.NoError(pub.t, err)
		assert.Equal(pub.t, -1, bytes.Compare(begin, end))
	}
	pub.handleFunc(pub.beginFunc(), handler)

	handler = func(p PubsubOps) {
		end, err = p.Publish(ns, topic, &pbMessage.Message{Topic: "t2"})
		pub.endFunc(p)
		assert.NoError(pub.t, err)
		assert.Equal(pub.t, -1, bytes.Compare(begin, end))
	}
	pub.handleFunc(pub.beginFunc(), handler)

	handler = func(p PubsubOps) {
		end, err = p.Publish(ns, topic, &pbMessage.Message{Topic: "t3"})
		pub.endFunc(p)
		assert.NoError(pub.t, err)
		assert.Equal(pub.t, -1, bytes.Compare(begin, end))
	}
	pub.handleFunc(pub.beginFunc(), handler)

	//range
	handler = func(p PubsubOps) {
		off, messages, comlete, err = p.Range(ns, topic, begin, 2)
		pub.endFunc(p)
		assert.Equal(pub.t, messages[0].Topic, "t1")
		assert.Equal(pub.t, messages[1].Topic, "t2")
		assert.False(pub.t, comlete)
		assert.NoError(pub.t, err)
		assert.Equal(pub.t, -1, bytes.Compare(begin, off))
		assert.Equal(pub.t, -1, bytes.Compare(off, end))
	}
	pub.handleFunc(pub.beginFunc(), handler)

	handler = func(p PubsubOps) {
		off, messages, comlete, err = p.Range(ns, topic, off, 2)
		pub.endFunc(p)
		assert.Equal(pub.t, messages[0].Topic, "t3")
		assert.True(pub.t, comlete)
		assert.NoError(pub.t, err)
		assert.Equal(pub.t, -1, bytes.Compare(begin, off))
		assert.NotEqual(pub.t, -1, bytes.Compare(off, end))
	}
	pub.handleFunc(pub.beginFunc(), handler)

	handler = func(p PubsubOps) {
		off, messages, comlete, err = p.Range(ns, topic, off, 1)
		pub.endFunc(p)
		assert.Equal(pub.t, len(messages), 0)
		assert.True(pub.t, comlete)
		assert.NoError(pub.t, err)
		assert.NotEqual(pub.t, -1, bytes.Compare(off, end))
	}
	pub.handleFunc(pub.beginFunc(), handler)

	//last
	handler = func(p PubsubOps) {
		begin, err = p.Last(ns, topic)
		pub.endFunc(p)
		assert.NoError(pub.t, err)
		assert.NotEqual(pub.t, -1, bytes.Compare(begin, end))
	}
	pub.handleFunc(pub.beginFunc(), handler)

	handler = func(p PubsubOps) {
		err = p.Drop(ns, topic)
		pub.endFunc(p)
		assert.NoError(pub.t, err)
	}
	pub.handleFunc(pub.beginFunc(), handler)
}

func (pub *pubsubT) OperatorRetain(ns, topic string) {

	var (
		msg *pbMessage.Message
		m1  *pbMessage.Message
		m3  *pbMessage.Message
		err error
	)
	// set msg
	handler := func(p PubsubOps) {
		msg = &pbMessage.Message{Topic: "t1"}
		err := p.SetRetain(ns, topic, msg)
		pub.endFunc(p)
		assert.NoError(pub.t, err)
	}
	pub.handleFunc(pub.beginFunc(), handler)

	// get msg
	handler = func(p PubsubOps) {
		m1, err = p.Retain(ns, topic)
		pub.endFunc(p)
		assert.NoError(pub.t, err)
		assert.Equal(pub.t, m1, msg)
	}
	pub.handleFunc(pub.beginFunc(), handler)

	// delete msg
	handler = func(p PubsubOps) {
		err = p.DeleteRetain(ns, topic)
		pub.endFunc(p)
		assert.NoError(pub.t, err)
	}
	pub.handleFunc(pub.beginFunc(), handler)

	// is no error
	handler = func(p PubsubOps) {
		err = p.DeleteRetain(ns, topic)
		pub.endFunc(p)
		assert.NoError(pub.t, err)
	}
	pub.handleFunc(pub.beginFunc(), handler)

	// get nil
	handler = func(p PubsubOps) {
		m1, err = p.Retain(ns, topic)
		pub.endFunc(p)
		assert.NoError(pub.t, err)
		assert.Equal(pub.t, m1, m3)
	}
	pub.handleFunc(pub.beginFunc(), handler)

	//get nil
	handler = func(p PubsubOps) {
		m2, err := p.Retain(ns, "t2")
		pub.endFunc(p)
		assert.NoError(pub.t, err)
		assert.Equal(pub.t, m2, m3)
	}
	pub.handleFunc(pub.beginFunc(), handler)
}

func (pub *pubsubT) OperatorSubscribe(ns, topic string) {
	var (
		err error
	)
	handler := func(p PubsubOps) {
		err := p.Subscribe(ns, topic, "c1", true)
		pub.endFunc(p)
		assert.NoError(pub.t, err)
	}
	pub.handleFunc(pub.beginFunc(), handler)

	handler = func(p PubsubOps) {
		sub, err := p.HasSubscriber(ns, topic)
		pub.endFunc(p)
		assert.NoError(pub.t, err)
		assert.True(pub.t, sub)
	}
	pub.handleFunc(pub.beginFunc(), handler)

	handler = func(p PubsubOps) {
		err = p.Unsubscribe(ns, topic, "c1")
		pub.endFunc(p)
		assert.NoError(pub.t, err)
	}
	pub.handleFunc(pub.beginFunc(), handler)

	handler = func(p PubsubOps) {
		err = p.Unsubscribe(ns, topic, "c1")
		pub.endFunc(p)
		assert.NoError(pub.t, err)
	}
	pub.handleFunc(pub.beginFunc(), handler)

	handler = func(p PubsubOps) {
		sub, err := p.HasSubscriber(ns, topic)
		pub.endFunc(p)
		assert.NoError(pub.t, err)
		assert.False(pub.t, sub)
	}
	pub.handleFunc(pub.beginFunc(), handler)
}

func (pub *pubsubT) OperatorDrop(ns, topic string) {
	handler := func(p PubsubOps) {
		err := p.Subscribe(ns, topic, "c1", true)
		assert.NoError(pub.t, err)
		msg := &pbMessage.Message{Topic: "t1"}
		err = p.SetRetain(ns, topic, msg)
		assert.NoError(pub.t, err)
		pub.endFunc(p)
	}
	pub.handleFunc(pub.beginFunc(), handler)

	handler = func(p PubsubOps) {
		p.Drop(ns, topic)
		pub.endFunc(p)
	}
	pub.handleFunc(pub.beginFunc(), handler)

	var m3 *pbMessage.Message
	handler = func(p PubsubOps) {
		m1, err := p.Retain(ns, topic)
		pub.endFunc(p)
		assert.NoError(pub.t, err)
		assert.Equal(pub.t, m1, m3)
	}
	pub.handleFunc(pub.beginFunc(), handler)

	handler = func(p PubsubOps) {
		sub, err := p.HasSubscriber(ns, topic)
		pub.endFunc(p)
		assert.NoError(pub.t, err)
		assert.False(pub.t, sub)
	}
	pub.handleFunc(pub.beginFunc(), handler)
}

func (pub *pubsubT) OperatorOnline(ns, topic string) {
	handler := func(p PubsubOps) {
		err := p.Subscribe(ns, topic, "c1", true)
		pub.endFunc(p)
		assert.NoError(pub.t, err)
	}
	pub.handleFunc(pub.beginFunc(), handler)

	handler = func(p PubsubOps) {
		has, err := p.ListOnlineSubscribers(ns, topic)
		pub.endFunc(p)
		assert.NoError(pub.t, err)
		assert.Equal(pub.t, has, []string{"c1"})
	}
	pub.handleFunc(pub.beginFunc(), handler)

	handler = func(p PubsubOps) {
		p.Drop(ns, topic)
		pub.endFunc(p)
	}
	pub.handleFunc(pub.beginFunc(), handler)

	handler = func(p PubsubOps) {
		has, err := p.ListOnlineSubscribers(ns, topic)
		pub.endFunc(p)
		assert.NoError(pub.t, err)
		assert.Equal(pub.t, has, []string{})
	}
	pub.handleFunc(pub.beginFunc(), handler)
}

func (pub *pubsubT) OperatorOffline(ns, topic string) {
	handler := func(p PubsubOps) {
		err := p.Subscribe(ns, topic, "c1", false)
		pub.endFunc(p)
		assert.NoError(pub.t, err)
	}
	pub.handleFunc(pub.beginFunc(), handler)

	handler = func(p PubsubOps) {
		has, err := p.ListOfflineSubscribers(ns, topic)
		pub.endFunc(p)
		assert.NoError(pub.t, err)
		assert.Equal(pub.t, has, []string{"c1"})
	}
	pub.handleFunc(pub.beginFunc(), handler)

	handler = func(p PubsubOps) {
		p.Drop(ns, topic)
		pub.endFunc(p)
	}
	pub.handleFunc(pub.beginFunc(), handler)

	handler = func(p PubsubOps) {
		has, err := p.ListOnlineSubscribers(ns, topic)
		pub.endFunc(p)
		assert.NoError(pub.t, err)
		assert.Equal(pub.t, has, []string{})
	}
	pub.handleFunc(pub.beginFunc(), handler)
}
