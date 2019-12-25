package pubsub

import (
	"testing"

	"github.com/meitu/bifrost/push/store/mock"
)

func MockRedisPubsub() PubsubOps {
	return NewRdsPubsub(mock.MockRedis(), mock.MockQueue())
}

func ATestRedisPubsub(t *testing.T) {
	rt := NewPubsubT(t, MockRedisPubsub, func(p PubsubOps) {})
	rt.OperatorRetain("ns", "topic")
	rt.OperatorSubscribe("ns", "topic")
	rt.OperatorOnline("ns", "topic")
	rt.OperatorOffline("ns", "topic")
	rt.OperatorPublish("ns", "topic")
}
