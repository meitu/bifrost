package session

import (
	"testing"

	"github.com/meitu/bifrost/push/store/mock"
)

func MockRedisSession() SessionOps {
	return NewRdsSession(mock.MockRedis())
}

func ATestRedisSession(t *testing.T) {
	rt := NewSessionT(t, MockRedisSession, func(p SessionOps) {})
	rt.OperatorMessageID("ns", "c1")
	rt.OperatorSubscribe("ns", "c1")
	rt.OperatorCursor("ns", "c1")
	rt.OperatorConndAddress("ns", "c1")
	rt.OperatorDrop("ns", "c1")
	//bug
	// rt.OperatorUnack("ns", "c1")
}
