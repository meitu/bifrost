package mock

import (
	"testing"
	"time"

	cd "github.com/meitu/bifrost/conn/mock"
	pd "github.com/meitu/bifrost/push/mock"
)

type MockBifrost struct {
	push *pd.MockPushd
	conn *cd.MockConnd
}

func NewMockBifrost(t *testing.T, push *pd.MockPushd, conn *cd.MockConnd) *MockBifrost {
	return &MockBifrost{
		push: push,
		conn: conn,
	}
}

func (bif *MockBifrost) Serve(t *testing.T) {
	bif.push.Start(t)
	time.Sleep(time.Second * 3)
	bif.conn.Start(t)
}

func (bif *MockBifrost) Stop(t *testing.T) {
	bif.push.Stop(t)
	bif.conn.Stop(t)
}
