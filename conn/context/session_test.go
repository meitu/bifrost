package context

import (
	"testing"

	"github.com/meitu/bifrost/conn/conf"
)

func TestSession(t *testing.T) {
	bctx := &BaseContext{
		Context: Background(),
	}
	mctx := &MqttSrvContext{
		BaseContext: bctx,
		Conf:        &conf.Session{},
	}

	ctx, cancel := NewSessionContext(mctx, nil)
	cancel()
	<-ctx.Done()

	ctx, cancel = NewSessionContext(mctx, nil)
	cctx := ctx
	ccctx := ctx
	cancel()
	<-cctx.Done()
	<-ccctx.Done()
}
