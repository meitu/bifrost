package context

import (
	"context"
	"sync"

	"github.com/meitu/bifrost/conn/conf"
	"github.com/meitu/bifrost/conn/context/clients"
	"github.com/meitu/bifrost/conn/context/pubsub"
	"github.com/meitu/bifrost/conn/context/pushcli"
	"github.com/meitu/bifrost/conn/poll"
)

// Context alias of the system library
type Context struct {
	context.Context
}

// CancelFunc tells an operation to abandon its work
type CancelFunc context.CancelFunc

// Canceled is the error returned by Context.Err when the context is canceled.
var Canceled = context.Canceled

// Background returns a non-nil, empty Context. It is never canceled, has no
// values, and has no deadline. It is typically used by the main function,
// initialization, and tests, and as the top-level Context for incoming
func Background() *Context {
	return &Context{context.Background()}
}

// WithCancel returns a copy of parent with a new Done channel. The returned
// context's Done channel is closed when the returned cancel function is called
// or when the parent context's Done channel is closed, whichever happens first.
func WithCancel(ctx *Context) (*Context, CancelFunc) {
	cctx, cancel := context.WithCancel(ctx.Context)
	return &Context{cctx}, CancelFunc(cancel)
}

// BaseContext is the runtime context of the base
type BaseContext struct {
	*Context
	PushCli *pushcli.PushCli
	Clients *clients.Clients
	Pubsub  *pubsub.Pubsub
}

// WithCancel returns a copy of parent with a new Done channel
func (ctx *BaseContext) WithCancel() (*BaseContext, CancelFunc) {
	cctx := *ctx
	child, cancel := WithCancel(ctx.Context)
	cctx.Context = child
	return &cctx, cancel
}

// GrcpSrvContext is the runtime context of the grpc server
type GrpcSrvContext struct {
	*BaseContext
}

// MqttSrvContext is the runtime context of the mqtt server
type MqttSrvContext struct {
	*BaseContext

	GrpcAddr string
	Conf     *conf.Session

	Poll        *poll.Poll
	Clock       *sync.RWMutex // Must be a pointer to implement subclasses and husband classes sharing
	Connections map[int]interface{}
}

// AddSession add session in ctx
func (ctx *MqttSrvContext) AddSession(fd int, s interface{}) {
	ctx.Clock.Lock()
	ctx.Connections[fd] = s
	ctx.Clock.Unlock()
}

// RemoveSession delete a session
func (ctx *MqttSrvContext) RemoveSession(fd int) {
	ctx.Clock.Lock()
	delete(ctx.Connections, fd)
	ctx.Clock.Unlock()
}

// GetSession get session in fd
func (ctx *MqttSrvContext) GetSession(fd int) interface{} {
	ctx.Clock.RLock()
	s := ctx.Connections[fd]
	ctx.Clock.RUnlock()
	return s
}

// WithCancel returns a copy of parent with a new Done channel
func (ctx *MqttSrvContext) WithCancel() (*MqttSrvContext, CancelFunc) {
	cctx := *ctx
	child, cancel := ctx.BaseContext.WithCancel()
	cctx.BaseContext = child
	return &cctx, cancel
}
