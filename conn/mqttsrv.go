package conn

import (
	"io"
	"net"
	"runtime/debug"
	"time"

	"github.com/meitu/bifrost/commons/log"
	"github.com/meitu/bifrost/conn/context"
	"github.com/meitu/bifrost/conn/poll"
	"github.com/meitu/bifrost/conn/session"
	"github.com/meitu/bifrost/conn/status/metrics"
	"go.uber.org/zap"
)

// MQTTServer
type MQTTServer struct {
	ctx      *context.MqttSrvContext
	cancel   context.CancelFunc
	listener poll.Listener
}

// NewMQTTServer create a new mqtt server
func NewMQTTServer(ctx *context.MqttSrvContext) *MQTTServer {
	cctx, cancel := ctx.WithCancel()
	srv := &MQTTServer{
		ctx:    cctx,
		cancel: cancel,
	}
	return srv
}

// Stop provide fastclose, will not send will msg
func (s *MQTTServer) Stop() error {
	s.listener.Close()
	s.ctx.Poll.Close()
	s.cancel()
	return nil
}

// GracefulStop first close the client connection before pushing out the program
func (s *MQTTServer) GracefulStop() error {
	// do will message send and close connection
	s.listener.Close()

	// close all clients
	s.ctx.Clients.Scan(func(cli interface{}) error {
		cli.(*session.Session).Close()
		return nil
	})

	// close the root context, and all clients detect an event that automatically initiates a disconnect
	s.ctx.Poll.Close()
	s.cancel()

	// wait clients close comlete
	// max wait time 9s
	count := 0
	for s.ctx.Clients.Length() != 0 {
		time.Sleep(time.Second * 3)
		if count == 3 {
			log.Error("closed client wait too long")
			break
		}
		count++
	}

	return nil
}

// Serve start server
func (s *MQTTServer) Serve(l net.Listener) error {
	ln, err := poll.NewListener(l)
	if err != nil {
		return err
	}

	s.ctx.Poll.AddRead(ln.FD(), nil)
	s.listener = ln

	return s.ctx.Poll.Wait(func(ev poll.Event) error {
		if ev.FD() == ln.FD() {
			go s.loopAccept(ev)
			return nil
		}

		cli := s.ctx.GetSession(ev.FD())
		if cli == nil {
			return nil
		}
		conn := cli.(*session.Session)

		// start read
		if (ev.Type()&poll.REvent) != 0 && conn.CompareAndSwapReadState(context.Idle, context.Busy) {
			go s.loopRead(conn, ev)
		}

		// start write
		if conn.CompareAndSwapWriteState(context.Idle, context.Busy) {
			go s.loopWrite(conn, ev)
		}
		return nil
	})
}

func (s *MQTTServer) loopAccept(ev poll.Event) {
	defer func() {
		if err := recover(); err != nil {
			log.Error("panic : loop read failed", zap.Reflect("err", err), zap.String("stack", string(debug.Stack())))
			metrics.GetMetrics().ClientPanicCount.Inc()
		}
	}()

	rawconn, err := s.listener.Accept()
	if err != nil {
		log.Error("server accept failed", zap.Stringer("addr", s.listener.Addr()), zap.Error(err))
		return
	}

	// add listen read event
	if err := s.ctx.Poll.ModRead(ev); err != nil {
		log.Error("panic : add listenFD read event failed", zap.Error(err))
		// metrics panic
		metrics.GetMetrics().ClientPanicCount.Inc()
		return
	}

	// create session and complete the MQTT connection process
	cli := session.NewSession(s.ctx, rawconn)

	// handle connect package
	if err := cli.Read(); err != nil {
		cli.Close()
		if err == io.EOF {
			return
		}
		log.Error("client connect failed", zap.Error(err))
		return
	}

	// record the connections in map
	s.ctx.AddSession(rawconn.FD(), cli)

	// add the client of read event
	if err := s.ctx.Poll.AddRead(rawconn.FD(), nil); err != nil {
		cli.Close()
		log.Error("the listener of adding read event failed", zap.Error(err))
		return
	}
}

func (s *MQTTServer) loopWrite(conn *session.Session, ev poll.Event) {
	if conn.CompareState(context.ClosedState) {
		conn.StoreWriteState(context.Idle)
		return
	}

	defer func() {
		if err := recover(); err != nil {
			conn.StoreWriteState(context.Idle)
			conn.Close()
			log.Error("panic : loop read failed", zap.Reflect("err", err), zap.String("stack", string(debug.Stack())))
			metrics.GetMetrics().ClientPanicCount.Inc()
		}
	}()

	if err := s.ctx.Poll.ModRead(ev); err != nil {
		log.Info("set read event failed", zap.Int("fd", ev.FD()), zap.Error(err))
		conn.StoreWriteState(context.Idle)
		return
	}

	// Send packets until the buffer is empty
	handler := func() {
		conn.StoreWriteState(context.Idle)
	}
	for conn.IsWrite(handler, nil) {
		if err := conn.Write(); err != nil {
			log.Error("client connect failed", zap.Int("fd", ev.FD()), zap.Error(err))
			conn.StoreWriteState(context.Idle)
			conn.Close()
			return
		}
	}
}

func (s *MQTTServer) loopRead(conn *session.Session, ev poll.Event) {
	if conn.CompareState(context.ClosedState) {
		conn.StoreReadState(context.Idle)
		return
	}

	defer func() {
		if err := recover(); err != nil {
			conn.StoreReadState(context.Idle)
			conn.Close()
			log.Error("panic : loop read failed", zap.Reflect("err", err), zap.String("stack", string(debug.Stack())))
			metrics.GetMetrics().ClientPanicCount.Inc()
		}
	}()

	if err := conn.Read(); err != nil {
		conn.StoreReadState(context.Idle)
		conn.Close()
		if err == io.EOF || err == session.ErrClosed {
			return
		}
		log.Error("client read failed", zap.Int("fd", ev.FD()), zap.Error(err))
		return
	}
	conn.StoreReadState(context.Idle)

	whandler := func() {
		if conn.CompareWriteState(context.Busy) {
			return
		}
		if err := s.ctx.Poll.ModReadWrite(ev); err != nil {
			log.Info("set read and wirte event failed", zap.Int("fd", ev.FD()), zap.Error(err))
		}
	}
	rhandler := func() {
		if err := s.ctx.Poll.ModRead(ev); err != nil {
			log.Info("set read event failed", zap.Int("fd", ev.FD()), zap.Error(err))
		}
	}
	conn.IsWrite(rhandler, whandler)
}
