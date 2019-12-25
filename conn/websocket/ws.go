package websocket

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/gobwas/ws"
	"github.com/shafreeck/retry"
	"go.uber.org/zap"
)

const (
	WebsocketListenBacklog = 512
	WebsocketAcceptTimeout = 3 * time.Second
)

type WebsocketListener struct {
	lis     net.Listener
	logger  *zap.Logger
	path    string
	server  *http.Server
	acceptQ chan net.Conn
	done    chan struct{}
}

//ListenWebsocket wrap a listener to a websocket listener
func ListenWebsocket(lis net.Listener, path string, logger *zap.Logger) *WebsocketListener {
	wl := &WebsocketListener{}
	wl.path = path
	wl.lis = lis
	wl.logger = logger
	wl.acceptQ = make(chan net.Conn, WebsocketListenBacklog)

	handler := func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w, nil)
		if err != nil {
			wl.logger.Error("failed to upgrade to websocket", zap.Error(err))
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		timeout, cancel := context.WithTimeout(context.Background(), WebsocketAcceptTimeout)
		defer cancel()

		err = retry.Ensure(timeout, func() error {
			select {
			case wl.acceptQ <- conn:
			default:
				wl.logger.Warn("webosocket accept queue is overflow")
				return retry.Retriable(errors.New("accept queue is overflow"))
			}
			return nil
		})

		if err != nil {
			wl.logger.Error("failed to create websocket conn", zap.Error(err))
			w.WriteHeader(http.StatusServiceUnavailable)
		}

	}

	mux := http.NewServeMux()
	mux.HandleFunc(wl.path, handler)
	srv := &http.Server{Handler: mux}
	go func() {
		if err := srv.Serve(lis); err != nil {
			if err == http.ErrServerClosed {
				wl.logger.Warn("websocket internal listener closed", zap.Error(err))
			} else {
				wl.logger.Error("websocket http serve failed, websocket listener dose not work now", zap.Error(err))
			}
		}
	}()
	return wl
}

func (lis *WebsocketListener) Accept() (net.Conn, error) {
	select {
	case <-lis.done:
		return nil, errors.New("websocket listener closed")
	case conn, ok := <-lis.acceptQ:
		if !ok {
			return nil, errors.New("websocket listener closed")
		}
		return conn, nil
	}
}
func (lis *WebsocketListener) Close() error {
	if lis.acceptQ != nil {
		close(lis.acceptQ)
		close(lis.done)
		lis.acceptQ = nil
	}
	return lis.lis.Close()
}
func (lis *WebsocketListener) Addr() net.Addr {
	return lis.lis.Addr()
}
