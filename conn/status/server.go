package status

import (
	"fmt"
	"net"
	"net/http"

	"github.com/meitu/bifrost/conn/conf"
	"github.com/meitu/bifrost/conn/status/metrics"
)

// Server state of th server
// Status contains reported monitoring information and provides basic operations for the client
type Server struct {
	statusServer *http.Server
	addr         string
}

// NewServer create a new the status server
func NewServer(config *conf.Monitor) (*Server, error) {
	s := &Server{
		addr:         config.Listen,
		statusServer: &http.Server{Handler: http.DefaultServeMux},
	}
	if config.Enable {
		s.handleFunc()
	}
	return s, nil
}

// Serve start http server
func (s *Server) Serve(lis net.Listener) error {
	return s.statusServer.Serve(lis)
}

// Stop close http server
func (s *Server) Stop() error {
	if s.statusServer != nil {
		if err := s.statusServer.Close(); err != nil {
			return err
		}
	}
	return nil
}

// GracefulStop http server
func (s *Server) GracefulStop() error {
	if s.statusServer != nil {
		if err := s.statusServer.Close(); err != nil {
			fmt.Printf("status Server stop failed err:%s \n", err)
			return err
		}
	}
	return nil
}

// handleFunc registers the handler function for the given pattern in the DefaultServeMux.
func (s *Server) handleFunc() {
	// http.HandleFunc("/connection", closeHandleFunc)
	metrics.Handler()
}
