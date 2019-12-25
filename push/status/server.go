package status

import (
	"fmt"
	"io"
	"net"
	"net/http"

	"github.com/meitu/bifrost/push/conf"
)

type Server struct {
	statusServer *http.Server
	trace        io.Closer
}

func NewServer(config *conf.Status) (*Server, error) {
	s := &Server{
		statusServer: &http.Server{Handler: http.DefaultServeMux},
	}

	if config.Enable {
		HandleMetrics()
	}
	return s, nil
}

func (s *Server) Serve(lis net.Listener) error {
	return s.statusServer.Serve(lis)
}

func (s *Server) Stop() error {
	if s.trace != nil {
		if err := s.trace.Close(); err != nil {
			fmt.Printf("trace stop failed err:%s \n", err)
			return err
		}
	}
	if s.statusServer != nil {
		if err := s.statusServer.Close(); err != nil {
			fmt.Printf("status Server stop failed err:%s \n", err)
			return err
		}
	}
	return nil
}

func (s *Server) GracefulStop() error {
	if s.trace != nil {
		if err := s.trace.Close(); err != nil {
			fmt.Printf("trace stop failed err:%s \n", err)
			return err
		}
	}
	if s.statusServer != nil {
		if err := s.statusServer.Close(); err != nil {
			fmt.Printf("status Server stop failed err:%s \n", err)
			return err
		}
	}
	return nil
}
