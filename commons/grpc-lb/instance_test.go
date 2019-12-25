package lb

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/coreos/etcd/clientv3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
)

type Client struct {
	srv string
	cli TestClient
	r   *Resolver
}

func NewClient(addrs []string, srv string) *Client {
	cc := clientv3.Config{Endpoints: addrs}
	r := NewResolver(cc, srv)
	resolver.Register(r)
	conn, err := grpc.Dial(r.URL(), grpc.WithBalancerName("round_robin"), grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	return &Client{
		srv: srv,
		cli: NewTestClient(conn),
		r:   r,
	}
}

func (c *Client) Run(content string) (string, error) {
	content = fmt.Sprintf("the client is %s.", content)
	resp, err := c.cli.Say(context.Background(), &SayReq{Content: content}, grpc.FailFast(true))
	if err != nil {
		return "", err
	}
	return resp.Content, err
}

func (c *Client) Close() {
	c.r.Close()
}

type Server struct {
	cc      clientv3.Config
	srv     string
	content string
	r       *Node
	ss      *grpc.Server
}

func NewServer(addrs []string, srv, num string) *Server {
	return &Server{
		cc:      clientv3.Config{Endpoints: addrs},
		srv:     srv,
		content: fmt.Sprintf("the server is %s.", num),
	}
}

func (s *Server) Run(addr string) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %s", err)
	}

	s.ss = grpc.NewServer()

	RegisterTestServer(s.ss, s)

	s.r, err = NewNode(s.cc, s.srv, addr)
	go s.r.Register()

	if err := s.ss.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}
}

func (s *Server) Close() {
	s.r.Deregister()
	s.ss.GracefulStop()
}

func (s *Server) Say(ctx context.Context, req *SayReq) (*SayResp, error) {
	return &SayResp{Content: req.Content + " AND " + s.content}, nil
}
