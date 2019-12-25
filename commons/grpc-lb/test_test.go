package lb

import (
	fmt "fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/coreos/etcd/integration"
	"github.com/coreos/pkg/capnslog"
	"github.com/stretchr/testify/assert"
)

var etcdAddrs []string

func TestMain(m *testing.M) {
	t := &testing.T{}
	capnslog.SetFormatter(capnslog.NewDefaultFormatter(ioutil.Discard))
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1, ClientTLS: nil})
	etcdAddrs = clus.RandClient().Endpoints()
	v := m.Run()
	clus.Terminate(t)
	os.Exit(v)
}

func TestNodeBlance(t *testing.T) {
	srv := "push"
	s1 := NewServer(etcdAddrs, srv, "1")
	s2 := NewServer(etcdAddrs, srv, "2")
	s3 := NewServer(etcdAddrs, srv, "3")
	c1 := NewClient(etcdAddrs, srv)
	c2 := NewClient(etcdAddrs, srv)
	go s1.Run("localhost:1234")
	time.Sleep(time.Second)
	go s2.Run("localhost:1235")
	time.Sleep(time.Second)
	go s3.Run("localhost:1236")
	time.Sleep(time.Second)
	resp, err := c1.Run("c1 1")
	assert.NoError(t, err)
	fmt.Println(resp)
	s1.Close()
	time.Sleep(time.Second)

	resp, err = c1.Run("c1 2")
	assert.NoError(t, err)
	fmt.Println(resp)

	time.Sleep(time.Second)

	resp, err = c2.Run("c2 3")
	assert.NoError(t, err)
	fmt.Println(resp)
}
