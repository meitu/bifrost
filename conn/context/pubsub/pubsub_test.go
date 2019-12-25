package pubsub

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/distributedio/configo"
	"github.com/meitu/bifrost/conn/conf"
	ph "github.com/meitu/bifrost/grpc/push"
	"github.com/stretchr/testify/assert"
)

var (
	PubsubConfig conf.Pubsub
	TopicConfig  conf.Topic
)

func init() {
	path := os.Getenv("GOPATH")
	path = path + "/src/github.com/meitu/bifrost/conn/conf/connd.toml"
	var config conf.Connd
	if err := configo.Load(path, &config); err != nil {
		fmt.Printf("load Config file failed , %s\n", err)
		os.Exit(1)
	}
	if err := config.Validate(); err != nil {
		panic(err)
	}
	PubsubConfig = config.Pubsub
	TopicConfig = config.Pubsub.Topic
}

/*
type testItem struct {
	topic   string
	service string
	cs      []*Connd
}

func testCompareItem(t *testing.T, tm *Pubsub, items []testItem) {
	for _, item := range items {
		assert.Equal(t, len(item.cs), tm.Size(item.service, item.topic), "")
		for _, c := range item.cs {
			req := &ph.RemoveRouteReq{
				Service: item.service,
				Topic:   item.topic,
			}
			assert.NoError(t, tm.RemoveRoute(req, c))
		}
		assert.Equal(t, 0, tm.Size(item.service, item.topic), "")
	}
}
*/

func TestPubsubAddRouteDel(t *testing.T) {
	tm := NewPubsub(&PubsubConfig, ctx, pcli)
	assert.Equal(t, tm.Size(service, topic1), 0)
	var wg sync.WaitGroup
	// add 2 del 1
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := tm.AddRoute(&ph.AddRouteReq{
				Service:     service,
				Topic:       topic1,
				GrpcAddress: "127.0.0.1",
				Version:     uint64(time.Now().UnixNano()),
			}, 1)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := tm.RemoveRoute(&ph.RemoveRouteReq{
				Service:     service,
				Topic:       topic1,
				GrpcAddress: "127.0.0.1",
				Version:     uint64(time.Now().UnixNano()),
			}, 1)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	assert.Equal(t, tm.Size(service, topic1), 90)
	for i := 10; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := tm.RemoveRoute(&ph.RemoveRouteReq{
				Service:     service,
				Topic:       topic1,
				GrpcAddress: "127.0.0.1",
				Version:     uint64(time.Now().UnixNano()),
			}, 1)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	assert.Equal(t, tm.Size(service, topic1), 0)
	assert.Equal(t, len(tm.topicMap), 0)
}

var (
	topic1  = "topic-1"
	topic2  = "topic-2"
	service = "service"

	req1 = &ph.AddRouteReq{
		Service:     service,
		Topic:       topic1,
		GrpcAddress: "127.0.0.1",
	}

	req2 = &ph.AddRouteReq{
		Service:     service,
		Topic:       topic2,
		GrpcAddress: "127.0.0.1",
	}

	rreq1 = &ph.RemoveRouteReq{
		Service:     service,
		Topic:       topic1,
		GrpcAddress: "127.0.0.1",
	}

	rreq2 = &ph.RemoveRouteReq{
		Service:     service,
		Topic:       topic2,
		GrpcAddress: "127.0.0.1",
	}
)

func TestPubsubNotify(t *testing.T) {
	tm := NewPubsub(&PubsubConfig, ctx, pcli)
	cs := make([]*Connd, 100)
	for i := 0; i < 100; i++ {
		cs[i] = &Connd{v: i}
	}
	for _, c := range cs {
		assert.NoError(t, tm.AddRoute(req1, c))
		assert.NoError(t, tm.AddRoute(req2, c))
	}

	assert.Equal(t, len(tm.topicMap), 2)
	assert.Equal(t, tm.Size(service, topic1), 100)
	assert.Equal(t, tm.Size(service, topic2), 100)

	for i := 5; i < 40; i++ {
		err := tm.RemoveRoute(rreq1, cs[i])
		assert.NoError(t, err)
	}
	for i := 40; i > 10; i-- {
		err := tm.RemoveRoute(rreq2, cs[i])
		assert.NoError(t, err)
	}

	assert.Equal(t, len(tm.topicMap), 2)
	assert.Equal(t, 65, tm.Size(service, topic1))
	assert.Equal(t, 70, tm.Size(service, topic2))
	//TODO scan

	var count int32
	handler := func(c interface{}) error {
		atomic.AddInt32(&count, 1)
		return nil
	}

	tm.Scan(service, topic1, handler)
	assert.Equal(t, 65, int(count))

	count = 0
	tm.Scan(service, topic2, handler)
	assert.Equal(t, 70, int(count))

}

func BenchmarkPubsubRemoveRoute(b *testing.B) {
	var wg sync.WaitGroup
	b.StopTimer()
	c := make([]*Connd, b.N)
	tm := NewPubsub(&PubsubConfig, ctx, pcli)
	for i := 0; i < b.N; i++ {
		c[i] = &Connd{v: i}
		assert.NoError(b, tm.AddRoute(req1, c[i]))
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			assert.NoError(b, tm.AddRoute(req1, &Connd{v: i}))
		}
	}()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		assert.NoError(b, tm.RemoveRoute(rreq1, c[i]))
	}
	b.StopTimer()
	wg.Wait()
}

func BenchmarkPubsubAddRoute(b *testing.B) {
	var wg sync.WaitGroup
	b.StopTimer()
	c := make([]*Connd, b.N)
	tm := NewPubsub(&PubsubConfig, ctx, pcli)
	for i := 0; i < b.N; i++ {
		c[i] = &Connd{v: i}
	}
	wg.Add(1)
	go func() {
		time.Sleep(time.Millisecond * 10)
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			assert.NoError(b, tm.RemoveRoute(rreq1, c[i]))
		}
	}()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		assert.NoError(b, tm.AddRoute(req1, c[i]))
	}
	b.StopTimer()
	wg.Wait()
}

func BenchmarkPubsubScan(b *testing.B) {
	var wg sync.WaitGroup
	b.StopTimer()
	c := make([]*Connd, 10000)
	tm := NewPubsub(&PubsubConfig, ctx, pcli)
	for i := 0; i < 10000; i++ {
		c[i] = &Connd{v: i}
		assert.NoError(b, tm.AddRoute(req1, c[i]))
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			assert.NoError(b, tm.RemoveRoute(rreq1, c[i]))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			assert.NoError(b, tm.AddRoute(req1, c[i]))
		}
	}()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tm.Scan(service, topic1, func(c interface{}) error { return nil })
	}
	b.StopTimer()
	wg.Wait()
}
