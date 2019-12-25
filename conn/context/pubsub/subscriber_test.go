package pubsub

import (
	"testing"
	"time"

	conn "github.com/meitu/bifrost/grpc/conn"
	ph "github.com/meitu/bifrost/grpc/push"
	"github.com/stretchr/testify/assert"
)

func TestSubscribersRoute(t *testing.T) {
	topics := NewSubscribers(ctx, &TopicConfig, pcli)
	resp := &ph.AddRouteReq{
		TraceID:     "traceid",
		Service:     "service",
		Topic:       "topic",
		GrpcAddress: "1234",
		Version:     1,
	}
	err := topics.AddRoute(resp)
	assert.NoError(t, err)

	err = topics.AddRoute(resp)
	assert.NoError(t, err)

	req := &ph.RemoveRouteReq{
		TraceID:     "traceid",
		Service:     "service",
		Topic:       "topic",
		GrpcAddress: "1234",
		Version:     1,
	}
	err = topics.DelRoute(req)
	assert.NotNil(t, err)

	req.Version = 2
	err = topics.DelRoute(req)
	assert.NoError(t, err)
	topics.Destory()
}

func TestSubscribersNotify_LessCounter(t *testing.T) {
	cf := TopicConfig
	cf.DowngradeLimit = time.Millisecond * 10
	cf.DowngradeThreshold = 2
	topics := NewSubscribers(ctx, &cf, pcli)
	for i := 0; i < 2; i++ {
		topics.Register(&Connd{v: i})
	}
	var count int
	handler := func(c interface{}, topic string, last []byte) {
		count++
		time.Sleep(time.Millisecond * 11)
	}

	tests := []struct {
		name string
		args *conn.NotifyReq
		want int
	}{
		{
			name: "NoneDowngrade",
			args: &conn.NotifyReq{NoneDowngrade: false},
			want: 2,
		},
		{
			name: "Downgrade",
			args: &conn.NotifyReq{NoneDowngrade: true},
			want: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count = 0
			topics.Notify(tt.args, handler)
			assert.Equal(t, tt.want, count)
		})
	}
	topics.Destory()
}

func TestSubscribersNotify_MoreCounter(t *testing.T) {
	cf := TopicConfig
	cf.DowngradeLimit = time.Millisecond * 10
	cf.DowngradeThreshold = 2
	topics := NewSubscribers(ctx, &cf, pcli)
	for i := 0; i < 10; i++ {
		topics.Register(&Connd{v: i})
	}
	var count int
	handler := func(c interface{}, topic string, last []byte) {
		count++
		time.Sleep(time.Millisecond * 11)
	}

	tests := []struct {
		name string
		args *conn.NotifyReq
		want int
	}{
		{
			name: "NoneDowngrade",
			args: &conn.NotifyReq{NoneDowngrade: false},
			want: 1,
		},
		{
			name: "Downgrade",
			args: &conn.NotifyReq{NoneDowngrade: true},
			want: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count = 0
			topics.Notify(tt.args, handler)
			assert.Equal(t, tt.want, count)
		})
	}
	topics.Destory()
}

// maybe 很复杂
func TestSubscribersPull(t *testing.T) {

}
