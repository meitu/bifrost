package callback

import (
	"context"
	"testing"
	"time"

	pb "github.com/meitu/bifrost/grpc/callback"
	cb "github.com/meitu/bifrost/misc/fake/callback"
	"github.com/stretchr/testify/assert"
)

func TestMutilCallbackFuncs(t *testing.T) {
	fnames := "OnConnect,PostSubscribe,OnDisconnect,OnSubscribe,OnUnsubscribe,OnACK,OnPublish,OnOffline"
	c1, err := cb.NewCallbackServer(etcdAddrs, service, "s1", fnames)
	assert.NoError(t, err)
	go func() {
		c1.Start("localhost:7742")
	}()
	ctx := context.Background()
	time.Sleep(time.Second)
	time.Sleep(time.Second)
	cresp, err := mockCallback.OnConnect(ctx, "s1", &pb.OnConnectRequest{})
	assert.NoError(t, err)
	assert.Equal(t, []byte("connect"), cresp.Cookie)
	sresp, err := mockCallback.OnSubscribe(ctx, "s1", &pb.OnSubscribeRequest{})
	assert.NoError(t, err)
	assert.Equal(t, []byte("subscribe"), sresp.Cookie)
	presp, err := mockCallback.PostSubscribe(ctx, "s1", &pb.PostSubscribeRequest{})
	assert.NoError(t, err)
	assert.Equal(t, []byte("postsubscribe"), presp.Cookie)
	aresp, err := mockCallback.OnACK(ctx, "s1", &pb.OnACKRequest{})
	assert.NoError(t, err)
	assert.Equal(t, []byte("postreceiveack"), aresp.Cookie)
	pubresp, err := mockCallback.OnPublish(ctx, "s1", &pb.OnPublishRequest{})
	assert.NoError(t, err)
	assert.Equal(t, []byte("publish"), pubresp.Cookie)
	uresp, err := mockCallback.OnUnsubscribe(ctx, "s1", &pb.OnUnsubscribeRequest{})
	assert.NoError(t, err)
	assert.Equal(t, []byte("unsubscribe"), uresp.Cookie)
	_, err = mockCallback.OnDisconnect(ctx, "s1", &pb.OnDisconnectRequest{})
	assert.NoError(t, err)
	oresp, err := mockCallback.OnOffline(ctx, "s1", &pb.OnOfflineRequest{})
	assert.NoError(t, err)
	assert.Equal(t, []byte("offline"), oresp.Cookie)
	c1.Stop()
	time.Sleep(time.Millisecond * 100)
}

func ATestCallbackFuncs(t *testing.T) {
	fnames := "OnConnect"
	c1, err := cb.NewCallbackServer(etcdAddrs, service, "s1", fnames)
	assert.NoError(t, err)
	go func() {
		c1.Start("localhost:7743")
	}()
	time.Sleep(time.Second)

	ctx := context.Background()
	cresp, err := mockCallback.OnConnect(ctx, "s1", &pb.OnConnectRequest{})
	assert.NoError(t, err)
	assert.Equal(t, []byte("connect"), cresp.Cookie)
	sresp, err := mockCallback.OnSubscribe(ctx, "s1", &pb.OnSubscribeRequest{Cookie: cresp.Cookie})
	assert.NoError(t, err)
	assert.Equal(t, []byte("connect"), sresp.Cookie)
	presp, err := mockCallback.PostSubscribe(ctx, "s1", &pb.PostSubscribeRequest{Cookie: cresp.Cookie})
	assert.NoError(t, err)
	assert.Equal(t, []byte("connect"), presp.Cookie)
	aresp, err := mockCallback.OnACK(ctx, "s1", &pb.OnACKRequest{Cookie: cresp.Cookie})
	assert.NoError(t, err)
	assert.Equal(t, []byte("connect"), aresp.Cookie)
	pubresp, err := mockCallback.OnPublish(ctx, "s1", &pb.OnPublishRequest{Cookie: cresp.Cookie})
	assert.NoError(t, err)
	assert.Equal(t, []byte("connect"), pubresp.Cookie)
	uresp, err := mockCallback.OnUnsubscribe(ctx, "s1", &pb.OnUnsubscribeRequest{Cookie: cresp.Cookie})
	assert.NoError(t, err)
	assert.Equal(t, []byte("connect"), uresp.Cookie)
	_, err = mockCallback.OnDisconnect(ctx, "s2", &pb.OnDisconnectRequest{Cookie: cresp.Cookie})
	assert.NoError(t, err)
	oresp, err := mockCallback.OnOffline(ctx, "s1", &pb.OnOfflineRequest{Cookie: cresp.Cookie})
	assert.NoError(t, err)
	assert.Equal(t, []byte("connect"), oresp.Cookie)

	assert.Equal(t, 1, c1.RecvLen())
	c1.Stop()
	time.Sleep(time.Millisecond * 100)
}
