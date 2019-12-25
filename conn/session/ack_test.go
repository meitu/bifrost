package session

import (
	"strconv"
	"sync"
	"testing"
	"time"

	ph "github.com/meitu/bifrost/grpc/push"
	"github.com/stretchr/testify/assert"
)

var (
	index int64
)

func getUnacks() []*ph.UnackDesc {
	acks := make([]*ph.UnackDesc, 0, 9)
	for ; len(acks) < 9; index++ {
		str := []byte(strconv.Itoa(int(index)))
		ind := make([]byte, 16)
		copy(ind, str)
		acks = append(acks, &ph.UnackDesc{Topic: "t1",
			Index:     ind,
			MessageID: index,
			BizID:     str,
			TraceID:   "trace",
			Payload:   str})
	}
	return acks
}

func TestACK(t *testing.T) {
	sw := NewACK()
	var wg sync.WaitGroup
	wg.Add(1)
	sw.PutUnack(sctx, &ph.PutUnackReq{
		TraceID:      "trace",
		ClientID:     "cid",
		Messages:     getUnacks(),
		Service:      "service",
		CleanSession: true,
	})

	go func() {
		defer wg.Done()
		sw.PutUnack(sctx, &ph.PutUnackReq{
			TraceID:      "trace",
			ClientID:     "cid",
			Messages:     getUnacks(),
			Service:      "service",
			CleanSession: true,
		})
	}()
	for i := 0; i < 8; i++ {
		resp, err := sw.DelUnack(sctx, &ph.DelUnackReq{
			ClientID:     "cid",
			MessageID:    int64(i),
			TraceID:      "trace",
			Service:      "service",
			CleanSession: true,
			StatLabel:    "stat",
		})
		assert.NoError(t, err)
		assert.Equal(t, string(resp.BizID), strconv.Itoa(i))
	}

	time.Sleep(time.Millisecond * 100)

	_, err := sw.DelUnack(sctx, &ph.DelUnackReq{
		ClientID:     "cid",
		MessageID:    12,
		TraceID:      "trace",
		Service:      "service",
		CleanSession: true,
		StatLabel:    "stat",
	})
	assert.Equal(t, err.Error(), "mid not match")
	wg.Wait()
}

func TestACKFalse(t *testing.T) {
	index = 0
	sw := NewACK()
	var wg sync.WaitGroup
	wg.Add(1)
	sw.PutUnack(sctx, &ph.PutUnackReq{
		TraceID:      "trace",
		ClientID:     "cid",
		Messages:     getUnacks(),
		Service:      "service",
		CleanSession: false,
	})
	go func() {
		defer wg.Done()
		sw.PutUnack(sctx, &ph.PutUnackReq{
			TraceID:      "trace",
			ClientID:     "cid",
			Messages:     getUnacks(),
			Service:      "service",
			CleanSession: false,
		})
	}()
	for i := 0; i < 8; i++ {
		resp, err := sw.DelUnack(sctx, &ph.DelUnackReq{
			ClientID:     "cid",
			MessageID:    int64(i),
			TraceID:      "trace",
			Service:      "service",
			CleanSession: false,
			StatLabel:    "stat",
		})
		assert.NoError(t, err)
		assert.Equal(t, string(resp.BizID), strconv.Itoa(i))
	}
	_, err := sw.DelUnack(sctx, &ph.DelUnackReq{
		ClientID:     "cid",
		MessageID:    12,
		TraceID:      "trace",
		Service:      "service",
		CleanSession: false,
		StatLabel:    "stat",
	})
	assert.NotNil(t, err)
	wg.Wait()
}
