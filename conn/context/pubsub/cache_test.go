package pubsub

import (
	"fmt"
	"testing"

	ph "github.com/meitu/bifrost/grpc/push"
	"github.com/stretchr/testify/assert"
)

func TestCache_getResp(t *testing.T) {
	cache := newCache(32)
	msgs := make([]*ph.Message, 0, 8)
	for i := 1; i <= 8; i++ {
		msgs = append(msgs, &ph.Message{
			Index: []byte(fmt.Sprintf("%d", i)),
		})
	}
	cache.putMessages([]byte("0"), msgs)
	endOffset := []byte{'8', 0}

	tests := []struct {
		name   string
		cache  *Cache
		offset []byte
		want   *ph.PullResp
	}{
		{
			name:   "offset exist",
			cache:  cache,
			offset: []byte("1"),
			want: &ph.PullResp{
				Messages: msgs[:8],
				Offset:   endOffset,
			},
		},
		{
			name:   "part messages",
			cache:  cache,
			offset: []byte("3"),
			want: &ph.PullResp{
				Messages: msgs[2:8],
				Offset:   endOffset,
			},
		},
		{
			name:   "part messages not math",
			cache:  cache,
			offset: []byte("30"),
			want: &ph.PullResp{
				Messages: msgs[3:8],
				Offset:   endOffset,
			},
		},
		{
			name:   "part messages not math",
			cache:  cache,
			offset: []byte("30"),
			want: &ph.PullResp{
				Messages: msgs[3:8],
				Offset:   endOffset,
			},
		},
		{
			name:   "last messages",
			cache:  cache,
			offset: []byte("8"),
			want: &ph.PullResp{
				Messages: msgs[7:8],
				Offset:   endOffset,
			},
		},
		{
			name:   "not exist",
			cache:  cache,
			offset: []byte("a"),
			want:   nil,
		},
		{
			name:   "earlier than the start time",
			cache:  cache,
			offset: []byte{'0' - 1},
			want:   nil,
		},
		{
			name:   "between offset and start",
			cache:  cache,
			offset: []byte{'0', '0'},
			want: &ph.PullResp{
				Messages: msgs[:8],
				Offset:   endOffset,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.cache.getResp(tt.offset))
		})
	}
}

func TestCache_putMessages(t *testing.T) {
	const CACHE_NUM = 16
	const NUM = CACHE_NUM * 2
	msgs := make([]*ph.Message, NUM)
	for i := 0; i < NUM; i++ {
		msgs[i] = &ph.Message{
			Index: []byte(fmt.Sprintf("%02d", i)),
		}
	}

	type request struct {
		offset []byte
		msgs   []*ph.Message
	}
	tests := []struct {
		name            string
		reqs            []*request
		wantCache       *Cache
		wantBeforeFront []byte
	}{
		{
			name: "put one message",
			reqs: []*request{
				&request{
					offset: msgs[0].Index,
					msgs:   msgs[0:1],
				},
			},
			wantCache: &Cache{
				size:  CACHE_NUM,
				buf:   msgs[0:1],
				front: 1,
				rear:  2,
			},
			wantBeforeFront: msgs[0].Index,
		},
		{
			name: "put two messages",
			reqs: []*request{
				&request{
					offset: msgs[0].Index,
					msgs:   msgs[0:2],
				},
			},
			wantCache: &Cache{
				size:  CACHE_NUM,
				buf:   msgs[0:2],
				front: 1,
				rear:  3,
			},
			wantBeforeFront: msgs[0].Index,
		},
		{
			name: "put mutil messages",
			reqs: []*request{
				&request{
					offset: msgs[0].Index,
					msgs:   msgs[0:10],
				},
			},
			wantCache: &Cache{
				size:  CACHE_NUM,
				buf:   msgs[0:10],
				front: 1,
				rear:  11,
			},
			wantBeforeFront: msgs[0].Index,
		},
		{
			name: "put overflow buf",
			reqs: []*request{
				&request{
					offset: msgs[0].Index,
					msgs:   msgs[0:NUM],
				},
			},
			wantCache: &Cache{
				size:  CACHE_NUM,
				buf:   msgs[NUM-CACHE_NUM+1:],
				front: NUM%CACHE_NUM + 2,
				rear:  NUM%CACHE_NUM + 1,
			},
			wantBeforeFront: msgs[NUM-CACHE_NUM].Index,
		},
		{
			name: "put mutil requests",
			reqs: []*request{
				&request{
					offset: msgs[0].Index,
					msgs:   msgs[0:3],
				},
				&request{
					offset: msgs[2].Index,
					msgs:   msgs[2:5],
				},
			},
			wantCache: &Cache{
				size:  CACHE_NUM,
				buf:   msgs[0:5],
				front: 1,
				rear:  6,
			},
			wantBeforeFront: msgs[0].Index,
		},
		{
			name: "put mutil request and overflow",
			reqs: []*request{
				&request{
					offset: msgs[0].Index,
					msgs:   msgs[0:3],
				},
				&request{
					offset: msgs[2].Index,
					msgs:   msgs[2:],
				},
			},
			wantCache: &Cache{
				size:  CACHE_NUM,
				buf:   msgs[NUM-CACHE_NUM+1:],
				front: 2,
				rear:  1,
			},
			wantBeforeFront: msgs[NUM-CACHE_NUM].Index,
		},
		{
			name: "put no overlap request",
			reqs: []*request{
				&request{
					offset: msgs[0].Index,
					msgs:   msgs[0:2],
				},
				&request{
					offset: msgs[2].Index,
					msgs:   msgs[2:4],
				},
			},
			wantCache: &Cache{
				size:  CACHE_NUM,
				buf:   msgs[0:2],
				front: 1,
				rear:  3,
			},
			wantBeforeFront: msgs[0].Index,
		},
		{
			name: "put overlap request offset",
			reqs: []*request{
				&request{
					offset: msgs[0].Index,
					msgs:   msgs[0:2],
				},
				&request{
					offset: append(msgs[1].Index, byte(0)),
					msgs:   msgs[2:4],
				},
			},
			wantCache: &Cache{
				size:  CACHE_NUM,
				buf:   msgs[0:4],
				front: 1,
				rear:  5,
			},
			wantBeforeFront: msgs[0].Index,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := newCache(CACHE_NUM)
			for _, r := range tt.reqs {
				cache.putMessages(r.offset, r.msgs)
			}
			assert.Equal(t, tt.wantCache.front, cache.front)
			assert.Equal(t, tt.wantCache.rear, cache.rear)
			i, j := cache.front, 0
			for i != cache.rear && j < len(tt.wantCache.buf) {
				assert.Equal(t, tt.wantCache.buf[j], cache.buf[i])
				i = (i + 1) % len(cache.buf)
				j++
			}
			assert.Equal(t, cache.rear, i)
			assert.Equal(t, len(tt.wantCache.buf), j)
			assert.Equal(t, tt.wantBeforeFront, cache.buf[(cache.front-1+len(cache.buf))%len(cache.buf)].Index)
		})
	}
}
