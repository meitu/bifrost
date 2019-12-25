package util

import (
	"fmt"
	"testing"

	"github.com/meitu/bifrost/commons/incr_uuid"
	"github.com/meitu/bifrost/push/store/mock"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

type MockPubsub struct {
	*Pubsub
	t *testing.T
}

func HandlerPubsub(t *testing.T, handler func(pubsub *MockPubsub)) *MockPubsub {
	txn, _ := mock.Store.Begin()
	pubsub, err := NewPubsub(txn, "namespace", "name", "pubsub")
	assert.NoError(t, err)
	pub := &MockPubsub{Pubsub: pubsub, t: t}
	handler(pub)

	txn.Commit(context.Background())
	return pub
}

func CleanPubsub(t *testing.T) {
	txn, _ := mock.Store.Begin()
	pubsub, err := NewPubsub(txn, "namespace", "name", "pubsub")
	assert.NoError(t, err)
	mq := &MockPubsub{Pubsub: pubsub, t: t}
	mq.Drop()
	txn.Commit(context.Background())
}

func SetPubsubData(t *testing.T) ([][]byte, []Data) {
	var datas []Data
	txn, _ := mock.Store.Begin()
	pubsub, err := NewPubsub(txn, "namespace", "name", "pubsub")
	assert.NoError(t, err)
	for i := 0; i < 10; i++ {
		datas = append(datas, Data(fmt.Sprintf("%d", i)))
	}
	offs, err := pubsub.Append(datas)
	assert.NoError(t, err)
	txn.Commit(context.Background())
	return offs, datas
}

func At(t *testing.T, offset []byte) Data {
	txn, _ := mock.Store.Begin()
	pubsub, err := NewPubsub(txn, "namespace", "name", "pubsub")
	data, err := pubsub.txn.Get(pubsub.getDataKey(offset))
	assert.NoError(t, err)
	txn.Commit(context.Background())
	return data
}

func TestPubsubAppend(t *testing.T) {
	tests := []struct {
		name string
		args Data
		want Data
	}{
		{
			name: "1",
			args: Data("he"),
			want: Data("he"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var off []byte
			handler := func(pub *MockPubsub) {
				offs, err := pub.Append([]Data{tt.args})
				assert.NoError(t, err)
				off = offs[0]
			}
			HandlerPubsub(t, handler)
			data := At(t, off)
			assert.Equal(t, tt.want, data)
		})
	}
}

func TestPubsubScan(t *testing.T) {
	defer CleanPubsub(t)
	offs, wants := SetPubsubData(t)
	tests := []struct {
		name  string
		args  []byte
		wants []Data
	}{
		{
			name:  "1",
			args:  offs[0],
			wants: wants,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var i int
			scanHandler := func(offset *incr_uuid.Offset, data Data) (bool, error) {
				assert.Equal(t, data, tt.wants[i])
				i++
				return false, nil
			}
			handler := func(q *MockPubsub) {
				err := q.Scan(tt.args, scanHandler)
				assert.NoError(t, err)
			}
			HandlerPubsub(t, handler)
			assert.Len(t, tt.wants, i)
		})
	}
}
