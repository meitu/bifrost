package store

import (
	"testing"

	pbMessage "github.com/meitu/bifrost/push/message"
	"github.com/meitu/bifrost/push/store/session"

	"github.com/pingcap/tidb/store/mockstore"
	"golang.org/x/net/context"
)

const namespace = "namespace"

func TestkvStore(t *testing.T) {
	s, err := mockstore.NewMockTikvStore()
	if err != nil {
		t.Fatalf("create tikv store failed, %s\n", err)
	}
	cli := &tiClient{
		store: s,
	}

	store := &kvStore{
		cli: cli,
	}

	if err := store.DropSession(context.Background(), namespace, "clientid"); err != nil {
		t.Fatalf("drop session failed, %s\n", err)
	}

	if err := store.Unsubscribe(context.Background(), namespace, "clientid", []string{"test"}, false); err != nil {
		t.Fatalf("unsubscribe failed, %s\n", err)
	}

	if err := store.RemoveRoute(context.Background(), namespace, "topic", "0.0.0.0:2345", 1000); err != nil {
		t.Fatalf("remove route failed, %s\n", err)
	}

	if err := store.PutUnackMessage(context.Background(), namespace, "xx", false, []*pbMessage.Message{&pbMessage.Message{Topic: "test", Index: []byte("1"), MessageID: 1, BizID: []byte("hehe")}}); err != nil {
		t.Fatalf("insert unack message failed, %s\n", err)
	}

	if _, err := store.DelUnackMessage(context.Background(), namespace, "xx", 1); err != nil {
		t.Fatalf("delete unack message failed, %s\n", err)
	}

	if _, err := store.DelUnackMessage(context.Background(), namespace, "xx", 1); err != session.ErrEmpty {
		t.Fatalf("delete unack message failed, %s\n", err)
	}
}
