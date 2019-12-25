package util

import (
	"bytes"
	"testing"
	"time"

	"github.com/meitu/bifrost/push/conf"
	"github.com/meitu/bifrost/push/store/mock"
	"github.com/pingcap/tidb/kv"
	"github.com/twinj/uuid"
	"golang.org/x/net/context"
)

var (
	gckeypre = []byte("waitgckey")
	gckey    = []byte("waitgckey:key")
)

func getGC(t *testing.T, store kv.Storage) *GC {
	return &GC{
		// Addrs: "127.0.0.1:2379",
		SysGC: &conf.GC{
			BatchLimit:     256,
			Enable:         true,
			Interval:       time.Second,
			LeaderLifeTime: time.Minute,
		},
		TikvGC: &conf.TikvGC{
			Enable:            true,
			LeaderLifeTime:    30 * time.Minute,
			Interval:          20 * time.Minute,
			SafePointLifeTime: 10 * time.Minute,
			Concurrency:       2,
		},
		store: store,
	}

}

func addGcKey(t *testing.T, store kv.Storage) {
	txn, err := store.Begin()
	if err != nil {
		t.Fatal("get txn err", err)
	}
	if err = txn.Set(gckey, []byte("val")); err != nil {
		t.Fatal("set gckey err", err)
	}
	if err = gc(txn, gckeypre); err != nil {
		t.Fatal("add gc err", err)
	}
	if err = txn.Commit(context.Background()); err != nil {
		t.Fatal("commimt err", err)
	}

}
func TestGC(t *testing.T) {
	store, _ := mock.MockOpen(mock.MockAddr)
	addGcKey(t, store)
	txn, err := store.Begin()
	if err != nil {
		t.Fatal("txn err", err)
	}
	prefix := toGCKey(gckeypre)
	val, err := txn.Get(prefix)
	if err != nil {
		t.Fatal("get gc key err", err)
	}
	if err = txn.Commit(context.Background()); err != nil {
		t.Fatal("commimt err", err)
	}

	if !bytes.Equal(val, []byte{0}) {
		t.Fatal("gc val not equeal 0")
	}
}

func TestDoGC(t *testing.T) {
	store, _ := mock.MockOpen(mock.MockAddr)
	addGcKey(t, store)
	if err := doGC(store, 256); err != nil {
		t.Fatal("do gc falat", err)
	}

	txn, err := store.Begin()
	if err != nil {
		t.Fatal("txn err", err)
	}

	prefix := toGCKey(gckeypre)
	_, err = txn.Get(prefix)
	if !IsErrNotFound(err) {
		t.Fatal("get gc key err", err)
	}

	_, err = txn.Get(gckey)
	if !IsErrNotFound(err) {
		t.Fatal("gc not success", err)
	}
	if err = txn.Commit(context.Background()); err != nil {
		t.Fatal("commimt err", err)
	}
}

func cleader(t *testing.T, txn kv.Transaction, id []byte) bool {
	isLead, err := checkLeader(txn, sysGCLeader, id, 1*time.Second)
	if err != nil {
		t.Fatal("check leader err", isLead, err)
	}
	if err = txn.Commit(context.Background()); err != nil {
		t.Fatal("commimt err", err)
	}
	return isLead

}

func TestLeader(t *testing.T) {
	store, _ := mock.MockOpen(mock.MockAddr)
	txn, err := store.Begin()
	if err != nil {
		t.Fatal("txn err", err)
	}
	id := uuid.NewV4().Bytes()
	isLead := cleader(t, txn, id)
	if !isLead {
		t.Fatal("add leader key fail", isLead)
	}
	txn, err = store.Begin()
	if err != nil {
		t.Fatal("txn err", err)
	}
	otherid := uuid.NewV4().Bytes()
	otherLead := cleader(t, txn, otherid)
	if otherLead {
		t.Fatal("set other leader is success ")
	}

	time.Sleep(2 * time.Second)

	txn, err = store.Begin()
	if err != nil {
		t.Fatal("txn err", err)
	}
	otherLead = cleader(t, txn, otherid)
	if !otherLead {
		t.Fatal("set other leader fail")
	}

}

func TestStartGC(t *testing.T) {
	store, _ := mock.MockOpen(mock.MockAddr)
	kv := getGC(t, store)
	addGcKey(t, store)
	go kv.startGC()
	time.Sleep(2 * time.Second)

	txn, err := store.Begin()
	if err != nil {
		t.Fatal("txn err", err)
	}
	_, err = txn.Get(gckey)
	if !IsErrNotFound(err) {
		t.Fatal("gc not success", err)
	}
	if err = txn.Commit(context.Background()); err != nil {
		t.Fatal("commimt err", err)
	}

}
