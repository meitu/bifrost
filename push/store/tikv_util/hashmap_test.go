package util

import (
	"testing"

	"github.com/meitu/bifrost/push/store/mock"
	"github.com/pingcap/tidb/kv"
	"golang.org/x/net/context"
)

type initfunc func(txn kv.Transaction) (HashMap, error)

func TestInfoHash(t *testing.T) {
	store, _ := mock.MockOpen(mock.MockAddr)
	f := func(txn kv.Transaction) (HashMap, error) {
		return NewHashMap(txn, "namespace", "name", "usage", true)
	}
	action(t, store, f)
}

func TestKeyHash(t *testing.T) {
	store, _ := mock.MockOpen(mock.MockAddr)
	f := func(txn kv.Transaction) (HashMap, error) {
		return NewHashMap(txn, "namespace", "name", "usage", false)
	}
	action(t, store, f)
}

func action(t *testing.T, store kv.Storage, f initfunc) {
	txn, _ := store.Begin()
	hash, err := f(txn)
	if err != nil {
		t.Fatalf("create hashmap failed, %s\n", err)
	}

	err = hash.Set("key", []byte("val"))
	if err != nil {
		t.Fatalf("set a val failed, %s\n", err)
	}

	exists := hash.Exists()
	if !exists {
		t.Fatalf("check exists failed")
	}

	err = hash.Destroy()
	if err != nil {
		t.Fatalf("destroy failed, %s \n", err)
	}

	err = txn.Commit(context.Background())
	if err != nil {
		t.Fatalf("commit failed, %s\n", err)
	}

	txn, _ = store.Begin()
	hash, err = f(txn)
	if err != nil {
		t.Fatalf("create hashmap failed, %s\n", err)
	}
	_, err = hash.Get("key")
	if err == nil {
		t.Fatal("get a delete hash failed")
	}

	err = hash.Set("rangekey1", []byte("val"))
	if err != nil {
		t.Fatalf("set rangekey1  val failed, %s\n", err)
	}
	err = hash.Set("rangekey2", []byte("val"))
	if err != nil {
		t.Fatalf("set rangekey2 val failed, %s\n", err)
	}

	err = txn.Commit(context.Background())
	if err != nil {
		t.Fatalf("commit failed, %s\n", err)
	}

	txn, _ = store.Begin()
	hash, err = f(txn)
	if err != nil {
		t.Fatalf("create hashmap failed, %s\n", err)
	}
	vals, err := hash.GetAll()
	if err != nil {
		t.Fatalf("range failed, %s\n", err)
	}
	count := len(vals)
	if count != 2 {
		t.Fatalf("range iter failed count, %d\n", count)
	}

	err = hash.Set("key", []byte("val"))
	if err != nil {
		t.Fatalf("set key val failed, %s\n", err)
	}

	err = hash.Delete("key")
	if err != nil {
		t.Fatalf("del key failed, %s\n", err)
	}

	_, err = hash.Get("key")
	if err == nil {
		t.Fatalf("get key failed, %s\n", err)
	}

	err = txn.Commit(context.Background())
	if err != nil {
		t.Fatalf("commit failed, %s\n", err)
	}

}
