package mock

import (
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/stretchr/testify/assert"
)

//MockAddr default mock tikv addr
var MockAddr = "mocktikv://"

var Store kv.Storage

func init() {
	t := &testing.T{}
	Store = MockKVStore(t)
}

// MockOpen create fake tikv db
func MockOpen(addrs string) (kv.Storage, error) {
	var driver mockstore.MockDriver
	return driver.Open(MockAddr)
}

func MockKVStore(t *testing.T) kv.Storage {
	store, err := mockstore.NewMockTikvStore()
	assert.NoError(t, err)
	return store
}

func MockKVTransaction(t *testing.T) kv.Transaction {
	store := MockKVStore(t)
	txn, err := store.Begin()
	assert.NoError(t, err)
	return txn
}
