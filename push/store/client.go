package store

import (
	"strings"

	"github.com/meitu/bifrost/push/conf"
	"github.com/meitu/bifrost/push/store/mock"
	rds_util "github.com/meitu/bifrost/push/store/redis_util"
	util "github.com/meitu/bifrost/push/store/tikv_util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
)

// TIKV
type tiClient struct {
	store kv.Storage
	gc    *util.GC
}

func newTiClient(tconf *conf.Tikv) (*tiClient, error) {
	var store kv.Storage
	var err error
	if strings.Contains(tconf.Addrs, mock.MockAddr) {
		store, err = mock.MockOpen(tconf.Addrs)
	} else {
		driver := tikv.Driver{}
		store, err = driver.Open(tconf.Addrs)
	}
	if err != nil {
		return nil, err
	}
	gc := util.NewGC(store, &tconf.GC, &tconf.TikvGC)
	return &tiClient{
		store: store,
		gc:    gc,
	}, nil
}

func (c *tiClient) Start() (Transaction, error) {
	txn, err := c.store.Begin()
	if err != nil {
		return nil, err
	}
	return newTiTxn(txn), nil
}

func (c *tiClient) Close() error {
	return c.store.Close()
}

// Redis
type rdsClient struct {
	redic rds_util.Redic
	queue rds_util.Queue
}

func newRdsClient(rconf *conf.RedisStore) (*rdsClient, error) {
	rconf.Queue.Redis = &rconf.Redis
	queue, err := rds_util.NewQueue(&rconf.Queue)
	if err != nil {
		return nil, err
	}

	return &rdsClient{
		redic: rds_util.New(&rconf.Redis),
		queue: queue,
	}, nil
}

func (c *rdsClient) Start() (Transaction, error) {
	return newRdsTxn(c.redic, c.queue), nil
}

func (c *rdsClient) Close() error {
	// return c.store.Close()
	return nil
}
