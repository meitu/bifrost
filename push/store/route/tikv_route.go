package route

import (
	"encoding/binary"

	util "github.com/meitu/bifrost/push/store/tikv_util"
	"github.com/pingcap/tidb/kv"
)

type tiRoute struct {
	txn kv.Transaction
}

func NewTiRoute(txn kv.Transaction) *tiRoute {
	return &tiRoute{
		txn: txn,
	}
}

// Exist routing information for the specified topic
func (r *tiRoute) Exist(ns, topic string) (bool, error) {
	route, err := r.getRouteMap(ns, topic)
	if err != nil {
		return false, err
	}
	return route.Exists(), nil
}

// Add a routing information for the specified topic
func (r *tiRoute) Add(ns, topic, address string, version uint64) error {
	route, err := r.getRouteMap(ns, topic)
	if err != nil {
		return err
	}
	return route.Set(address, Uint64ToBytes(version))
}

// Remove a routing information for the specified topic
// version  ensure operation sequence , the version less than the current record version not be deleted
func (r *tiRoute) Remove(ns, topic, address string, version uint64) error {
	route, err := r.getRouteMap(ns, topic)
	if err != nil {
		return err
	}
	value, err := route.Get(address)
	if err != nil {
		if util.IsErrNotFound(err) {
			return nil
		}
		return err
	}
	if len(value) > 0 {
		preVersion := BytesToUint64(value)
		if preVersion < version {
			return route.Delete(address)
		}
	}
	return ErrUnexpectedDelete
}

// Lookup all routing information for the specified topic
func (r *tiRoute) Lookup(ns, topic string) ([]string, error) {
	route, err := r.getRouteMap(ns, topic)
	if err != nil {
		return nil, err
	}

	addressArray := make([]string, 0)
	vals, err := route.GetAll()
	if err != nil {
		if util.IsErrNotFound(err) {
			return addressArray, nil
		}
		return nil, err
	}

	for addr, _ := range vals {
		addressArray = append(addressArray, addr)
	}
	return addressArray, nil
}

func (r *tiRoute) getRouteMap(ns, topic string) (util.HashMap, error) {
	return util.NewHashMap(r.txn, ns, topic, "ROUTE", false)
}

// Uint64ToBytes the type of uint64 change the type of bytes
func Uint64ToBytes(i uint64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, i)
	return buf
}

// BytesToUint64 the type of bytes change the type of uint64
func BytesToUint64(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}
