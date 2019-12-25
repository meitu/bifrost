package util

import (
	"encoding/json"

	"github.com/pingcap/tidb/kv"
)

type KvMeta struct {
	obj   *Object
	Value map[string][]byte
}

type HashMap interface {
	Get(key string) ([]byte, error)
	Delete(key string) error
	Destroy() error
	Exists() bool
	GetAll() (map[string][]byte, error)
	Set(key string, val []byte) error
}

type infoHash struct {
	obj     *Object
	txn     kv.Transaction
	metaKey []byte
	ns      []byte
}

func (h *infoHash) Get(key string) ([]byte, error) {
	if !h.Exists() {
		return nil, ErrNotFound
	}
	datakey := h.getDataKey(EscapeKey(key))
	val, err := h.txn.Get(datakey)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (h *infoHash) Set(key string, val []byte) error {
	if !h.Exists() {
		if err := h.setMeta(); err != nil {
			return err
		}
	}

	datakey := h.getDataKey(EscapeKey(key))
	if err := h.txn.Set(datakey, val); err != nil {
		return err
	}
	return nil
}

func (h *infoHash) Delete(key string) error {
	if !h.Exists() {
		return nil
	}
	datakey := h.getDataKey(EscapeKey(key))
	err := h.txn.Delete(datakey)
	if err != nil {
		return err
	}
	prefix := h.getDataKey(nil)
	iter, err := h.txn.Iter(prefix, kv.Key(prefix).PrefixNext())
	if err != nil {
		return err
	}

	for iter.Valid() && iter.Key().HasPrefix(prefix) {
		return nil
	}

	err = h.deleteMeta()
	if err != nil {
		return err
	}

	return nil
}

func (h *infoHash) Exists() bool {
	return h.obj != nil
}

func (h *infoHash) Destroy() error {
	if !h.Exists() {
		return nil
	}
	err := h.deleteMeta()
	if err != nil {
		return err
	}

	// add gc
	if err = gc(h.txn, h.getDataKey(nil)); err != nil {
		return err
	}
	h.obj = nil
	return nil
}

func (h *infoHash) GetAll() (map[string][]byte, error) {
	vals := make(map[string][]byte)
	if !h.Exists() {
		return nil, ErrNotFound
	}
	dataKeyPre := h.getDataKey(nil)
	iter, err := h.txn.Iter(dataKeyPre, kv.Key(dataKeyPre).PrefixNext())

	if err != nil {
		return nil, err
	}
	for iter.Valid() && iter.Key().HasPrefix(dataKeyPre) {
		key, err := Unescape(iter.Key()[len(dataKeyPre):])
		if err != nil {
			return nil, err
		}
		vals[key] = iter.Value()
		if err := iter.Next(); err != nil {
			break
		}
	}
	return vals, nil
}

func (h *infoHash) getDataKey(name []byte) []byte {
	key := h.obj.ID
	key = append(key, defaultDelimiter)
	key = append(key, name...)
	return DataKey(h.ns, key)

}

func (h *infoHash) getMeta() (*Object, error) {
	data, err := h.txn.Get(h.metaKey)
	if err != nil {
		return nil, err
	}

	obj, err := DecodeObject(data)
	if err != nil {
		return nil, err
	}

	return obj, nil
}

func (h *infoHash) setMeta() error {
	obj := h.obj
	if !h.Exists() {
		obj = NewObject(ObjectHashMap)
	}
	if err := h.txn.Set(h.metaKey, EncodeObject(obj)); err != nil {
		return err
	}
	h.obj = obj
	return nil
}

func (h *infoHash) deleteMeta() error {
	return h.txn.Delete(h.metaKey)
}

type kvHash struct {
	meta    *KvMeta
	txn     kv.Transaction
	metaKey []byte
}

func (k *kvHash) Get(key string) ([]byte, error) {
	if !k.Exists() {
		return nil, ErrNotFound
	}
	if val, ok := k.meta.Value[key]; ok {
		return []byte(val), nil
	}
	return nil, ErrNotFound
}

func (k *kvHash) Delete(key string) error {
	var err error
	if !k.Exists() {
		return nil
	}
	if _, ok := k.meta.Value[key]; ok {
		delete(k.meta.Value, key)
		if len(k.meta.Value) == 0 {
			err = k.deleteMeta()
		} else {
			err = k.setMeta()
		}
	}
	return err
}

//Destroy destory all key/val, recommend to use infoHash
func (k *kvHash) Destroy() error {
	if !k.Exists() {
		return nil
	}
	return k.deleteMeta()
}

//Exists check hash is exists, recommend to use infoHash
func (k *kvHash) Exists() bool {
	if len(k.meta.Value) != 0 {
		return true
	}
	return false
}

// return iterator and prefix key
func (k *kvHash) GetAll() (map[string][]byte, error) {
	vals := make(map[string][]byte)
	if !k.Exists() {
		return nil, ErrNotFound
	}
	vals = k.meta.Value
	return vals, nil
}

func (k *kvHash) Set(key string, val []byte) error {
	k.meta.Value[key] = val
	return k.setMeta()
}

func (k *kvHash) getMeta() (*KvMeta, error) {
	data, err := k.txn.Get(k.metaKey)
	if err != nil {
		return nil, err
	}

	meta := &KvMeta{Value: make(map[string][]byte)}
	meta.obj, err = DecodeObject(data)
	if err != nil {
		return nil, err
	}
	if len(data) > ObjectEncodeLen {
		if err := json.Unmarshal(data[ObjectEncodeLen:], &meta.Value); err != nil {
			return nil, err
		}
	}

	return meta, nil

}

func (k *kvHash) setMeta() error {
	data := EncodeObject(k.meta.obj)
	val, err := json.Marshal(k.meta.Value)
	if err != nil {
		return err
	}
	data = append(data, val...)
	return k.txn.Set(k.metaKey, data)
}

func (k *kvHash) deleteMeta() error {
	return k.txn.Delete(k.metaKey)
}

func NewHashMap(txn kv.Transaction, ns, name, usage string, useMeta bool) (HashMap, error) {
	if name == "" || usage == "" {
		return nil, ErrInvalid
	}

	var (
		h   HashMap
		err error
	)
	bNs := EscapeKey(ns)
	bName := EscapeKey(name)
	bUsage := EscapeKey(usage)

	if useMeta {
		h, err = newInfoHash(txn, bNs, bName, bUsage)
	} else {
		h, err = newKvHash(txn, bNs, bName, bUsage)
	}

	return h, err
}

func newInfoHash(txn kv.Transaction, ns, name, usage []byte) (HashMap, error) {
	var err error
	h := &infoHash{
		txn:     txn,
		ns:      ns,
		metaKey: getMetaKey(ns, name, usage),
	}

	h.obj, err = h.getMeta()
	if err != nil {
		if kv.IsErrNotFound(err) {
			h.obj = nil
			return h, nil
		} else {
			return nil, err
		}
	}

	return h, nil

}

func newKvHash(txn kv.Transaction, ns, name, usage []byte) (HashMap, error) {
	var err error

	k := &kvHash{
		txn:     txn,
		metaKey: getMetaKey(ns, name, usage),
	}
	k.meta, err = k.getMeta()
	if err != nil {
		if kv.IsErrNotFound(err) {
			k.meta = &KvMeta{
				obj:   NewObject(ObjectHashMap),
				Value: make(map[string][]byte),
			}

			return k, nil
		} else {
			return nil, err
		}

	}

	return k, nil
}

func getMetaKey(ns, name, usage []byte) []byte {
	key := name
	key = append(key, defaultDelimiter)
	key = append(key, []byte(usage)...)
	return MetaKey(ns, key)
}
