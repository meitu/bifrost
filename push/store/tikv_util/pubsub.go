package util

import (
	"github.com/meitu/bifrost/commons/incr_uuid"
	"github.com/pingcap/tidb/kv"
)

// Data the queue of data
type Data []byte

// PubsubMeta is the meta of a pubsub
type PubsubMeta struct {
	*Object
}

// DecodePubsubMeta decode the meta of a pubsub
func DecodePubsubMeta(b []byte) (*PubsubMeta, error) {
	obj, err := DecodeObject(b)
	if err != nil {
		return nil, err
	}
	return &PubsubMeta{Object: obj}, nil
}

// EncodePubsubMeta encode the meta of a pubsub
func EncodePubsubMeta(meta *PubsubMeta) []byte {
	return EncodeObject(meta.Object)
}

// NewPubsubMeta creates of the meta of a pubsub
func NewPubsubMeta() *PubsubMeta {
	obj := NewObject(ObjectPubsub)
	return &PubsubMeta{Object: obj}
}

// Pubsub is the meta of a pubsub
type Pubsub struct {
	txn           kv.Transaction
	metaKey       []byte
	dataKeyPrefix []byte
	meta          *PubsubMeta
	exists        bool
}

// NewPubsub creates a pubsub, if the pubsub has existed, return it
func NewPubsub(txn kv.Transaction, ns, name, usage string) (*Pubsub, error) {
	if len(name) == 0 || len(usage) == 0 {
		return nil, ErrInvalid
	}

	var (
		pubsub *Pubsub
		err    error
	)

	bNs := EscapeKey(ns)
	bName := EscapeKey(name)
	bUsage := EscapeKey(usage)

	pubsub = &Pubsub{
		txn:           txn,
		dataKeyPrefix: DataKey(bNs, nil),
		exists:        false,
	}

	pubsub.metaKey = pubsub.getMetaKey(bNs, bName, bUsage)
	pubsub.meta, err = pubsub.getMeta()

	if err != nil {
		if !kv.IsErrNotFound(err) {
			return nil, err
		}
		pubsub.meta = NewPubsubMeta()
		return pubsub, nil
	}
	pubsub.exists = true
	return pubsub, nil
}

// Append write a piece of data
func (pubs *Pubsub) Append(datas []Data) ([][]byte, error) {
	if !pubs.exists {
		err := pubs.setMeta(pubs.meta)
		if err != nil {
			return nil, err
		}
	}
	offs := make([][]byte, 0, len(datas))

	offset := incr_uuid.NewOffset(int64(pubs.txn.StartTS()), 0)
	for _, data := range datas {
		if err := pubs.txn.Set(pubs.getDataKey(offset.Bytes()), data); err != nil {
			return nil, err
		}
		offs = append(offs, offset.Bytes())
		offset = offset.Next()
	}
	return offs, nil
}

// ScanHandler is a handler to process scanned messages
type ScanHandler func(offset *incr_uuid.Offset, data Data) (bool, error)

// Scan seeks to the offset and calls handler for each message
// if offset is null ,scan location from the first record
func (pubs *Pubsub) Scan(offset []byte, handler ScanHandler) error {
	if !pubs.exists {
		return ErrEmpty
	}
	iter, err := pubs.txn.Iter(pubs.getDataKey(offset), kv.Key(pubs.getDataKeyPrefix()).PrefixNext())
	if err != nil {
		return err
	}

	for iter.Valid() && iter.Key().HasPrefix(pubs.getDataKeyPrefix()) {
		offset := incr_uuid.OffsetFromBytes(iter.Key()[len(pubs.getDataKeyPrefix()):])
		complete, err := handler(offset, iter.Value())
		if err != nil {
			return err
		}
		if complete {
			break
		}
		if err := iter.Next(); err != nil {
			return err
		}
	}
	return nil
}

func (pubs *Pubsub) Get(pos []byte) (Data, error) {
	// datas []Data
	return pubs.txn.Get(pubs.getDataKey(pos))
}

// DeleteIndex delete pubsub index
func (pubs *Pubsub) DeleteIndex(offset []byte) error {
	if !pubs.exists {
		return ErrEmpty
	}
	return pubs.txn.Delete(pubs.getDataKey(offset))
}

// Drop pubsub information
func (pubs *Pubsub) Drop() error {
	if !pubs.exists {
		return nil
	}

	if err := pubs.deleteMeta(); err != nil {
		return err
	}

	if err := gc(pubs.txn, pubs.getDataKeyPrefix()); err != nil {
		return err
	}
	return nil
}

func (pubs *Pubsub) getDataKey(offset []byte) []byte {
	key := pubs.getDataKeyPrefix()
	key = append(key, offset...)
	return key
}

func (pubs *Pubsub) getDataKeyPrefix() []byte {
	key := pubs.dataKeyPrefix
	key = append(key, pubs.meta.Object.ID...)
	key = append(key, defaultDelimiter)
	return key
}

func (pubs *Pubsub) getMetaKey(ns, name, usage []byte) []byte {
	key := name
	key = append(key, defaultDelimiter)
	key = append(key, []byte(usage)...)
	return MetaKey(ns, key)
}

func (pubs *Pubsub) getMeta() (*PubsubMeta, error) {
	data, err := pubs.txn.Get(pubs.metaKey)
	if err != nil {
		return nil, err
	}
	return DecodePubsubMeta(data)
}

func (pubs *Pubsub) setMeta(meta *PubsubMeta) error {
	pubs.exists = true
	return pubs.txn.Set(pubs.metaKey, EncodePubsubMeta(meta))
}

func (pubs *Pubsub) deleteMeta() error {
	pubs.exists = false
	return pubs.txn.Delete(pubs.metaKey)
}
