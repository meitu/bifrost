package incr_uuid

import (
	"encoding/binary"
	"fmt"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
)

// UUID a uuid object
type UUID struct {
	store kv.Storage
}

// NewUUID new a uuid object
// addrs pd addr
func NewUUID(addrs string) (*UUID, error) {
	driver := tikv.Driver{}
	store, err := driver.Open(addrs)
	if err != nil {
		return nil, err
	}
	return &UUID{
		store: store,
	}, nil
}

// UUID get offset object
func (uuid *UUID) UUID() (*Offset, error) {
	txn, err := uuid.store.Begin()
	if err != nil {
		return nil, err
	}
	return &Offset{TS: int64(txn.StartTS()), Index: 0}, nil
}

// Offfset uniq id
type Offset struct {
	TS    int64 // TS is the StartTS of the transaction
	Index int64
}

// NewOffset create a offset object
// ts is the startTS the transaction
func NewOffset(ts, index int64) *Offset {
	return &Offset{TS: ts, Index: index}
}

// Bytes returns offset as bytes
func (offset *Offset) Bytes() []byte {
	var out []byte
	out = append(out, Int64ToBytes(offset.TS)...)
	out = append(out, Int64ToBytes(offset.Index)...)
	return out
}

// String returns offset as human-friendly string
func (offset *Offset) String() string {
	return fmt.Sprintf("%v-%v", offset.TS, offset.Index)
}

// Next returns a greater offset
func (offset *Offset) Next() *Offset {
	o := *offset
	o.Index++
	return &o
}

// OffsetFromBytes parses offset from bytes
func OffsetFromBytes(d []byte) *Offset {
	ts := Int64FromBytes(d[:8])
	idx := Int64FromBytes(d[8:])
	return &Offset{TS: ts, Index: idx}
}

// OffsetFromString parses offset from a string
func OffsetFromString(s string) *Offset {
	offset := &Offset{}
	fmt.Sscanf(s, "%d-%d", &offset.TS, &offset.Index)
	return offset
}

// Int64ToBytes  int64 change to bytes
func Int64ToBytes(val int64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(val))
	return bytes
}

// Int64FromBytes bytes change to  int64
func Int64FromBytes(data []byte) int64 {
	return int64(binary.BigEndian.Uint64(data))
}
