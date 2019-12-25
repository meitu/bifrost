package util

import (
	"encoding/binary"
	"errors"
	"net/url"

	"github.com/pingcap/tidb/kv"
)

var (
	ErrNotFound      = kv.ErrNotExist
	defaultNamespace = []byte("bifrost-default")

	ErrInvalidKey = errors.New("invalid key")
	ErrInvalid    = errors.New("invalid argument")
	ErrEmpty      = errors.New("empty")
)

const (
	defaultDelimiter = ':'
	prefixByte       = 'B'
)

func EscapeKey(key string) []byte {
	return []byte(url.QueryEscape(key))
}

func Unescape(key []byte) (string, error) {
	return url.QueryUnescape(string(key))
}

//{namesapce}:M:{name}
func MetaKey(namespace, name []byte) []byte {
	var key []byte
	if len(namespace) == 0 {
		namespace = defaultNamespace
	}
	key = append(key, prefixByte, defaultDelimiter)
	key = append(key, namespace...)
	key = append(key, defaultDelimiter, 'M', defaultDelimiter)
	key = append(key, name...)
	return key
}

//{namespace}:D:{name}
func DataKey(namespace, name []byte) []byte {
	var key []byte
	if len(namespace) == 0 {
		namespace = defaultNamespace
	}
	key = append(key, prefixByte, defaultDelimiter)
	key = append(key, namespace...)
	key = append(key, defaultDelimiter, 'D', defaultDelimiter)
	key = append(key, name...)
	return key
}

func IsErrEmpty(err error) bool {
	return err == ErrEmpty
}

func Int64ToBytes(val int64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(val))
	return bytes
}

func Int64FromBytes(data []byte) int64 {
	return int64(binary.BigEndian.Uint64(data))
}

func IsErrNotFound(err error) bool {
	return kv.IsErrNotFound(err)
}

func getInt64(txn kv.Transaction, key kv.Key) (int64, error) {
	val, err := txn.Get(key)
	if err != nil {
		return 0, err
	}

	if len(val) != 8 {
		return 0, ErrInvalidKey
	}

	return Int64FromBytes(val), nil
}

func setInt64(txn kv.Transaction, key kv.Key, val int64) error {
	return txn.Set(key, Int64ToBytes(val))
}

func incInt64(txn kv.Transaction, key kv.Key, step int64) (int64, error) {
	val, err := getInt64(txn, key)
	if err != nil {
		if kv.IsErrNotFound(err) {
			val = 0
		} else {
			return 0, err
		}
	}

	val += step
	if err := setInt64(txn, key, val); err != nil {
		return 0, err
	}

	return val, nil
}
