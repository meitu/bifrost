package util

import (
	"github.com/twinj/uuid"
)

type ObjectType byte

const (
	ObjectQueue = ObjectType(iota)
	ObjectHashMap
	ObjectPubsub

	ObjectEncodeLen   = 17
	ObjectEncodeIdLen = 16
)

type Object struct {
	ID   []byte
	Type ObjectType
}

func EncodeObject(obj *Object) []byte {
	b := make([]byte, ObjectEncodeLen, ObjectEncodeLen)
	copy(b, obj.ID[:ObjectEncodeIdLen])
	b[ObjectEncodeLen-1] = byte(obj.Type)
	return b
}

func DecodeObject(b []byte) (obj *Object, err error) {
	if len(b) < ObjectEncodeLen {
		return nil, ErrInvalid
	}
	obj = &Object{
		ID:   make([]byte, ObjectEncodeIdLen, ObjectEncodeIdLen),
		Type: ObjectType(b[ObjectEncodeLen-1]),
	}
	copy(obj.ID, b[:ObjectEncodeIdLen])
	return obj, nil
}

func NewObject(ty ObjectType) *Object {
	return &Object{
		ID:   uuid.NewV4().Bytes(),
		Type: ty,
	}

}
