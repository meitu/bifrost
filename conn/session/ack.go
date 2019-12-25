package session

import (
	"container/list"
	"errors"
	"sync"

	"github.com/meitu/bifrost/conn/context"
	ph "github.com/meitu/bifrost/grpc/push"
)

// ack base infomation
type item struct {
	bizID     []byte
	messageID int64
}

var itemPool *sync.Pool = &sync.Pool{
	New: func() interface{} { return &item{} },
}

type syncList struct {
	lock sync.RWMutex
	l    *list.List
}

// NewSyncList returns an initialized list.
func NewSyncList() *syncList {
	return &syncList{
		l: list.New(),
	}
}

// PushFront inserts a new element e with value v at the front of list l and returns e.
func (l *syncList) PushFront(it *item) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.l.PushFront(it)
}

// Back returns the last element of list l or nil if the list is empty.
func (l *syncList) Back() *item {
	l.lock.Lock()
	defer l.lock.Unlock()
	elem := l.l.Back()
	if elem == nil {
		return nil
	}
	return l.l.Remove(elem).(*item)
}

// ack complete the recording and deletion of message ack
type ack struct {
	ackList *syncList
}

// NewACK create a new object
func NewACK() *ack {
	return &ack{
		ackList: NewSyncList(),
	}
}

// If cleansession == true only records in memory. Otherwise record to storage.
func (sd *ack) PutUnack(ctx *context.SessionContext, req *ph.PutUnackReq) (*ph.PutUnackResp, error) {
	if req.CleanSession {
		for _, ackMsg := range req.Messages {
			sd.put(ackMsg.MessageID, ackMsg.BizID)
		}
	}
	return ctx.PushCli.PutUnack(ctx, req)
}

// DelUnack delete ack and notify push
func (sd *ack) DelUnack(ctx *context.SessionContext, req *ph.DelUnackReq) (*ph.DelUnackResp, error) {
	if req.CleanSession {
		bizID, err := sd.getBizID(req.MessageID)
		if err != nil {
			return nil, err
		}
		req.BizID = bizID
	}

	resp, err := ctx.PushCli.DelUnack(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.BizID == nil {
		resp.BizID = req.BizID
	}
	return resp, nil
}

func (sd *ack) getBizID(mid int64) ([]byte, error) {
	it := sd.ackList.Back()
	if it == nil {
		return nil, errors.New("list is null")
	}
	defer itemPool.Put(it)
	if it.messageID == mid {
		return it.bizID, nil
	}
	return nil, errors.New("mid not match")

}

func (sd *ack) put(mid int64, bizID []byte) {
	it := itemPool.Get().(*item)
	it.bizID = bizID
	it.messageID = mid
	sd.ackList.PushFront(it)
}
