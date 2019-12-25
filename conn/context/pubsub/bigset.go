package pubsub

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

var itemPool *sync.Pool = &sync.Pool{
	New: func() interface{} { return NewItem() },
}

// GetItem get a item
func GetItem(c interface{}) *Item {
	item := itemPool.Get().(*Item)
	item.c = unsafe.Pointer(&c)
	item.Prev = nil
	item.Next = nil
	return item
}

// PutItem put item to pool
func PutItem(item *Item) {
	itemPool.Put(item)
}

// Item list a node
type Item struct {
	c    unsafe.Pointer
	Prev *Item
	Next *Item
}

// NewItem create a new item
func NewItem() *Item {
	it := &Item{
		c:    nil,
		Prev: nil,
		Next: nil,
	}
	return it
}

// syncMap thread-safe map
type syncMap struct {
	mapItem map[interface{}]*Item
	mapLock sync.RWMutex
}

// NewSyncMap create a new map
func NewSyncMap() *syncMap {
	return &syncMap{
		mapItem: make(map[interface{}]*Item, 1),
	}
}

// addMap add an element to the map
func (m *syncMap) addMap(c interface{}, item *Item) bool {
	m.mapLock.Lock()
	defer m.mapLock.Unlock()
	if _, exist := m.mapItem[c]; !exist {
		m.mapItem[c] = item
		return true
	}
	return false
}

// delMap delete an element to the map
func (m *syncMap) delMap(c interface{}) *Item {
	m.mapLock.Lock()
	defer m.mapLock.Unlock()
	if item, exist := m.mapItem[c]; exist {
		delete(m.mapItem, c)
		return item
	}
	return nil
}

// sizeMap number of elements in a map
func (m *syncMap) sizeMap() int {
	m.mapLock.RLock()
	defer m.mapLock.RUnlock()
	return len(m.mapItem)
}

// BigSet the collection of items provides concurrent safe additions,
// deletions, and traversals
type BigSet struct {
	head     *Item
	tail     *Item // TODO 暂时未使用到
	listLock sync.RWMutex

	mapItem  *syncMap
	delLock  sync.Mutex
	delQueue []*Item

	discardThreshold int
	clearLock        sync.RWMutex
	wg               sync.WaitGroup
}

// NewBigSet new a object
func NewBigSet(discardThreshold int) *BigSet {
	bs := &BigSet{
		discardThreshold: discardThreshold,
		mapItem:          NewSyncMap(),
		delQueue:         make([]*Item, 0, discardThreshold),
	}
	return bs
}

func (bs *BigSet) delList(item *Item) {
	if item.Prev != nil {
		item.Prev.Next = item.Next
	} else {
		bs.head = item.Next
	}
	if item.Next != nil {
		item.Next.Prev = item.Prev
	} else {
		bs.tail = item.Prev
	}
}

func (bs *BigSet) addList(item *Item) {
	if bs.head == nil {
		bs.tail = item
		bs.head = item
		return
	}

	item.Prev = bs.tail
	bs.tail.Next = item
	bs.tail = item
}

// Put elements, map and list need to be added
// Does not add or return an error if it already exists
func (bs *BigSet) Put(c interface{}) {
	item := GetItem(c)
	if bs.mapItem.addMap(c, item) {
		bs.listLock.Lock()
		bs.addList(item)
		bs.listLock.Unlock()
		return
	}
	PutItem(item)
}

// Delete elements, delete in mapItem, and add delQueue
// List is deleted if delQueue reaches a certain length
func (bs *BigSet) Delete(c interface{}) {
	if it := bs.mapItem.delMap(c); it != nil {
		atomic.StorePointer(&it.c, nil)
		bs.delLock.Lock()
		bs.delQueue = append(bs.delQueue, it)
		if len(bs.delQueue) >= bs.discardThreshold {
			queue := bs.delQueue
			bs.delQueue = make([]*Item, 0, bs.discardThreshold)
			bs.wg.Add(1)
			go bs.clear(queue)
		}
		bs.delLock.Unlock()
	}
}

// Scan Iterates through the handlers callback function
func (bs *BigSet) Scan(handlers func(c interface{}) error) {
	bs.clearLock.RLock()
	defer bs.clearLock.RUnlock()

	bs.listLock.RLock()
	item := bs.head
	end := bs.tail
	bs.listLock.RUnlock()

	// 由于采用尾插法进行插入元素，所以这里只遍历到开始时候的结尾
	for item != end {
		c := (atomic.LoadPointer(&item.c))
		if c != nil {
			conn := *(*interface{})(c)
			if err := handlers(conn); err != nil {
				return
			}
		}
		item = item.Next
	}
	// 遍历最后一个元素
	if end != nil {
		c := (atomic.LoadPointer(&end.c))
		if c != nil {
			conn := *(*interface{})(c)
			if err := handlers(conn); err != nil {
				return
			}
		}
	}
}

// TotalSize Total number of current elements: the number of items that are valid
// and the number of items that are invalid for deletion
func (bs *BigSet) TotalSize() int {
	bs.delLock.Lock()
	delLen := len(bs.delQueue)
	bs.delLock.Unlock()
	return bs.mapItem.sizeMap() + delLen
}

// ValidSize valid number of current elements
func (bs *BigSet) ValidSize() int {
	return bs.mapItem.sizeMap()
}

// Destory  bigset
func (bs *BigSet) Destory() {
	bs.wg.Wait()
}

func (bs *BigSet) clear(qs []*Item) {
	defer bs.wg.Done()
	bs.clearLock.Lock()
	bs.listLock.Lock()
	for _, it := range qs {
		bs.delList(it)
	}
	bs.listLock.Unlock()
	bs.clearLock.Unlock()
}
