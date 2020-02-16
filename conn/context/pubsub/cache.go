package pubsub

import (
	"sync"

	ph "github.com/meitu/bifrost/grpc/push"
)

// Cache 使用循环队列缓存pull拉取回来的消息
// 外部在调用`getResp`或`putResp`的时候需要先使用cache.mu进行加锁
// buf按照前闭后开的方式存储消息，也就是[front, rear)
// 其中 buf[front-1] 存储用户拉取消息时的offset
type Cache struct {
	mu    sync.Mutex
	size  int
	buf   []*ph.Message
	front int
	rear  int
	noff  []byte // next offset
}

func newCache(size int) *Cache {
	return &Cache{
		size: size,
		buf:  make([]*ph.Message, size),
	}
}

// expire 判断请求的offset是否在cache缓存的区间之前
// 在缓冲区非空并且offset小于缓冲区起始位置的时候返回true，其它返回false
func (c *Cache) expire(offset []byte) bool {
	if c.buf[c.front] != nil && BytesCompare(offset, c.buf[c.front].Index, Less) {
		return true
	}
	return false
}

// getResp 在cache中查找从offset开始的数据
// 也就是说数据范围是 [offset, ...)
func (c *Cache) getResp(offset []byte) *ph.PullResp {
	// 以下几种情况表示拉取数据不在缓冲区中：
	// 1. cache为空
	// 2. offset大于缓冲区的结尾
	// 3. offset小于buf[front-1]
	if c.rear == c.front ||
		BytesCompare(offset, c.lastIndex(), Greater) ||
		BytesCompare(offset, c.startOffsetIndex(), LessOrEqual) {
		return nil
	}
	i := c.front
	// 查找第一个大于等于offset的位置
	for i != c.rear && BytesCompare(c.buf[i].Index, offset, Less) {
		i = (i + 1) % c.size
	}

	if c.rear == i {
		return nil
	}

	resp := &ph.PullResp{}
	for i != c.rear {
		resp.Messages = append(resp.Messages, c.buf[i])
		i = (i + 1) % c.size
	}
	resp.Offset = c.noff
	return resp
}

func (c *Cache) lastIndex() []byte {
	return c.buf[(c.rear-1+c.size)%c.size].Index
}

func (c *Cache) startOffsetIndex() []byte {
	return c.buf[(c.front-1+c.size)%c.size].Index
}

// putResp 将拉取回来的消息与cache中的消息进行合并
// offset 拉取请求的offset
// msgs 请求对应的messages
// 在如下情况下对消息进行合并：
// 1. 当前队列为空
// 2. 拉取消息的offset是队列的末尾元素补位生成
// 3. 拉取回来的消息与当前区间有重叠
func (c *Cache) putMessages(offset, noff []byte, msgs []*ph.Message) {
	if len(msgs) == 0 {
		return
	}
	if c.front == c.rear {
		c.updateBuf(offset, noff, msgs)
		return
	}

	// 上次结尾的offset与这次拉取起始位置重叠
	if BytesCompare(c.noff, offset, Equal) {
		c.updateBuf(offset, noff, msgs)
		return
	}

	// 对于如下情况直接跳过
	// 1. 拉取消息区间在缓冲区间之前
	//    buf     |------------|
	//    msgs |----|
	// 2. 拉取消息区间超过缓冲区的范围
	//    buf     |------------|
	//    msgs                    |----|
	// 3. msgs  在 msgs 区间内
	//    buf     |------------|
	//    msgs         |----|

	// 拉取消息与缓冲区重叠，并且msgs的内容超过buf中的内容
	// buf     |------------|
	// msgs               |----|
	li := c.lastIndex()
	if BytesCompare(msgs[len(msgs)-1].Index, li, Greater) &&
		BytesCompare(li, msgs[0].Index, GreaterOrEqual) {
		// msg 中的 0 位置 index  一定小于等于 li
		// 因此从 1 位置开始寻找大于 li的元素
		i := 1
		for BytesCompare(msgs[i].Index, li, LessOrEqual) {
			i++
		}
		if i < len(msgs) {
			c.updateBuf(offset, noff, msgs[i:])
		}
	}
}

// updateBuf 将msgs数据追加到cache的结尾
// offset 拉取请求的offset, 当队列为空的时候，将offset放到front之前
// msgs   需要追加的消息列表
func (c *Cache) updateBuf(offset, noff []byte, msgs []*ph.Message) {
	if len(msgs) == 0 {
		return
	}
	// 如果缓冲区为空，则设置buf[front-1]=offset
	if c.front == c.rear {
		c.buf[c.front] = &ph.Message{
			Index: offset,
		}
		c.front++
		c.rear++
	}
	for _, m := range msgs {
		c.buf[c.rear] = m
		c.rear = (c.rear + 1) % c.size
		if c.rear == c.front {
			c.front = (c.front + 1) % c.size
		}
	}

	c.noff = noff
}
