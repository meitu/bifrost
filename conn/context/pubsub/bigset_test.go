package pubsub

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var testhandler = func(c interface{}) error { return nil }
var dis = 1000

type Connd struct {
	v int
}

func TestBigSet(t *testing.T) {
	bs := NewBigSet(1)
	c := &Connd{}
	c1 := &Connd{}
	bs.Put(c)
	bs.Put(c1)
	for i := 0; i < 10; i++ {
		c := &Connd{}
		bs.Put(c)
	}

	assert.Equal(t, 12, bs.TotalSize())
	assert.Equal(t, 12, bs.ValidSize())
	bs.Delete(c)

	time.Sleep(time.Millisecond * 10)
	assert.Equal(t, 11, bs.ValidSize())
	assert.Equal(t, 11, bs.TotalSize())

	bs.Delete(c1)

	time.Sleep(time.Millisecond * 10)
	assert.Equal(t, bs.ValidSize(), 10)
	assert.Equal(t, bs.TotalSize(), 10)

	bs.Destory()
}

func BenchmarkBigSetAdd(b *testing.B) {
	b.StopTimer()
	bs := NewBigSet(dis)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c := &Connd{}
		bs.Put(c)
	}
	b.StopTimer()
}

func BenchmarkBigSetDelete(b *testing.B) {
	b.StopTimer()
	c := make([]*Connd, b.N)
	// its := make([]*Item, b.N)
	for i := 0; i < b.N; i++ {
		c[i] = &Connd{}
	}
	bs := NewBigSet(dis)
	for i := 0; i < b.N; i++ {
		bs.Put(c[i])
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		bs.Delete(c[i])
	}
}

func BenchmarkBigSetScan(b *testing.B) {
	b.StopTimer()
	clientNum := 10000
	bs := NewBigSet(dis)
	for i := 0; i < clientNum; i++ {
		c := &Connd{}
		bs.Put(c)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		bs.Scan(testhandler)
	}
}

func BenchmarkBigSetAddScan(b *testing.B) {
	b.StopTimer()
	clientNum := 10000
	c := make([]*Connd, clientNum)
	for i := 0; i < clientNum; i++ {
		c[i] = &Connd{}
	}
	bs := NewBigSet(100)

	for j := 0; j < 10; j++ {
		go func() {
			for i := 0; i < 100; i++ {
				bs.Put(c[i])
				time.Sleep(time.Millisecond)
			}
		}()
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		bs.Scan(testhandler)
	}
}

func BenchmarkBigSetDeleteScan(b *testing.B) {
	b.StopTimer()
	bs := NewBigSet(dis)
	// its := make([]*Item, 20000)
	c := make([]*Connd, 100000)
	for i := 0; i < 100000; i++ {
		c[i] = &Connd{}
		bs.Put(c[i])
	}

	for j := 0; j < 10; j++ {
		go func(j int) {
			for i := j; i < 100; i = i + j {
				bs.Delete(c[i])
				time.Sleep(time.Millisecond)
			}
		}(j)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		bs.Scan(testhandler)
	}
	b.StopTimer()
}
