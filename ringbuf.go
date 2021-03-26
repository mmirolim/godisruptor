package godisruptor

import (
	"runtime"
	"sync/atomic"
	"time"

	"golang.org/x/sys/cpu"
)

const defaultCursorValue = -1

type RingBuffer struct {
	size     int64
	_        cpu.CacheLinePad
	cursor   int64
	_        cpu.CacheLinePad
	seq      int64
	_        cpu.CacheLinePad
	stopped  int64
	_        cpu.CacheLinePad
	consumer *Consumer
	d        int64 // denominator for bit module operation
}

func NewRingBuffer(p2 int, con *Consumer) *RingBuffer {
	var size int64 = 1 << p2
	ring := &RingBuffer{
		size:     size,
		cursor:   defaultCursorValue,
		seq:      defaultCursorValue,
		stopped:  0,
		d:        size - 1,
		consumer: con,
	}
	return ring
}

// single writer expected
func (r *RingBuffer) Next() int64 {
	spin := 0
	spinMask := 1024*16 - 1

	minCon := &r.consumer.Last
	for {
		minCons := atomic.LoadInt64(minCon)
		if r.seq-minCons < r.d {
			break
		}
		if spin&spinMask == 0 {
			if atomic.LoadInt64(&r.stopped) == 1 {
				return -1
			}
			runtime.Gosched()
		}
	}
	r.seq++
	return r.seq
}

func (r *RingBuffer) Publish(i int64) {
	atomic.StoreInt64(&r.cursor, i)
}

func (r *RingBuffer) Stop() {
	atomic.StoreInt64(&r.stopped, 1)
}

func (r *RingBuffer) WaitFor(cursor int64) int64 {
	var nextCur int64
	for {
		nextCur = atomic.LoadInt64(&r.cursor)
		if nextCur < cursor {
			if atomic.LoadInt64(&r.stopped) == 1 { // TODO use named const
				return -1
			}
			time.Sleep(50 * time.Microsecond)
			continue
		}
		break
	}
	return nextCur
}

type Consumer struct {
	_      cpu.CacheLinePad
	Last   int64
	_      cpu.CacheLinePad
	cursor int64
	_      cpu.CacheLinePad
	fn     func(lower, upper int64)
}

func NewConsumer(fn func(lower, upper int64)) *Consumer {
	return &Consumer{Last: -1, cursor: -1, fn: fn}
}

func (con *Consumer) Run(buf *RingBuffer) {
	for {
		con.cursor = buf.WaitFor(con.cursor + 1)
		if con.cursor == -1 {
			return
		}
		con.fn(con.Last+1, con.cursor)
		// advance
		atomic.StoreInt64(&con.Last, con.cursor)
	}
}
