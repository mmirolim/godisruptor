package godisruptor

import (
	"sync"
	"sync/atomic"
)

type Ring struct {
	size   uint64
	indexr uint64
	indexw uint64
	arr    []int64
}

func NewRing(size int) *Ring {
	ring := &Ring{
		uint64(size), 0, 0, make([]int64, size),
	}
	for i := range ring.arr {
		ring.arr[i] = -1
	}
	return ring
}

func (r *Ring) Insert(v int) {
	val := int64(v)
	for {
		indexw := atomic.LoadUint64(&r.indexw)
		indexr := atomic.LoadUint64(&r.indexr)
		if indexw-indexr >= r.size-1 {
			// wait full
			continue
		}
		// free slots
		if atomic.CompareAndSwapUint64(&r.indexw, indexw, indexw+1) {
			// reserved
			atomic.StoreInt64(&r.arr[indexw%r.size], val)
			break
		}
	}
}

func (r *Ring) Get() int {
	for {
		indexr := atomic.LoadUint64(&r.indexr)
		if atomic.LoadUint64(&r.indexw) > indexr {
			// read
			v := atomic.LoadInt64(&r.arr[indexr%r.size])
			// check
			if v != -1 && atomic.CompareAndSwapUint64(&r.indexr, indexr, indexr+1) {
				atomic.StoreInt64(&r.arr[indexr%r.size], -1)
				return int(v)
			}
		}
	}
}

type RingSync struct {
	mu     sync.Mutex
	size   uint64
	indexr uint64
	indexw uint64
	arr    []int64
}

func NewRingSync(size int) *RingSync {
	ring := &RingSync{
		size: uint64(size), indexr: 0, indexw: 0, arr: make([]int64, size),
	}
	return ring
}

func (r *RingSync) Insert(v int) {
	val := int64(v)
	for {
		r.mu.Lock()
		if r.indexw-r.indexr > r.size-1 {
			r.mu.Unlock()
			// wait full
			continue
		}
		// free slots
		r.arr[r.indexw%r.size] = val
		r.indexw++
		r.mu.Unlock()
		break
	}
}

func (r *RingSync) Get() int {
	for {

		r.mu.Lock()
		if r.indexw > r.indexr {
			// reserved
			v := r.arr[r.indexr%r.size]
			r.indexr++
			r.mu.Unlock()
			return int(v)
		}
		r.mu.Unlock()
	}
}
