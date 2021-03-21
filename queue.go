package godisruptor

import (
	"runtime"
	"sync"
	"sync/atomic"
)

type Ring struct {
	size   uint64
	indexr uint64 // TODO increase to cache line
	indexw uint64
	d      uint64
	arr    []int64
}

// power used as power of 2 for array size
func NewRing(power int) *Ring {
	size := 1 << power
	ring := &Ring{
		indexr: 0, indexw: 0, d: uint64(size) - 1,
		size: uint64(size),
		arr:  make([]int64, size),
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
			runtime.Gosched()
			continue
		}
		// TODO maybe to store first and then inc indexw
		// then sentinal value can be removed
		// free slots
		if atomic.CompareAndSwapUint64(&r.indexw, indexw, indexw+1) {
			// reserved
			atomic.StoreInt64(&r.arr[indexw&(r.size-1)], val)
			break
		}
	}
}

func (r *Ring) Get() int {
	for {
		indexr := atomic.LoadUint64(&r.indexr)
		if atomic.LoadUint64(&r.indexw) > indexr {
			// read
			v := atomic.LoadInt64(&r.arr[indexr&(r.size-1)])
			// check
			if v != -1 && atomic.CompareAndSwapUint64(&r.indexr, indexr, indexr+1) {
				atomic.StoreInt64(&r.arr[indexr&(r.size-1)], -1)
				return int(v)
			}
		}
		runtime.Gosched()
	}
}

func (r *Ring) Insert2(v int) {
	val := int64(v)
	for {
		indexr := atomic.LoadUint64(&r.indexr)
		if r.indexw-indexr >= r.d {
			// wait full
			continue
		}
		// free slots
		r.arr[r.indexw&r.d] = val
		atomic.AddUint64(&r.indexw, 1)
		break
	}
}

func (r *Ring) Get2() int {
	for {
		indexw := atomic.LoadUint64(&r.indexw)
		if indexw > r.indexr {
			// read
			v := r.arr[r.indexr&r.d]
			// check
			if v != -1 {
				r.arr[r.indexr&r.d] = -1
				atomic.AddUint64(&r.indexr, 1)
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

// power of 2 of size
func NewRingSync(power int) *RingSync {
	size := 1 << power
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
		r.arr[r.indexw&(r.size-1)] = val
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
			v := r.arr[r.indexr&(r.size-1)]
			r.indexr++
			r.mu.Unlock()
			return int(v)
		}
		r.mu.Unlock()
	}
}
