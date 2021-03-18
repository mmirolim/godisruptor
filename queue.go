package godisruptor

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type cmd struct {
	cmd      string
	index    uint64
	val      int
	arrIndex uint64
}

type Ring struct {
	size      uint64
	indexr    uint64
	indexw    uint64
	arr       []int64
	indexChan chan cmd
}

func NewRing(size int) *Ring {
	ring := &Ring{
		uint64(size), 0, 0, make([]int64, size),
		make(chan cmd, 100),
	}
	go func() {
		for i := range ring.indexChan {
			fmt.Println("cmd", i.cmd, " index:", i.index, " arrIndex:", i.arrIndex, " val:", i.val)
		}
	}()
	return ring
}

func (r *Ring) Insert(v int) {
	val := int64(v)
	for {
		indexw := atomic.LoadUint64(&r.indexw)
		if indexw-atomic.LoadUint64(&r.indexr) > r.size-1 {
			// wait full
			time.Sleep(time.Microsecond * 10)
			continue
		}
		// free slots
		if atomic.CompareAndSwapUint64(&r.indexw, indexw, indexw+1) {
			// reserved
			atomic.StoreInt64(&r.arr[indexw%r.size], val)
			//r.indexChan <- cmd{"insert", indexw, v, indexw % r.size}
			break
		}
	}
}

func (r *Ring) Get() int {
	for {
		indexr := atomic.LoadUint64(&r.indexr)
		if atomic.LoadUint64(&r.indexw) > indexr {
			if atomic.CompareAndSwapUint64(&r.indexr, indexr, indexr+1) {
				// reserved
				v := atomic.LoadInt64(&r.arr[indexr%r.size])
				//r.indexChan <- cmd{"get", indexr, v, indexr % r.size}
				return int(v)
			}
		}
		// wait
		time.Sleep(time.Microsecond * 10)
	}
}

type RingSync struct {
	mu     sync.Mutex
	size   uint64
	indexr uint64
	indexw uint64
	arr    []int64
}

func NewRingSync(size int) *Ring {
	ring := &Ring{
		uint64(size), 0, 0, make([]int64, size),
		make(chan cmd, 100),
	}
	go func() {
		for i := range ring.indexChan {
			fmt.Println("cmd", i.cmd, " index:", i.index, " arrIndex:", i.arrIndex, " val:", i.val)
		}
	}()
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
		// reserved
		r.arr[r.indexw%r.size] = val
		r.mu.Unlock()
		//r.indexChan <- cmd{"insert", indexw, v, indexw % r.size}
		break
	}
}

func (r *RingSync) Get() int {
	for {
		r.mu.Lock()
		if r.indexw > r.indexr {
			// reserved
			v := r.arr[r.indexr%r.size]
			r.mu.Unlock()
			//r.indexChan <- cmd{"get", indexr, v, indexr % r.size}
			return int(v)
		}
		r.mu.Unlock()
	}
}
