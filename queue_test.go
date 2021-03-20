package godisruptor

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInsert(t *testing.T) {
	ring := NewRing(4)
	var vals []int64
	for i := 0; i < 10; i++ {
		vals = append(vals, int64(i))
	}
	for i := range vals {
		ring.Insert(int(vals[i]))
	}

	assert.ElementsMatch(t, vals, ring.arr[0:10])
}

func TestInsertSync(t *testing.T) {
	ring := NewRingSync(5)
	var vals []int64
	for i := 0; i < 1<<5; i++ {
		vals = append(vals, int64(i))
	}
	for i := range vals {
		ring.Insert(int(vals[i]))
	}

	assert.ElementsMatch(t, vals, ring.arr)
}

func TestRaceForArr(t *testing.T) {
	arr := make([]int, 10)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			arr[9] = 1000
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			arr[8] = 9999
		}
	}()

	wg.Wait()

	assert.Equal(t, arr[9], 1000)
	assert.Equal(t, arr[8], 9999)
}

func Test10e6Insert(t *testing.T) {
	step := 10000
	numSteps := 10
	ring := NewRing(numSteps)
	var sumExpected uint64
	var inserts uint64
	for i := 1; i <= numSteps; i++ {
		for j := (i - 1) * step; j < i*step; j++ {
			sumExpected += uint64(j)
			inserts++
		}
	}

	start := time.Now()
	var wg sync.WaitGroup
	for i := 1; i <= numSteps; i++ {
		wg.Add(1)
		go func(m int) {
			defer wg.Done()
			for j := (m - 1) * step; j < m*step; j++ {
				ring.Insert(j)
			}
		}(i)
	}

	var sumOut uint64
	var reads uint64
	for {
		v := ring.Get()
		sumOut += uint64(v)
		reads++
		if reads == uint64(numSteps*step) {
			break
		}
	}
	wg.Wait()
	fmt.Println("time took:", time.Since(start).Milliseconds(), "ms")

	assert.Equal(t, sumExpected, sumOut)
	assert.Equal(t, inserts, reads)
	assert.Equal(t, inserts, ring.indexr)
	assert.Equal(t, inserts, ring.indexw)
}

func Test10e6InsertSync(t *testing.T) {
	step := 10000
	numSteps := 10
	ring := NewRingSync(numSteps)

	var sumExpected uint64
	var inserts uint64
	start := time.Now()
	for i := 1; i <= numSteps; i++ {
		for j := (i - 1) * step; j < i*step; j++ {
			sumExpected += uint64(j)
			inserts++
		}
	}
	var wg sync.WaitGroup
	for i := 1; i <= numSteps; i++ {
		wg.Add(1)
		go func(m int) {
			defer wg.Done()
			for j := (m - 1) * step; j < m*step; j++ {
				ring.Insert(j)
				//				atomic.AddUint64(&sumExpected, uint64(j))
				//atomic.AddUint64(&inserts, 1)
			}
		}(i)
	}

	var sumOut uint64
	var reads uint64
	for {
		v := ring.Get()
		sumOut += uint64(v)
		reads++
		if reads == uint64(numSteps*step) {
			break
		}
	}
	wg.Wait()
	fmt.Println("time took:", time.Since(start).Milliseconds(), "ms")
	assert.Equal(t, sumExpected, sumOut)
	assert.Equal(t, inserts, reads)
	assert.Equal(t, inserts, ring.indexr)
	assert.Equal(t, inserts, ring.indexw)
}

func mod(v uint, d uint) uint {
	return v & (d - 1)
}

func TestModBit(t *testing.T) {
	for v := 100; v < 200; v++ {
		for _, d := range []uint{2, 4, 8, 16, 32, 64, 128} {
			assert.Equal(t, uint(v)%d, mod(uint(v), d))
		}
	}
}

func BenchmarkInsertGet(b *testing.B) {
	ring := NewRing(20)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.Insert(1000)
		ring.Get()
	}
}

func BenchmarkInsertGetSync(b *testing.B) {
	ring := NewRingSync(20)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.Insert(1000)
		ring.Get()
	}
}

func BenchmarkAtomicLoadCompareAndStore(b *testing.B) {
	var in int64 = 1000
	var store int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := atomic.LoadInt64(&store)
		atomic.CompareAndSwapInt64(&store, v, in)
		atomic.StoreInt64(&in, in+1)
	}
	fmt.Println("in", in, "store", store)
}

func BenchmarkMutexInc(b *testing.B) {
	var in int64 = 1000
	var store int64
	var mu sync.Mutex
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		store = in
		in++
		mu.Unlock()
	}
	fmt.Println("in", in, "store", store)
}
