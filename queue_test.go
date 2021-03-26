package godisruptor

import (
	"fmt"
	"runtime"
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
	arr := make([]int64, 10)
	var wg sync.WaitGroup
	wg.Add(2)
	start := time.Now()
	n := 1000000
	var val int64 = 0
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			val = atomic.LoadInt64(&arr[9])
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			atomic.StoreInt64(&arr[9], 9999)
		}
	}()

	wg.Wait()
	assert.Equal(t, arr[9], int64(9999))
	assert.Equal(t, val, int64(9999))
	dur := time.Since(start).Seconds()
	fmt.Printf("%f \n", float64(n)/dur)
}

func TestFalseSharing(t *testing.T) {
	arr := make([]int64, 8*2)
	var wg sync.WaitGroup

	start := time.Now()
	var n int = 10000
	for i := 0; i < 1000; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				//atomic.AddInt64(&arr[0], int64(i))
				arr[0] = int64(i)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				//atomic.AddInt64(&arr[15], int64(i))
				arr[8] = int64(i)
			}
		}()
	}
	wg.Wait()
	dur := time.Since(start).Milliseconds()
	fmt.Printf("%d \n", int64(n)/dur)
}

func Test10e6Insert(t *testing.T) {
	step := 100000
	numSteps := 2
	ring := NewRing(10)
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
				ring.Insert3(j)
			}
		}(i)
	}

	var sumOut uint64
	var reads uint64
	for {
		v := ring.Get3()
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
	step := 100000
	numSteps := 2
	ring := NewRingSync(10)

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

func TestWriteReadOpsPerSecond(t *testing.T) {
	nwriters := 1
	nreaders := 1
	var sec uint64 = 2
	bufSizePower := 14 // 65536
	fmt.Println("GOMAXPROCS: ", runtime.GOMAXPROCS(-1))
	fmt.Printf("NumOfWriters %d NumOfReaders %d\n", nwriters, nreaders)
	t.Run("NoLock", func(t *testing.T) {
		var inserts, reads [8]uint64
		ring := NewRing(bufSizePower)
		quit := make(chan bool)
		for i := 0; i < nwriters; i++ {
			go func() {
				for {
					select {
					case <-quit:
						return
					default:
					}
					ring.Insert(1000)
					atomic.AddUint64(&inserts[0], 1)
				}
			}()
		}
		//	var v int
		for i := 0; i < nreaders; i++ {
			go func() {
				for {
					select {
					case <-quit:
						return
					default:
					}
					_ = ring.Get()
					atomic.AddUint64(&reads[0], 1)
				}
			}()
		}
		time.Sleep(time.Duration(sec) * time.Second)
		close(quit)
		fmt.Println("average inserts/s", atomic.LoadUint64(&inserts[0])/sec)
		fmt.Println("average read/s", atomic.LoadUint64(&reads[0])/sec)
	})
	t.Run("Sync", func(t *testing.T) {
		var inserts, reads [8]uint64
		ring := NewRingSync(bufSizePower)
		quit := make(chan bool)
		for i := 0; i < nwriters; i++ {
			go func() {
				for {
					select {
					case <-quit:
						return
					default:
					}
					ring.Insert(1000)
					atomic.AddUint64(&inserts[0], 1)
				}
			}()
		}
		//	var v int
		for i := 0; i < nreaders; i++ {
			go func() {
				for {
					select {
					case <-quit:
						return
					default:
					}
					_ = ring.Get()
					atomic.AddUint64(&reads[0], 1)
				}
			}()
		}
		time.Sleep(time.Duration(sec) * time.Second)
		close(quit)
		fmt.Println("average inserts/s", atomic.LoadUint64(&inserts[0])/sec)
		fmt.Println("average read/s", atomic.LoadUint64(&reads[0])/sec)
	})
	t.Run("BufferedChan", func(t *testing.T) {
		var inserts, reads [8]uint64
		channel := make(chan int, 1<<bufSizePower)
		quit := make(chan bool)
		for i := 0; i < nwriters; i++ {
			go func() {
				for {
					select {
					case <-quit:
						return
					default:
					}
					channel <- 1000
					atomic.AddUint64(&inserts[0], 1)
				}
			}()
		}

		for i := 0; i < nreaders; i++ {
			go func() {
				for {
					select {
					case <-quit:
						return
					case <-channel:
						atomic.AddUint64(&reads[0], 1)
					default:
					}

				}
			}()
		}
		time.Sleep(time.Duration(sec) * time.Second)
		close(quit)
		fmt.Println("average inserts/s", atomic.LoadUint64(&inserts[0])/sec)
		fmt.Println("average read/s", atomic.LoadUint64(&reads[0])/sec)
	})
}

func mod(v int, d int) int {
	return v & (d - 1)
}

func TestModBit(t *testing.T) {
	for v := 100; v < 200; v++ {
		for _, d := range []int{2, 4, 8, 16, 32, 64, 128} {
			assert.Equal(t, v%d, mod(v, d))
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

func BenchmarkInsertGet2(b *testing.B) {
	ring := NewRing(20)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.Insert2(1000)
		ring.Get2()
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
