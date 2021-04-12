package godisruptor

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRingBufferSingleWriterSingleConsumer(t *testing.T) {
	bufSizePower := 14
	var d int64 = 1<<bufSizePower - 1
	buffer := make([]int, 1<<bufSizePower)

	var sumExpected, sum, inserts, reads [8]uint64
	fn := func(lower, upper int64) {
		for i := lower; i <= upper; i++ {
			reads[0]++
			sum[0] += uint64(buffer[i&d])
		}
	}

	ring := NewRingBuffer(bufSizePower, fn)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1e6; i++ {
			id := ring.Next()
			buffer[id&d] = i
			ring.Publish(id)

			inserts[0]++
			sumExpected[0] += uint64(i)
		}
		time.Sleep(10 * time.Millisecond)
		ring.Stop()
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		ring.cons[0].Run(ring)
	}()

	wg.Wait()

	assert.Equal(t, sumExpected[0], sum[0])
	assert.Equal(t, inserts[0], reads[0])
}

func TestRingBufferWriteReadOpsPerSecond(t *testing.T) {
	nwriters := 1
	nreaders := 1

	bufSizePower := 16 // 65536
	var d int64 = 1<<bufSizePower - 1

	fmt.Println("GOMAXPROCS: ", runtime.GOMAXPROCS(-1))
	fmt.Printf("NumOfWriters %d NumOfReaders %d\n", nwriters, nreaders)

	t.Run("OneWriterOneConsumer", func(t *testing.T) {
		var reads [8]uint64
		arr := make([]int, 1<<bufSizePower)

		fn := func(lower, upper int64) {
			for i := lower; i <= upper; i++ {
				reads[0]++
			}
		}

		ring := NewRingBuffer(bufSizePower, fn)

		var wg sync.WaitGroup
		iters := int(10e6)
		start := time.Now()
		for i := 0; i < nwriters; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < iters; i++ {
					id := ring.Next()
					arr[id&d] = i
					ring.Publish(id)
				}
				ring.Stop()
			}()
		}

		for i := 0; i < nreaders; i++ {
			go func() {
				ring.cons[0].Run(ring)
			}()
		}
		wg.Wait()
		dur := time.Since(start)
		fmt.Printf("average %.f inserts/s\n", float64(iters)/dur.Seconds())
		fmt.Printf("average %.f read/s\n", float64(iters)/dur.Seconds())
	})
	t.Run("OneWriterOneConsumerMutex", func(t *testing.T) {
		var reads [8]uint64
		arr := make([]int, 1<<bufSizePower)
		var mu sync.Mutex
		var wg sync.WaitGroup
		iters := int(10e6)
		var cursor int64 = -1
		quit := make(chan bool)
		start := time.Now()
		for i := 0; i < nwriters; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var i int64 = 0
				for ; i < int64(iters); i++ {
					spin := 0
					spinMask := 1024*16 - 1

					for {
						mu.Lock()
						if i-cursor < d {
							mu.Unlock()
							break
						}
						mu.Unlock()
						spin++
						if spin&spinMask == 0 {
							runtime.Gosched()
						}
					}
					mu.Lock()
					arr[i&d] = int(i)
					cursor++
					mu.Unlock()
				}
				close(quit)
			}()
		}

		for i := 0; i < nreaders; i++ {
			go func() {
				var lower int64 = -1
				for {
					select {
					case <-quit:
						break
					default:
					}
					mu.Lock()
					max := cursor
					mu.Unlock()
					if max > lower {
						for i := lower; i <= max; i++ {
							reads[0]++
						}
						mu.Lock()
						lower = max
						mu.Unlock()
					} else {
						time.Sleep(50 * time.Microsecond)
					}
				}
			}()
		}
		wg.Wait()
		dur := time.Since(start)
		fmt.Printf("average %.f inserts/s\n", float64(iters)/dur.Seconds())
		fmt.Printf("average %.f read/s\n", float64(iters)/dur.Seconds())
	})
}

func BenchmarkRingBufferSingleWriterSingleConsumer(b *testing.B) {
	var (
		sum            int = 0
		RingBufferSize     = 16
		RingBufferMask     = RingBufferSize - 1
	)

	arr := make([]int, 1<<RingBufferSize)
	fn := func(lower, upper int64) {
		for i := lower; i <= upper; i++ {
			sum += arr[i&int64(RingBufferMask)]
		}
	}

	ring := NewRingBuffer(RingBufferSize, fn)

	go func() {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			id := ring.Next()
			arr[id&int64(RingBufferMask)] = i
			ring.Publish(id)

		}
		ring.Stop()
	}()
	ring.cons[0].Run(ring)
}
