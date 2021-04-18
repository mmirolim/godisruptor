package godisruptor

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDisruptorSingleWriterSingleConsumer(t *testing.T) {
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

	disruptor, err := NewDisruptor(bufSizePower, CONSUMER_UNICAST, fn)
	assert.Nil(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1e6; i++ {
			id := disruptor.Next()
			buffer[id&d] = i
			disruptor.Publish(id)

			inserts[0]++
			sumExpected[0] += uint64(i)
		}
		time.Sleep(10 * time.Millisecond)
		disruptor.Stop()
	}()

	disruptor.Start()

	wg.Wait()

	assert.Equal(t, sumExpected[0], sum[0])
	assert.Equal(t, inserts[0], reads[0])
}

func TestDisruptorSingleWriterThreeStepPipeline(t *testing.T) {
	var numIters int = 1e5
	data := make([]int, numIters)
	bufSizePower := 14
	var d int64 = 1<<bufSizePower - 1
	buffer := make([]int, 1<<bufSizePower)

	var sumExpected, resSum, inserts, reads [8]int64
	add1 := func(lower, upper int64) {
		for i := lower; i <= upper; i++ {
			buffer[i&d] = buffer[i&d] + 1
		}
	}
	mul2 := func(lower, upper int64) {
		for i := lower; i <= upper; i++ {
			buffer[i&d] = buffer[i&d] * 2
		}
	}
	sub5 := func(lower, upper int64) {
		for i := lower; i <= upper; i++ {
			reads[0]++
			buffer[i&d] = buffer[i&d] - 5
			resSum[0] += int64(buffer[i&d])
		}
	}
	// init expected data set
	for i := 0; i < numIters; i++ {
		data[i] = i
		// add1
		data[i] += 1
		// mul2
		data[i] *= 2
		// sub5
		data[i] -= 5
	}
	for i := range data {
		sumExpected[0] += int64(data[i])
	}
	disruptor, err := NewDisruptor(bufSizePower, CONSUMER_PIPELINE, add1, mul2, sub5)
	assert.Nil(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numIters; i++ {
			id := disruptor.Next()
			buffer[id&d] = i
			disruptor.Publish(id)

			inserts[0]++
		}
		time.Sleep(10 * time.Millisecond)
		disruptor.Stop()
	}()
	disruptor.Start()
	wg.Wait()

	assert.Equal(t, sumExpected[0], resSum[0])
	assert.Equal(t, inserts[0], reads[0])
}

func TestDisruptorSingleWriterMultipleUnicastConsumers(t *testing.T) {
	var numIters int = 1e5
	data := make([]int, numIters)
	bufSizePower := 14
	var d int64 = 1<<bufSizePower - 1
	buffer := make([]int, 1<<bufSizePower)

	var sum1s, sum2s, sum3s [8]int

	add1 := func(lower, upper int64) {
		for i := lower; i <= upper; i++ {
			sum1s[0] += buffer[i&d] + 1
		}
	}
	add2 := func(lower, upper int64) {
		for i := lower; i <= upper; i++ {
			sum2s[0] += buffer[i&d] + 2
		}
	}
	add3 := func(lower, upper int64) {
		for i := lower; i <= upper; i++ {
			sum3s[0] += buffer[i&d] + 3
		}
	}

	var sum1sExp, sum2sExp, sum3sExp [8]int
	// init expected data set
	for i := 0; i < numIters; i++ {
		data[i] = i
		sum1sExp[0] += data[i] + 1
		sum2sExp[0] += data[i] + 2
		sum3sExp[0] += data[i] + 3
	}

	disruptor, err := NewDisruptor(bufSizePower, CONSUMER_MULTICAST, add1, add2, add3)
	assert.Nil(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numIters; i++ {
			id := disruptor.Next()
			buffer[id&d] = i
			disruptor.Publish(id)
		}
		time.Sleep(10 * time.Millisecond)
		disruptor.Stop()
	}()
	disruptor.Start()
	wg.Wait()

	assert.Equal(t, sum1sExp[0], sum1s[0])
	assert.Equal(t, sum2sExp[0], sum2s[0])
	assert.Equal(t, sum3sExp[0], sum3s[0])
}

func TestDisruptorWriteReadOpsPerSecond(t *testing.T) {
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

		disruptor, err := NewDisruptor(bufSizePower, CONSUMER_UNICAST, fn)
		assert.Nil(t, err)

		var wg sync.WaitGroup
		iters := int(10e6)
		start := time.Now()
		for i := 0; i < nwriters; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < iters; i++ {
					id := disruptor.Next()
					arr[id&d] = i
					disruptor.Publish(id)
				}
				disruptor.Stop()
			}()
		}
		disruptor.Start()

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

func BenchmarkDisruptorSingleWriterSingleConsumer(b *testing.B) {
	var (
		sum           int = 0
		DisruptorSize     = 16
		DisruptorMask     = DisruptorSize - 1
	)

	arr := make([]int, 1<<DisruptorSize)
	fn := func(lower, upper int64) {
		for i := lower; i <= upper; i++ {
			sum += arr[i&int64(DisruptorMask)]
		}
	}

	disruptor, err := NewDisruptor(DisruptorSize, CONSUMER_UNICAST, fn)
	assert.Nil(b, err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			id := disruptor.Next()
			arr[id&int64(DisruptorMask)] = i
			disruptor.Publish(id)

		}
		disruptor.Stop()
	}()
	disruptor.Start()

	wg.Wait()
}
