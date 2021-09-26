package godisruptor

import (
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"golang.org/x/sys/cpu"
)

// TODO add support for multiple writers
// TODO add benchmarks and compare with mutex and channels

const defaultCursorValue = -1

type Disruptor struct {
	prodStr ProducerConfiguration
	consStr ConsumerConfiguration
	size    int64
	cursor  *Cursor
	_       cpu.CacheLinePad
	seq     int64
	_       cpu.CacheLinePad
	stopped *Cursor
	cons    []*Consumer
	d       int64 // denominator for bit module operation
}
type ConsumerConfiguration int
type ProducerConfiguration int

const (
	CONSUMER_UNICAST   ConsumerConfiguration = 1
	CONSUMER_PIPELINE  ConsumerConfiguration = 2
	CONSUMER_MULTICAST ConsumerConfiguration = 3

	PRODUCER_SINGLE ProducerConfiguration = 1
	PRODUCER_MULTI  ProducerConfiguration = 2
)

func NewDisruptor(p2 int, producerConf ProducerConfiguration,
	consumerConf ConsumerConfiguration,
	fns ...func(low, up int64)) (*Disruptor, error) {
	var size int64 = 1 << p2

	ring := &Disruptor{
		prodStr: producerConf,
		consStr: consumerConf,
		size:    size,
		cursor:  &Cursor{val: defaultCursorValue},
		seq:     defaultCursorValue,
		stopped: &Cursor{},
		cons:    nil,
		d:       size - 1,
	}
	barrier := NewSeqBarrier(ring.cursor)

	var cons []*Consumer
	cons = append(cons, NewConsumer("C0", barrier, fns[0]))

	switch ring.consStr {
	case CONSUMER_UNICAST:
		if len(fns) != 1 {
			return nil, errors.New("only one consumer expected")
		}
	case CONSUMER_PIPELINE:
		if len(fns) > 1 {
			for i := 1; i < len(fns); i++ {
				barrier := NewSeqBarrier(cons[i-1].Last)
				con := NewConsumer(fmt.Sprintf("C%d", i), barrier, fns[i])
				cons = append(cons, con)
			}
		}
	case CONSUMER_MULTICAST:
		if len(fns) > 1 {
			for i := 1; i < len(fns); i++ {
				con := NewConsumer(fmt.Sprintf("C%d", i), barrier, fns[i])
				cons = append(cons, con)
			}
		}
	default:
		return nil, errors.New("unhandled Consumer Configuration")
	}

	ring.cons = cons

	return ring, nil
}

// single writer expected
func (r *Disruptor) Next() int64 {
	spin := 0
	spinMask := 1024*16 - 1

	// for step pipeline consumers take last one
	consNum := len(r.cons)
	minCons := r.cons[consNum-1].Last

	// TODO add logic for diamond consumers
	for {
		minIndex := minCons.Load()
		if r.consStr == CONSUMER_MULTICAST {
			// get min Index
			for i := 0; i < consNum-1; i++ {
				val := r.cons[i].Last.Load()
				if val < minIndex {
					minIndex = val
				}
			}
		}

		if r.seq-minIndex < r.d {
			break
		}
		spin++
		if spin&spinMask == 0 {
			if r.stopped.Load() == STATE_STOPPED {
				return -1
			}
			runtime.Gosched()
		}
	}
	r.seq++
	return r.seq
}

// TODO implement for multi writer
func (r *Disruptor) NextMulti() int64 {
	spin := 0
	spinMask := 1024*16 - 1
	// for step pipeline consumers take last one
	consNum := len(r.cons)
	minCons := r.cons[consNum-1].Last

	// TODO add logic for diamond consumers
	var seq int64
	for {
		for {
			if r.stopped.Load() == STATE_STOPPED {
				return -1
			}

			minIndex := minCons.Load()
			if r.consStr == CONSUMER_MULTICAST {
				// get min Index
				for i := 0; i < consNum-1; i++ {
					val := r.cons[i].Last.Load()
					if val < minIndex {
						minIndex = val
					}
				}
			}
			seq = atomic.LoadInt64(&r.seq)
			if seq-minIndex < r.d {
				break
			}
			spin++
			if spin&spinMask == 0 {
				runtime.Gosched()
			}
		}
		if atomic.CompareAndSwapInt64(&r.seq, seq, seq+1) {
			break
		}
	}
	return seq + 1
}

func (r *Disruptor) Publish(i int64) {
	r.cursor.Store(i)
}

func (r *Disruptor) PublishMulti(i int64) {
	spin := 0
	spinMask := 1024*16 - 1

	expected := i - 1
	for r.cursor.Load() != expected {
		spin++
		if spin&spinMask == 0 {
			runtime.Gosched()
		}
	}
	r.cursor.Store(i)
}

func (r *Disruptor) Start() {
	for i := range r.cons {
		go func(i int) {
			r.cons[i].Start()
		}(i)
	}
}

func (r *Disruptor) Stop() {
	r.cursor.Store(STATE_STOPPED)
	for i := range r.cons {
		r.cons[i].seq.Stop()
	}
}

type SeqBarrier struct {
	cursor  *Cursor
	stopped *Cursor
}

const STATE_STOPPED = 1

func NewSeqBarrier(seq *Cursor) *SeqBarrier {
	return &SeqBarrier{cursor: seq, stopped: &Cursor{val: 0}}
}

func (b *SeqBarrier) Stop() {
	b.stopped.Store(STATE_STOPPED)
}

// TODO configure wait strategy
func (b *SeqBarrier) WaitFor(nextseq int64) int64 {
	var nextCur int64
	for {
		nextCur = b.cursor.Load()
		if nextCur < nextseq {
			if b.stopped.Load() == STATE_STOPPED { // TODO use named const
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
	name   string
	seq    *SeqBarrier
	Last   *Cursor
	_      cpu.CacheLinePad
	cursor int64
	_      cpu.CacheLinePad
	fn     func(lower, upper int64)
}

func NewConsumer(name string, bar *SeqBarrier, fn func(lower, upper int64)) *Consumer {
	return &Consumer{
		name:   name,
		seq:    bar,
		Last:   &Cursor{val: -1},
		cursor: -1, fn: fn}
}

func (con *Consumer) Start() {
	for {
		con.cursor = con.seq.WaitFor(con.cursor + 1)
		if con.cursor == -1 {
			// TODO remove
			fmt.Println(con.name, "return on", con.cursor)
			return
		}
		con.fn(con.Last.Load()+1, con.cursor)
		// advance
		con.Last.Store(con.cursor)
	}
}

type Cursor struct {
	_   cpu.CacheLinePad
	val int64
	_   cpu.CacheLinePad
}

func (c *Cursor) Load() int64 {
	return atomic.LoadInt64(&c.val)
}

func (c *Cursor) Store(n int64) {
	atomic.StoreInt64(&c.val, n)
}
