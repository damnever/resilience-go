package metric

import (
	"sync"
	"time"

	"github.com/beorn7/perks/quantile"
)

// The following comments and code are copied and modified from:
//  https://github.com/prometheus/client_golang/blob/b8fcd28885584356c562e9ecad42cbec45f4ce96/prometheus/summary.go#L144
//
// Problem with the sliding-window decay algorithm... The Merge method of
// perk/quantile is actually not working as advertised - and it might be
// unfixable, as the underlying algorithm is apparently not capable of merging
// summaries in the first place. To avoid using Merge, we are currently adding
// observations to _each_ age bucket, i.e. the effort to add a sample is
// essentially multiplied by the number of age buckets. When rotating age
// buckets, we empty the previous head stream. On scrape time, we simply take
// the quantiles from the head stream (no merging required). Result: More effort
// on observation time, less effort on scrape time, which is exactly the
// opposite of what we try to accomplish, but at least the results are correct.

// NewSummary creates a new Summary.
func NewSummary(quantile float64, window time.Duration, windowBuckets int) *Summary {
	const bufCap = 512
	s := &Summary{
		objectives: map[float64]float64{quantile: 0.001},

		hotBuf:         make([]float64, 0, bufCap),
		coldBuf:        make([]float64, 0, bufCap),
		streamDuration: window / time.Duration(windowBuckets),
	}
	s.headStreamExpTime = time.Now().Add(s.streamDuration)
	s.hotBufExpTime = s.headStreamExpTime

	for i := 0; i < windowBuckets; i++ {
		s.streams = append(s.streams, s.newStream())
	}
	s.headStream = s.streams[0]
	return s
}

// A Summary captures individual observations from an event or sample stream and
// summarizes them in a manner similar to traditional summary statistics: 1. sum
// of observations, 2. observation count, 3. rank estimations.
type Summary struct {
	bufMtx sync.Mutex // Protects hotBuf and hotBufExpTime.
	mtx    sync.Mutex // Protects every other moving part.
	// Lock bufMtx before mtx if both are needed.

	quantile   float64
	objectives map[float64]float64

	sum float64
	cnt uint64

	hotBuf, coldBuf []float64

	streams                          []*quantile.Stream
	streamDuration                   time.Duration
	headStream                       *quantile.Stream
	headStreamIdx                    int
	headStreamExpTime, hotBufExpTime time.Time
}

func (s *Summary) Observe(d time.Duration) {
	s.bufMtx.Lock()
	defer s.bufMtx.Unlock()

	now := time.Now()
	if now.After(s.hotBufExpTime) {
		s.asyncFlush(now)
	}
	s.hotBuf = append(s.hotBuf, float64(d.Nanoseconds()))
	if len(s.hotBuf) == cap(s.hotBuf) {
		s.asyncFlush(now)
	}
}

func (s *Summary) Value() time.Duration {
	s.bufMtx.Lock()
	s.mtx.Lock()
	// Swap bufs even if hotBuf is empty to set new hotBufExpTime.
	s.swapBufs(time.Now())
	s.bufMtx.Unlock()

	s.flushColdBuf()

	v := float64(-1)
	if s.headStream.Count() > 0 {
		v = s.headStream.Query(s.quantile)
	}
	s.mtx.Unlock()

	return time.Duration(v) * time.Nanosecond
}

func (s *Summary) newStream() *quantile.Stream {
	return quantile.NewTargeted(s.objectives)
}

// asyncFlush needs bufMtx locked.
func (s *Summary) asyncFlush(now time.Time) {
	s.mtx.Lock()
	s.swapBufs(now)

	// Unblock the original goroutine that was responsible for the mutation
	// that triggered the compaction.  But hold onto the global non-buffer
	// state mutex until the operation finishes.
	go func() {
		s.flushColdBuf()
		s.mtx.Unlock()
	}()
}

// rotateStreams needs mtx AND bufMtx locked.
func (s *Summary) maybeRotateStreams() {
	for !s.hotBufExpTime.Equal(s.headStreamExpTime) {
		s.headStream.Reset()
		s.headStreamIdx++
		if s.headStreamIdx >= len(s.streams) {
			s.headStreamIdx = 0
		}
		s.headStream = s.streams[s.headStreamIdx]
		s.headStreamExpTime = s.headStreamExpTime.Add(s.streamDuration)
	}
}

// flushColdBuf needs mtx locked.
func (s *Summary) flushColdBuf() {
	for _, v := range s.coldBuf {
		for _, stream := range s.streams {
			stream.Insert(v)
		}
		s.cnt++
		s.sum += v
	}
	s.coldBuf = s.coldBuf[0:0]
	s.maybeRotateStreams()
}

// swapBufs needs mtx AND bufMtx locked, coldBuf must be empty.
func (s *Summary) swapBufs(now time.Time) {
	if len(s.coldBuf) != 0 {
		panic("coldBuf is not empty")
	}
	s.hotBuf, s.coldBuf = s.coldBuf, s.hotBuf
	// hotBuf is now empty and gets new expiration set.
	for now.After(s.hotBufExpTime) {
		s.hotBufExpTime = s.hotBufExpTime.Add(s.streamDuration)
	}
}
