package retry

import (
	"math/rand"
	"time"
)

const (
	defaultMinFailures = 0
	defaultMaxWait     = 2 * time.Minute
)

// Jitter should return a new wait duration optionally with some time added or
// removed to create some randomness in wait time.
type Jitter func(baseTime time.Duration) time.Duration

// NewJitter returns a new random Jitter that is up to percent longer than the
// original wait time.
func NewJitter(percent int64) Jitter {
	if percent < 0 {
		percent = 0
	}

	return func(baseTime time.Duration) time.Duration {
		if percent == 0 {
			return baseTime
		}

		// TODO: test with math.MaxInt64
		max := int64(baseTime) * percent / 100
		return baseTime + time.Duration(rand.Int63n(max))
	}
}

// Waiter records failures, and returns
// a channel to wait on before a failed operation can be retried.
type Waiter struct {
	// MinFailures before waiting starts.
	MinFailures uint
	// MinWait time for a returned channel. Returned after the first failure.
	MinWait time.Duration
	// MaxWait time for a returned channel.
	MaxWait time.Duration
	// Jitter to add to each wait time.
	Jitter   Jitter
	failures uint
}

// Creates a Waiter
// Deprecated: create a Waiter by setting fields on the struck.
func NewWaiter(minFailures int, minWait, maxWait time.Duration, jitter Jitter) *Waiter {
	if minFailures < 0 {
		minFailures = defaultMinFailures
	}

	if maxWait <= 0 {
		maxWait = defaultMaxWait
	}

	if minWait <= 0 {
		minWait = 0 * time.Nanosecond
	}

	return &Waiter{
		MinFailures: uint(minFailures),
		MinWait:     minWait,
		MaxWait:     maxWait,
		failures:    0,
		Jitter:      jitter,
	}
}

// calculates the necessary wait time before the
// next operation should be allowed.
func (w *Waiter) calculateWait() time.Duration {
	if w.failures <= w.MinFailures {
		return w.MinWait
	}

	shift := w.failures - w.MinFailures - 1
	waitTime := w.MaxWait
	if shift < 31 {
		waitTime = (1 << shift) * time.Second
	}
	if w.Jitter != nil {
		waitTime = w.Jitter(waitTime)
	}
	if waitTime > w.MaxWait {
		return w.MaxWait
	}
	if waitTime < w.MinWait {
		return w.MinWait
	}
	return waitTime
}

// calculates the waitTime and returns a chan
// that will become selectable once that amount
// of time has elapsed.
func (w *Waiter) wait() <-chan struct{} {
	waitTime := w.calculateWait()
	ch := make(chan struct{})
	if waitTime > 0 {
		time.AfterFunc(waitTime, func() { close(ch) })
	} else {
		// if there should be 0 wait time then we ensure
		// that the chan will be immediately selectable
		close(ch)
	}
	return ch
}

// Marks that an operation is successful which resets the failure count.
// The chan that is returned will be immediately selectable.
func (w *Waiter) Success() <-chan struct{} {
	w.failures = 0
	ch := make(chan struct{})
	close(ch)
	return ch
}

// Marks that an operation failed. The chan returned will be selectable
// once the calculated retry wait amount of time has elapsed
func (w *Waiter) Failed() <-chan struct{} {
	w.failures += 1
	ch := w.wait()
	return ch
}

// Failures returns the current number of consecutive failures recorded.
func (w *Waiter) Failures() int {
	return int(w.failures)
}

// WaitIf is a convenience method to record whether the last
// operation was a success or failure and return a chan that
// will be selectable when the next operation can be done.
// Unlike Success, if failure is false the returned chan will block for MinWait.
func (w *Waiter) WaitIf(failure bool) <-chan struct{} {
	if failure {
		return w.Failed()
	}
	w.Success()
	return w.wait()
}
