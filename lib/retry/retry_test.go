package retry

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJitter(t *testing.T) {
	repeat(t, "0 percent", func(t *testing.T) {
		jitter := NewJitter(0)
		for i := 0; i < 10; i++ {
			baseTime := time.Duration(i) * time.Second
			require.Equal(t, baseTime, jitter(baseTime))
		}
	})

	repeat(t, "10 percent", func(t *testing.T) {
		jitter := NewJitter(10)
		for i := 0; i < 10; i++ {
			baseTime := 5000 * time.Millisecond
			maxTime := 5500 * time.Millisecond
			newTime := jitter(baseTime)
			require.True(t, newTime > baseTime)
			require.True(t, newTime <= maxTime)
		}
	})

	repeat(t, "100 percent", func(t *testing.T) {
		jitter := NewJitter(100)
		for i := 0; i < 10; i++ {
			baseTime := 1234 * time.Millisecond
			maxTime := 2468 * time.Millisecond
			newTime := jitter(baseTime)
			require.True(t, newTime > baseTime)
			require.True(t, newTime <= maxTime)
		}
	})

	t.Run("with MaxInt64", func(t *testing.T) {
		jitter := NewJitter(100)
		jitter(time.Duration(math.MaxInt64))
	})
}

func repeat(t *testing.T, name string, fn func(t *testing.T)) {
	t.Run(name, func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			fn(t)
		}
	})
}

func TestRetryWaiter_calculateWait(t *testing.T) {
	t.Run("Defaults", func(t *testing.T) {
		rw := NewWaiter(0, 0, 0, nil)

		require.Equal(t, 0*time.Nanosecond, rw.calculateWait())
		rw.failures += 1
		require.Equal(t, 1*time.Second, rw.calculateWait())
		rw.failures += 1
		require.Equal(t, 2*time.Second, rw.calculateWait())
		rw.failures = 31
		require.Equal(t, defaultMaxWait, rw.calculateWait())
	})

	t.Run("Minimum Wait", func(t *testing.T) {
		rw := NewWaiter(0, 5*time.Second, 0, nil)

		require.Equal(t, 5*time.Second, rw.calculateWait())
		rw.failures += 1
		require.Equal(t, 5*time.Second, rw.calculateWait())
		rw.failures += 1
		require.Equal(t, 5*time.Second, rw.calculateWait())
		rw.failures += 1
		require.Equal(t, 5*time.Second, rw.calculateWait())
		rw.failures += 1
		require.Equal(t, 8*time.Second, rw.calculateWait())
	})

	t.Run("Minimum Failures", func(t *testing.T) {
		rw := NewWaiter(5, 0, 0, nil)
		require.Equal(t, 0*time.Nanosecond, rw.calculateWait())
		rw.failures += 5
		require.Equal(t, 0*time.Nanosecond, rw.calculateWait())
		rw.failures += 1
		require.Equal(t, 1*time.Second, rw.calculateWait())
	})

	t.Run("Maximum Wait", func(t *testing.T) {
		rw := NewWaiter(0, 0, 5*time.Second, nil)
		require.Equal(t, 0*time.Nanosecond, rw.calculateWait())
		rw.failures += 1
		require.Equal(t, 1*time.Second, rw.calculateWait())
		rw.failures += 1
		require.Equal(t, 2*time.Second, rw.calculateWait())
		rw.failures += 1
		require.Equal(t, 4*time.Second, rw.calculateWait())
		rw.failures += 1
		require.Equal(t, 5*time.Second, rw.calculateWait())
		rw.failures = 31
		require.Equal(t, 5*time.Second, rw.calculateWait())
	})
}

func TestWaiter_WaitChannels(t *testing.T) {
	w := &Waiter{
		MinFailures: 0,
		MinWait:     10 * time.Millisecond,
		MaxWait:     50 * time.Millisecond,
	}

	t.Run("Minimum Wait - Success", func(t *testing.T) {
		select {
		case <-time.After(2 * time.Millisecond):
			require.Fail(t, "Success should not wait")
		case <-w.Success():
		}
	})

	t.Run("Minimum Wait - WaitIf", func(t *testing.T) {
		defer w.Success()
		select {
		case <-time.After(2 * time.Millisecond):
		case <-w.WaitIf(false):
			require.Fail(t, "WaitIf did not wait long enough")
		}
	})

	w.MinWait = 0
	w.MaxWait = 50 * time.Millisecond
	t.Run("Maximum Wait - Failed", func(t *testing.T) {
		defer w.Success()
		select {
		case <-time.After(200 * time.Millisecond):
			require.Fail(t, "Failed waited too long")
		case <-w.Failed():
		}
	})

	t.Run("Maximum Wait - WaitIf", func(t *testing.T) {
		defer w.Success()
		select {
		case <-time.After(200 * time.Millisecond):
			require.Fail(t, "maximum WaitIf waited too long")
		case <-w.WaitIf(true):
		}
	})
}
