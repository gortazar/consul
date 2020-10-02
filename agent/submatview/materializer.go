package submatview

import (
	"context"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hashicorp/consul/agent/cache"
	"github.com/hashicorp/consul/lib/retry"
	"github.com/hashicorp/consul/proto/pbsubscribe"
)

// View is the interface used to manage they type-specific
// materialized view logic.
type View interface {
	// Update is called when one or more events are received. The first call will
	// include _all_ events in the initial snapshot which may be an empty set.
	// Subsequent calls will contain one or more update events in the order they
	// are received.
	Update(events []*pbsubscribe.Event) error

	// Result returns the type-specific cache result based on the state. When no
	// events have been delivered yet the result should be an empty value type
	// suitable to return to clients in case there is an empty result on the
	// servers. The index the materialized view represents is maintained
	// separately and passed in in case the return type needs an Index field
	// populating. This allows implementations to not worry about maintaining
	// indexes seen during Update.
	Result(index uint64) (interface{}, error)

	// Reset the view to the zero state, done in preparation for receiving a new
	// snapshot.
	Reset()
}

// resetErr represents a server request to reset the subscription, it's typed so
// we can mark it as temporary and so attempt to retry first time without
// notifying clients.
type resetErr string

// Temporary Implements the internal Temporary interface
func (e resetErr) Temporary() bool {
	return true
}

// Error implements error
func (e resetErr) Error() string {
	return string(e)
}

// TODO: update godoc
// Materializer is a partial view of the state on servers, maintained via
// streaming subscriptions. It is specialized for different cache types by
// providing a View that encapsulates the logic to update the
// state and format it as the correct result type.
//
// The Materializer object becomes the cache.Result.State for a streaming
// cache type and manages the actual streaming RPC call to the servers behind
// the scenes until the cache result is discarded when TTL expires.
type Materializer struct {
	// Properties above the lock are immutable after the view is constructed in
	// NewMaterializer and must not be modified.
	deps MaterializerDeps

	retryWaiter *retry.Waiter
	handler     eventHandler

	// l protects the mutable state - all fields below it must only be accessed
	// while holding l.
	l        sync.Mutex
	index    uint64
	view     View
	updateCh chan struct{}
	err      error
}

// TODO: rename
type MaterializerDeps struct {
	View    View
	Client  StreamingClient
	Logger  hclog.Logger
	Waiter  *retry.Waiter
	Request func(index uint64) pbsubscribe.SubscribeRequest
	Stop    func()
	Done    <-chan struct{}
}

// StreamingClient is the interface we need from the gRPC client stub. Separate
// interface simplifies testing.
type StreamingClient interface {
	Subscribe(ctx context.Context, in *pbsubscribe.SubscribeRequest, opts ...grpc.CallOption) (pbsubscribe.StateChangeSubscription_SubscribeClient, error)
}

// NewMaterializer retrieves an existing view from the cache result
// state if one exists, otherwise creates a new one. Note that the returned view
// MUST have Close called eventually to avoid leaking resources. Typically this
// is done automatically if the view is returned in a cache.Result.State when
// the cache evicts the result. If the view is not returned in a result state
// though Close must be called some other way to avoid leaking the goroutine and
// memory.
func NewMaterializer(deps MaterializerDeps) *Materializer {
	v := &Materializer{
		deps:        deps,
		view:        deps.View,
		retryWaiter: deps.Waiter,
	}
	v.reset()
	return v
}

// Close implements io.Close and discards view state and stops background view
// maintenance.
func (m *Materializer) Close() error {
	m.l.Lock()
	defer m.l.Unlock()
	m.deps.Stop()
	return nil
}

func (m *Materializer) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		req := m.deps.Request(m.index)
		err := m.runSubscription(ctx, req)
		if ctx.Err() != nil {
			return
		}

		m.l.Lock()
		// TODO: move this into a func
		// If this is a temporary error and it's the first consecutive failure,
		// retry to see if we can get a result without erroring back to clients.
		// If it's non-temporary or a repeated failure return to clients while we
		// retry to get back in a good state.
		if _, ok := err.(temporary); !ok || m.retryWaiter.Failures() > 0 {
			// Report error to blocked fetchers
			m.err = err
			m.notifyUpdateLocked()
		}
		waitCh := m.retryWaiter.Failed()
		failures := m.retryWaiter.Failures()
		m.l.Unlock()

		m.deps.Logger.Error("subscribe call failed",
			"err", err,
			"topic", req.Topic,
			"key", req.Key,
			"failure_count", failures)

		select {
		case <-ctx.Done():
			return
		case <-waitCh:
		}
	}
}

// temporary is a private interface as used by net and other std lib packages to
// show error types represent temporary/recoverable errors.
type temporary interface {
	Temporary() bool
}

// runSubscription opens a new subscribe streaming call to the servers and runs
// for it's lifetime or until the view is closed.
func (m *Materializer) runSubscription(ctx context.Context, req pbsubscribe.SubscribeRequest) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	m.handler = m.initialHandler(req.Index)

	s, err := m.deps.Client.Subscribe(ctx, &req)
	if err != nil {
		return err
	}

	for {
		event, err := s.Recv()
		switch {
		case isGrpcStatus(err, codes.Aborted):
			m.reset()
			return resetErr("stream reset requested")
		case err != nil:
			return err
		}

		m.handler, err = m.handler(event)
		if err != nil {
			m.reset()
			return err
		}
	}
}

func isGrpcStatus(err error, code codes.Code) bool {
	s, ok := status.FromError(err)
	return ok && s.Code() == code
}

// reset clears the state ready to start a new stream from scratch.
func (m *Materializer) reset() {
	m.l.Lock()
	defer m.l.Unlock()

	m.view.Reset()
	m.index = 0
	m.err = nil
	m.notifyUpdateLocked()
	m.retryWaiter.Success()
}

func (m *Materializer) updateView(events []*pbsubscribe.Event, index uint64) error {
	m.l.Lock()
	defer m.l.Unlock()
	if err := m.view.Update(events); err != nil {
		return err
	}

	m.index = index
	m.err = nil
	m.notifyUpdateLocked()
	m.retryWaiter.Success()
	return nil
}

// notifyUpdateLocked closes the current update channel and recreates a new
// one. It must be called while holding the s.l lock.
func (m *Materializer) notifyUpdateLocked() {
	if m.updateCh != nil {
		close(m.updateCh)
	}
	m.updateCh = make(chan struct{})
}

// Fetch implements the logic a StreamingCacheType will need during it's Fetch
// call. Cache types that use streaming should just be able to proxy to this
// once they have a subscription object and return it's results directly.
func (m *Materializer) Fetch(opts cache.FetchOptions) (cache.FetchResult, error) {
	var result cache.FetchResult

	// Get current view Result and index
	m.l.Lock()
	index := m.index
	val, err := m.view.Result(m.index)
	updateCh := m.updateCh
	m.l.Unlock()

	if err != nil {
		return result, err
	}

	result.Index = index
	result.Value = val
	result.State = m

	// If our index is > req.Index return right away. If index is zero then we
	// haven't loaded a snapshot at all yet which means we should wait for one on
	// the update chan. Note it's opts.MinIndex that the cache is using here the
	// request min index might be different and from initial user request.
	if index > 0 && index > opts.MinIndex {
		return result, nil
	}

	// Watch for timeout of the Fetch. Note it's opts.Timeout not req.Timeout
	// since that is the timeout the client requested from the cache Get while the
	// options one is the internal "background refresh" timeout which is what the
	// Fetch call should be using.
	timeoutCh := time.After(opts.Timeout)
	for {
		select {
		case <-updateCh:
			// View updated, return the new result
			m.l.Lock()
			result.Index = m.index
			// Grab the new updateCh in case we need to keep waiting for the next
			// update.
			updateCh = m.updateCh
			fetchErr := m.err
			if fetchErr == nil {
				// Only generate a new result if there was no error to avoid pointless
				// work potentially shuffling the same data around.
				result.Value, err = m.view.Result(m.index)
			}
			m.l.Unlock()

			// If there was a non-transient error return it
			if fetchErr != nil {
				return result, fetchErr
			}
			if err != nil {
				return result, err
			}

			// Sanity check the update is actually later than the one the user
			// requested.
			if result.Index <= opts.MinIndex {
				// The result is still older/same as the requested index, continue to
				// wait for further updates.
				continue
			}

			// Return the updated result
			return result, nil

		case <-timeoutCh:
			// Just return whatever we got originally, might still be empty
			return result, nil

		case <-m.deps.Done:
			return result, context.Canceled
		}
	}
}
