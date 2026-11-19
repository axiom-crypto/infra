package proxyd

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/ethereum/go-ethereum/log"
)

// MinBufferedResponseBytes is the free budget a call needs before it is sent
// to a backend, so that a nearly full budget refuses a call before the backend
// acts on it rather than after, when its response would be lost.
const MinBufferedResponseBytes = 1 << 20

// A responseBudget bounds the backend response bytes that all requests hold in
// memory at once: [server] max_buffered_response_bytes.
type responseBudget struct {
	mu   sync.Mutex
	size int64
	held int64
}

// A responseLease holds the backend response bytes of one client request, from
// when they are read until the response to the client is written.
type responseLease struct {
	budget *responseBudget
	held   int64 // guarded by budget.mu, as is ended
	ended  bool
}

type responseLeaseKey struct{}

// withResponseLease returns ctx carrying a new lease on budget, and the
// function that ends the lease and returns its bytes. A nil budget is
// unlimited.
func withResponseLease(ctx context.Context, budget *responseBudget) (context.Context, func()) {
	if budget == nil {
		return ctx, func() {}
	}
	lease := &responseLease{budget: budget}
	return context.WithValue(ctx, responseLeaseKey{}, lease), func() {
		budget.mu.Lock()
		defer budget.mu.Unlock()
		if !lease.ended {
			budget.held -= lease.held
			lease.held, lease.ended = 0, true
			bufferedResponseBytesGauge.Set(float64(budget.held))
		}
	}
}

// add takes n bytes from the budget. It fails if they are not free, and once
// the lease has ended, so that a backend read that outlives its request holds
// nothing.
func (l *responseLease) add(n int64) bool {
	b := l.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if l.ended || b.held+n > b.size {
		return false
	}
	l.held += n
	b.held += n
	bufferedResponseBytesGauge.Set(float64(b.held))
	return true
}

// admitResponse reports whether the call in ctx may be sent to a backend. It
// holds nothing, so a call that waits long for its backend takes no budget
// until its response arrives.
func admitResponse(ctx context.Context) bool {
	lease, _ := ctx.Value(responseLeaseKey{}).(*responseLease)
	if lease == nil {
		return true
	}
	b := lease.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size-b.held >= MinBufferedResponseBytes
}

// A leaseReader holds every byte it reads on the lease in ctx, until the lease
// ends, and fails with ErrServerOverCapacity once the lease cannot hold more.
type leaseReader struct {
	r     io.Reader
	lease *responseLease
}

func newLeaseReader(ctx context.Context, r io.Reader) *leaseReader {
	lease, _ := ctx.Value(responseLeaseKey{}).(*responseLease)
	return &leaseReader{r: r, lease: lease}
}

func (l *leaseReader) Read(p []byte) (int, error) {
	n, err := l.r.Read(p)
	if l.lease != nil && n > 0 && !l.lease.add(int64(n)) {
		return 0, ErrServerOverCapacity
	}
	return n, err
}

// forwardErrorLog logs a refused call at debug level: any client can cause
// one for every call it sends.
func forwardErrorLog(err error) func(msg string, ctx ...interface{}) {
	if errors.Is(err, ErrServerOverCapacity) {
		return log.Debug
	}
	return log.Error
}
