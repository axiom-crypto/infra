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
// memory at once, [server] max_buffered_response_bytes, and the part of them
// that the requests of one client hold, max_buffered_response_bytes_per_client,
// so that one client filling its part leaves the rest to the others.
type responseBudget struct {
	mu        sync.Mutex
	size      int64
	perClient int64
	held      int64
	// Bytes each client holds, by its address: the first entry of the rate
	// limit header, which whatever is in front of proxyd must set. A client
	// that holds nothing has no entry.
	heldBy map[string]int64
}

func newResponseBudget(size, perClient int64) *responseBudget {
	if perClient == 0 {
		perClient = size
	}
	return &responseBudget{size: size, perClient: perClient, heldBy: make(map[string]int64)}
}

// free is what client may still take. The caller holds mu.
func (b *responseBudget) free(client string) int64 {
	return min(b.size-b.held, b.perClient-b.heldBy[client])
}

// take moves n bytes to client, or back from it if n is negative. The caller
// holds mu.
func (b *responseBudget) take(client string, n int64) {
	b.held += n
	if b.heldBy[client] += n; b.heldBy[client] == 0 {
		delete(b.heldBy, client)
	}
	bufferedResponseBytesGauge.Set(float64(b.held))
}

// A responseLease holds the backend response bytes of one client request, from
// when they are read until the response to the client is written.
type responseLease struct {
	budget *responseBudget
	client string
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
	lease := &responseLease{budget: budget, client: stripXFF(GetXForwardedFor(ctx))}
	return context.WithValue(ctx, responseLeaseKey{}, lease), func() {
		budget.mu.Lock()
		defer budget.mu.Unlock()
		if !lease.ended {
			budget.take(lease.client, -lease.held)
			lease.held, lease.ended = 0, true
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
	if l.ended || n > b.free(l.client) {
		return false
	}
	l.held += n
	b.take(l.client, n)
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
	return b.free(lease.client) >= MinBufferedResponseBytes
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
