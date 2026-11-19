package integration_tests

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum-optimism/infra/proxyd"
	"github.com/ethereum/go-ethereum/log"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// response_budget.toml's max_buffered_response_bytes, 3.5 MiB, has room for
// three of these results, and not for one of overBudgetResult. Once three are
// held, less than proxyd.MinBufferedResponseBytes is free.
var (
	budgetResult     = strings.Repeat("a", 1_000_000)
	overBudgetResult = strings.Repeat("b", 4_000_000)
)

func budgetOK(id int) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","result":"%s","id":%d}`, budgetResult, id)
}

func budgetRejected(id int) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","error":{"code":-32012,"message":"server is over capacity, retry later"},"id":%d}`, id)
}

func budgetBatch(n int) []*proxyd.RPCReq {
	reqs := make([]*proxyd.RPCReq, n)
	for i := range reqs {
		reqs[i] = NewRPCReq(fmt.Sprint(i+1), "eth_getBlockReceipts", nil)
	}
	return reqs
}

// budgetBackend answers eth_getBlockReceipts with budgetResult and eth_getLogs
// with overBudgetResult, and counts the requests that reach it. eth_call sends
// heldBytes of its result and then waits for hold to close before finishing,
// so proxyd holds those bytes for as long as the test wants.
type budgetBackend struct {
	router    *BatchRPCResponseRouter
	requests  atomic.Int32
	hold      chan struct{}
	release   func()
	heldBytes int
}

func newBudgetBackend() *budgetBackend {
	router := NewBatchRPCResponseRouter()
	router.SetFallbackRoute("eth_getBlockReceipts", budgetResult)
	router.SetFallbackRoute("eth_getLogs", overBudgetResult)
	hold := make(chan struct{})
	return &budgetBackend{
		router:    router,
		hold:      hold,
		release:   sync.OnceFunc(func() { close(hold) }),
		heldBytes: 2_900_000,
	}
}

func (b *budgetBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}
	if strings.Contains(string(body), `"eth_call"`) {
		_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":999,"result":"%s`, strings.Repeat("c", b.heldBytes))
		w.(http.Flusher).Flush()
		<-b.hold
		_, _ = w.Write([]byte(`"}`))
		return
	}
	b.requests.Add(1)
	r.Body = io.NopCloser(bytes.NewReader(body))
	b.router.ServeHTTP(w, r)
}

func bufferedResponseBytes(t *testing.T) float64 {
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == "proxyd_buffered_response_bytes" {
			return mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	return -1
}

// requireBudgetFree waits for every request's lease to end: a lease ends after
// the response is written, so the client can see the response first.
func requireBudgetFree(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool { return bufferedResponseBytes(t) == 0 }, 2*time.Second, 10*time.Millisecond)
}

// requireNoWarnings fails the test if anything logs at warn level or above
// after it is called: any client can cause a rejection for every call it
// sends, so rejections must not reach the logs that operators collect.
func requireNoWarnings(t *testing.T) {
	t.Helper()
	var buf bytes.Buffer
	prev := log.Root()
	log.SetDefault(log.NewLogger(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() {
		log.SetDefault(prev)
		require.Empty(t, buf.String())
	})
}

func startBudgetProxyd(t *testing.T, backend *budgetBackend, configure func(*proxyd.Config)) *ProxydHTTPClient {
	t.Helper()
	server := httptest.NewServer(backend)
	t.Cleanup(server.Close)
	// Runs before server.Close, which waits for a held eth_call to finish.
	t.Cleanup(backend.release)
	wsBackend := NewMockWSBackend(nil, nil, nil)
	t.Cleanup(wsBackend.Close)
	require.NoError(t, os.Setenv("GOOD_BACKEND_RPC_URL", server.URL))
	require.NoError(t, os.Setenv("GOOD_BACKEND_WS_URL", wsBackend.URL()))

	config := ReadConfig("response_budget")
	if configure != nil {
		configure(config)
	}
	_, shutdown, err := proxyd.Start(config)
	require.NoError(t, err)
	t.Cleanup(shutdown)
	return NewProxydClient("http://127.0.0.1:8545")
}

func TestResponseBudgetFailsCallsThatDoNotFit(t *testing.T) {
	backend := newBudgetBackend()
	client := startBudgetProxyd(t, backend, nil)
	requireNoWarnings(t)

	// Each call is its own backend request (max_upstream_batch_size = 1), and
	// the first three responses take the budget until the batch is written, so
	// the last two calls never reach the backend.
	res, code, err := client.SendBatchRPC(budgetBatch(5)...)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	RequireEqualJSON(t, []byte(asArray(budgetOK(1), budgetOK(2), budgetOK(3), budgetRejected(4), budgetRejected(5))), res)
	require.Equal(t, int32(3), backend.requests.Load())
	requireBudgetFree(t)

	// The whole budget is free again.
	res, code, err = client.SendBatchRPC(budgetBatch(3)...)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	RequireEqualJSON(t, []byte(asArray(budgetOK(1), budgetOK(2), budgetOK(3))), res)
	requireBudgetFree(t)

	// A response larger than what is free fails after the backend answered,
	// rather than waits; a single request takes the status of its error.
	res, code, err = client.SendRPC("eth_getLogs", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, code)
	RequireEqualJSON(t, []byte(budgetRejected(999)), res)
	requireBudgetFree(t)

	res, code, err = client.SendRPC("eth_getBlockReceipts", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	RequireEqualJSON(t, []byte(budgetOK(999)), res)
	requireBudgetFree(t)
}

// The budget is shared by independent requests: while one holds it, another
// is refused before its call reaches the backend, and is served once the
// first has been written.
func TestResponseBudgetHeldByAnotherRequest(t *testing.T) {
	backend := newBudgetBackend()
	client := startBudgetProxyd(t, backend, nil)
	requireNoWarnings(t)

	holder := make(chan []byte)
	go func() {
		res, _, _ := client.SendRPC("eth_call", nil)
		holder <- res
	}()
	require.Eventually(t, func() bool {
		return bufferedResponseBytes(t) >= float64(backend.heldBytes)
	}, 5*time.Second, 10*time.Millisecond)

	res, code, err := client.SendRPC("eth_getBlockReceipts", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, code)
	RequireEqualJSON(t, []byte(budgetRejected(999)), res)
	require.Equal(t, int32(0), backend.requests.Load())

	backend.release()
	require.Equal(t, fmt.Sprintf(`{"jsonrpc":"2.0","result":"%s","id":999}`, strings.Repeat("c", backend.heldBytes)), strings.TrimSpace(string(<-holder)))
	requireBudgetFree(t)

	res, code, err = client.SendRPC("eth_getBlockReceipts", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	RequireEqualJSON(t, []byte(budgetOK(999)), res)
	require.Equal(t, int32(1), backend.requests.Load())
	requireBudgetFree(t)
}

func TestResponseBudgetWSForwardedRPC(t *testing.T) {
	startBudgetProxyd(t, newBudgetBackend(), nil)
	requireNoWarnings(t)

	conn, _, err := websocket.DefaultDialer.Dial("ws://127.0.0.1:8546", nil) // nolint:bodyclose
	require.NoError(t, err)
	defer conn.Close()

	// RPCs in rpc_method_mappings reach the backend over HTTP and hold their
	// response until it is written to the WebSocket.
	for _, tc := range []struct{ method, res string }{
		{"eth_getLogs", budgetRejected(1)},
		{"eth_getBlockReceipts", budgetOK(1)},
	} {
		require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"jsonrpc":"2.0","method":"%s","id":1}`, tc.method))))
		_, msg, err := conn.ReadMessage()
		require.NoError(t, err)
		RequireEqualJSON(t, []byte(tc.res), msg)
		requireBudgetFree(t)
	}
}
