package integration_tests

import (
	"testing"
	"time"

	"github.com/ethereum-optimism/infra/proxyd"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

// The backend receives each relayed message with exactly the fields the client
// sent, once each, under the method proxyd checked against ws_method_whitelist.
func TestWSRelaysDecodedRequest(t *testing.T) {
	received := make(chan []byte, 16)
	wsBackend := NewMockWSBackend(nil, func(conn *websocket.Conn, msgType int, data []byte) {
		received <- data
	}, nil)
	defer wsBackend.Close()

	config := ReadConfig("ws")
	config.Server.RPCPort = 0
	config.Backends["good"].RPCURL = wsBackend.URL()
	config.Backends["good"].WSURL = wsBackend.URL()
	config.WSMethodWhitelist = []string{"eth_subscribe", "eth_unsubscribe"}

	_, shutdown, err := proxyd.Start(config)
	require.NoError(t, err)
	defer shutdown()

	conn, _, err := websocket.DefaultDialer.Dial("ws://127.0.0.1:8546", nil) // nolint:bodyclose
	require.NoError(t, err)
	defer conn.Close()

	tests := []struct {
		name    string
		sent    string
		relayed string
	}{
		{
			"string id",
			`{"jsonrpc":"2.0","id":"a","method":"eth_subscribe","params":["newHeads"]}`,
			`{"jsonrpc":"2.0","method":"eth_subscribe","params":["newHeads"],"id":"a"}`,
		},
		{
			"numeric id",
			`{"jsonrpc":"2.0","id":12345678901234567890,"method":"eth_subscribe","params":["newHeads"]}`,
			`{"jsonrpc":"2.0","method":"eth_subscribe","params":["newHeads"],"id":12345678901234567890}`,
		},
		{
			"null id",
			`{"jsonrpc":"2.0","id":null,"method":"eth_subscribe","params":["newHeads"]}`,
			`{"jsonrpc":"2.0","method":"eth_subscribe","params":["newHeads"],"id":null}`,
		},
		{
			"notification",
			`{"jsonrpc":"2.0","method":"eth_unsubscribe","params":["0x1"]}`,
			`{"jsonrpc":"2.0","method":"eth_unsubscribe","params":["0x1"]}`,
		},
		{
			"no params",
			`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe"}`,
			`{"jsonrpc":"2.0","method":"eth_subscribe","id":1}`,
		},
		{
			"case-variant method key",
			`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{}],"METHOD":"eth_subscribe"}`,
			`{"jsonrpc":"2.0","method":"eth_subscribe","params":[{}],"id":1}`,
		},
		{
			"duplicate method key",
			`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{}],"method":"eth_subscribe"}`,
			`{"jsonrpc":"2.0","method":"eth_subscribe","params":[{}],"id":1}`,
		},
		{
			"escaped method key",
			`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{}],"\u004dethod":"eth_subscribe"}`,
			`{"jsonrpc":"2.0","method":"eth_subscribe","params":[{}],"id":1}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(tt.sent)))
			select {
			case got := <-received:
				require.Equal(t, tt.relayed, string(got))
			case <-time.After(5 * time.Second):
				t.Fatal("backend received nothing")
			}
		})
	}
}

// A method outside ws_method_whitelist is counted as method_not_allowed, never
// under the name the client sent.
func TestWSRejectedMethodMetricLabel(t *testing.T) {
	wsBackend := NewMockWSBackend(nil, nil, nil)
	defer wsBackend.Close()

	config := ReadConfig("ws")
	config.Server.RPCPort = 0
	config.Backends["good"].RPCURL = wsBackend.URL()
	config.Backends["good"].WSURL = wsBackend.URL()

	_, shutdown, err := proxyd.Start(config)
	require.NoError(t, err)
	defer shutdown()

	conn, _, err := websocket.DefaultDialer.Dial("ws://127.0.0.1:8546", nil) // nolint:bodyclose
	require.NoError(t, err)
	defer conn.Close()

	before := sumCounter(t, "proxyd_rpc_errors_total", map[string]string{"method_name": proxyd.MethodNotAllowed})
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":1,"method":"client_chosen_name"}`)))
	_, _, err = conn.ReadMessage()
	require.NoError(t, err)

	require.Equal(t, before+1, sumCounter(t, "proxyd_rpc_errors_total", map[string]string{"method_name": proxyd.MethodNotAllowed}))
	require.Zero(t, sumCounter(t, "proxyd_rpc_errors_total", map[string]string{"method_name": "client_chosen_name"}))
}
