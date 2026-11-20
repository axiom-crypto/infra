package proxyd

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResponseLease(t *testing.T) {
	budget := newResponseBudget(MinBufferedResponseBytes+5, 0)
	ctx, end := withResponseLease(context.Background(), budget)
	require.True(t, admitResponse(ctx))

	_, err := io.ReadAll(newLeaseReader(ctx, strings.NewReader("123456")))
	require.NoError(t, err)
	require.False(t, admitResponse(ctx))
	require.True(t, admitResponse(context.Background()), "a call without a lease is unlimited")

	// A read that outlives its request takes nothing once the lease has ended,
	// and ending the lease twice returns its bytes once.
	end()
	_, err = io.ReadAll(newLeaseReader(ctx, strings.NewReader("1")))
	require.ErrorIs(t, err, ErrServerOverCapacity)
	end()
	require.Zero(t, budget.held)
}

// A client that has filled its part is refused while the budget still has
// room, which another client can use. Its part is free again once its request
// ends.
func TestResponseBudgetPerClient(t *testing.T) {
	budget := newResponseBudget(4*MinBufferedResponseBytes, MinBufferedResponseBytes+5)
	from := func(client string) context.Context {
		return context.WithValue(context.Background(), ContextKeyXForwardedFor, client) // nolint:staticcheck
	}
	read := func(ctx context.Context, n int) error {
		_, err := io.ReadAll(newLeaseReader(ctx, strings.NewReader(strings.Repeat("a", n))))
		return err
	}

	first, endFirst := withResponseLease(from("10.0.0.1"), budget)
	require.NoError(t, read(first, 6))
	require.False(t, admitResponse(first))

	// Another request of the same client shares its part.
	second, endSecond := withResponseLease(from("10.0.0.1, 10.9.9.9"), budget)
	require.False(t, admitResponse(second))
	require.ErrorIs(t, read(second, MinBufferedResponseBytes), ErrServerOverCapacity)

	other, endOther := withResponseLease(from("10.0.0.2"), budget)
	defer endOther()
	require.True(t, admitResponse(other))
	require.NoError(t, read(other, MinBufferedResponseBytes))

	endFirst()
	endSecond()
	third, endThird := withResponseLease(from("10.0.0.1"), budget)
	defer endThird()
	require.True(t, admitResponse(third))
	require.Equal(t, map[string]int64{"10.0.0.2": MinBufferedResponseBytes}, budget.heldBy)
}
