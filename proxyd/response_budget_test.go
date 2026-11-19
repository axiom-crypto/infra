package proxyd

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResponseLease(t *testing.T) {
	budget := &responseBudget{size: MinBufferedResponseBytes + 5}
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
