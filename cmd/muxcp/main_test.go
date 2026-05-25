package main

import (
	"context"
	"testing"
	"time"
)

func TestWatchParentCancelsOnOrphan(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	orphaned := make(chan struct{})
	go watchParent(ctx, cancel, func() int { return 1 }, func() { close(orphaned) })

	select {
	case <-orphaned:
	case <-time.After(3 * time.Second):
		t.Fatal("watchParent did not detect orphaned parent")
	}

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("watchParent did not cancel context")
	}
}

func TestWatchParentReturnsOnContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	orphaned := make(chan struct{})
	done := make(chan struct{})
	go func() {
		watchParent(ctx, func() {}, func() int { return 1234 }, func() { close(orphaned) })
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watchParent did not return after context cancellation")
	}

	select {
	case <-orphaned:
		t.Fatal("watchParent called orphan callback after context cancellation")
	default:
	}
}
