package casserve

import "context"

type runnable interface {
	// Serve starts the runnable.
	// It blocks until the runnable is stopped.
	Serve(ctx context.Context) error
	// Shutdown stops the runnable.
	// If the runnable is already stopped, it does nothing.
	Shutdown(ctx context.Context) error
}
