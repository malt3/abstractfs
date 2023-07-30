package casserve

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/malt3/abstractfs-core/api"
)

type Server struct {
	cas       api.CAS
	runnables []runnable
	stop      chan struct{}
}

func New(cas api.CAS) *Server {
	return &Server{
		cas:  cas,
		stop: make(chan struct{}, 1),
	}
}

func (s *Server) Add(r runnable) {
	s.runnables = append(s.runnables, r)
}

func (s *Server) AddHTTPListener(listener net.Listener) {
	s.Add(newHTTPServer(s.cas, listener))
}

func (s *Server) AddRecorderListener(listener net.Listener) {
	s.Add(newRecorderListener(s.cas, listener))
}

func (s *Server) AddRecorder(reader io.Reader) {
	recoder := newRecorderConsumerBuilder().
		WithCAS(s.cas).WithReader(reader).
		Build()
	s.Add(recoder)
}

func (s *Server) AddRecorderWithCancel(reader io.Reader, cancelFunc func() error) {
	recorderWithCancel := newRecorderConsumerBuilder().
		WithCAS(s.cas).WithReader(reader).WithCancelFunc(cancelFunc).
		Build()
	s.Add(recorderWithCancel)
}

func (s *Server) Serve(ctx context.Context) (err error) {
	wg := &sync.WaitGroup{}
	startErrs := make(chan error, len(s.runnables))
	stopErrs := make(chan error, len(s.runnables))
	defer func() {
		wg.Wait()
		err = errors.Join(collectErrors(startErrs), collectErrors(stopErrs))
	}()

	for _, run := range s.runnables {
		wg.Add(1)
		go func(run runnable) {
			defer wg.Done()
			err := run.Serve(ctx)
			if err != nil {
				startErrs <- err
			}
		}(run)
	}

	// wait for stop signal or context cancelation
	// TODO: detect case where all runnable Serve methods return before stop signal, log and return early.
	select {
	case <-ctx.Done():
	case <-s.stop:
	}

	// gracefully shutdown all runnables
	for _, run := range s.runnables {
		wg.Add(1)
		go func(run runnable) {
			defer wg.Done()
			err := run.Shutdown(ctx)
			if err != nil {
				stopErrs <- err
			}
		}(run)
	}

	return
}

func (s *Server) Stop() {
	select {
	case s.stop <- struct{}{}:
	default:
	}
}

func collectErrors(errs chan error) error {
	var collected []error
	close(errs)
	for err := range errs {
		collected = append(collected, err)
	}
	return errors.Join(collected...)
}
