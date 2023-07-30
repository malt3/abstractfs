package casserve

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/malt3/abstractfs-core/api"
	"github.com/malt3/abstractfs-core/cas/recorder"
)

type recorderListener struct {
	wg             sync.WaitGroup
	cas            api.CASWriter
	listener       net.Listener
	stop           chan struct{}
	accept         chan net.Conn
	requestCounter atomic.Uint64
	handlers       map[uint64]runnable
	handlersLock   sync.Mutex
}

func newRecorderListener(cas api.CASWriter, listener net.Listener) *recorderListener {
	return &recorderListener{
		wg:       sync.WaitGroup{},
		cas:      cas,
		listener: listener,
		stop:     make(chan struct{}, 1),
		accept:   make(chan net.Conn),
		handlers: make(map[uint64]runnable),
	}
}

func (l *recorderListener) Serve(ctx context.Context) error {
	defer l.wg.Wait()

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.acceptRoutine()
	}()

	for {
		stop, err := l.acceptConn(ctx)
		if stop {
			closeErr := l.listener.Close()
			return errors.Join(err, closeErr)
		}
		if err != nil {
			// TODO: maybe log and continue?
			return err
		}
	}
}

func (l *recorderListener) Shutdown(ctx context.Context) error {
	defer l.wg.Wait()
	select {
	case l.stop <- struct{}{}:
	default:
	}
	l.handlersLock.Lock()
	defer l.handlersLock.Unlock()
	shutdownErrs := make([]error, 0, len(l.handlers))
	for connID, handler := range l.handlers {
		l.wg.Add(1)
		go func(connID uint64, handler runnable) {
			defer l.wg.Done()
			defer l.removeHandler(connID)
			shutdownErrs = append(shutdownErrs, handler.Shutdown(ctx))
		}(connID, handler)
	}
	return errors.Join(shutdownErrs...)
}

func (l *recorderListener) acceptConn(ctx context.Context) (stop bool, err error) {
	var conn net.Conn
	select {
	case <-ctx.Done():
		return true, ctx.Err()
	case <-l.stop:
		return true, l.listener.Close()
	case conn = <-l.accept:
	}

	connID := l.requestCounter.Add(1)
	handler := newRecorderConsumerBuilder().
		WithCAS(l.cas).WithReader(conn).WithCancelFunc(conn.Close).
		Build()

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		defer handler.Shutdown(ctx)
		defer l.removeHandler(connID)
		l.addHandler(connID, handler)
		// TODO: collect errors?
		handler.Serve(ctx)
	}()

	return false, nil
}

func (l *recorderListener) addHandler(id uint64, handler runnable) {
	l.handlersLock.Lock()
	defer l.handlersLock.Unlock()
	l.handlers[id] = handler
}

func (l *recorderListener) removeHandler(id uint64) {
	l.handlersLock.Lock()
	defer l.handlersLock.Unlock()
	delete(l.handlers, id)
}

func (l *recorderListener) acceptRoutine() {
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			return
		}
		l.accept <- conn
	}
}

type recorderConsumerBuilder struct {
	CAS        api.CASWriter
	Reader     io.Reader
	CancelFunc func() error
}

func newRecorderConsumerBuilder() *recorderConsumerBuilder {
	return &recorderConsumerBuilder{}
}

func (b *recorderConsumerBuilder) WithCAS(cas api.CASWriter) *recorderConsumerBuilder {
	b.CAS = cas
	return b
}

func (b *recorderConsumerBuilder) WithReader(reader io.Reader) *recorderConsumerBuilder {
	b.Reader = reader
	return b
}

func (b *recorderConsumerBuilder) WithCancelFunc(cancelFunc func() error) *recorderConsumerBuilder {
	b.CancelFunc = cancelFunc
	return b
}

func (b *recorderConsumerBuilder) Build() runnable {
	return &recorderConsumer{
		wg:         sync.WaitGroup{},
		cas:        b.CAS,
		reader:     b.Reader,
		cancelFunc: b.CancelFunc,
	}
}

// recorderConsumer handles a single connection.
// This can be an arbitrary io.Reader or a net.Conn.
type recorderConsumer struct {
	wg         sync.WaitGroup
	running    atomic.Bool
	cas        api.CASWriter
	reader     io.Reader
	cancelFunc func() error
}

func (l *recorderConsumer) Serve(ctx context.Context) error {
	if !l.running.CompareAndSwap(false, true) {
		return errors.New("already running")
	}
	defer l.running.Store(false)
	l.wg.Add(1)
	defer l.wg.Done()

	rec := recorder.New(l.cas, l.reader)
	return rec.Consume()
}

func (l *recorderConsumer) Shutdown(ctx context.Context) error {
	defer l.wg.Wait()
	if l.cancelFunc != nil {
		return l.cancelFunc()
	}
	return nil
}

var _ runnable = (*recorderListener)(nil)
