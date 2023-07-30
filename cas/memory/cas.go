package memory

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sync"

	"github.com/malt3/abstractfs-core/api"
	coresri "github.com/malt3/abstractfs-core/sri"
)

type CAS struct {
	mux      sync.RWMutex
	m        map[string][]byte
	readonly bool
}

func NewCAS(readonly bool) *CAS {
	return &CAS{
		m:        map[string][]byte{},
		readonly: readonly,
	}
}

func (c *CAS) Open(sri string) (io.ReadCloser, error) {
	c.mux.RLock()
	defer c.mux.RUnlock()

	b, ok := c.m[sri]
	if !ok {
		return nil, fs.ErrNotExist
	}

	return io.NopCloser(bytes.NewReader(b)), nil
}

func (c *CAS) Write(sri string, r io.Reader) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if _, ok := c.m[sri]; ok {
		return nil
	}

	if c.readonly {
		return errors.New("cas is readonly")
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	integrity, err := coresri.FromString(sri)
	if err != nil {
		return fmt.Errorf("checking sri on write: %w", err)
	}
	if err := integrity.Validate(bytes.NewReader(b)); err != nil {
		return fmt.Errorf("validating sri on write: %w", err)
	}

	c.m[sri] = b
	return nil
}

const (
	modeReadWrite = false
	modeReadOnly  = true
)

var _ api.CAS = (*CAS)(nil)
