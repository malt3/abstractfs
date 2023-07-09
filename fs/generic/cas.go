package generic

import (
	"io"
	"io/fs"
	"sync"
)

type CAS struct {
	inner    fs.FS
	casStore *CASStore
}

func NewCAS(inner fs.FS, casStore *CASStore) *CAS {
	return &CAS{
		inner:    inner,
		casStore: casStore,
	}
}

func (c *CAS) Open(sri string) (io.ReadCloser, error) {
	path, ok := c.casStore.Get(sri)
	if !ok {
		return nil, fs.ErrNotExist
	}
	return c.inner.Open(path)
}

type CASStore struct {
	mux sync.RWMutex
	// inner is the lookup table for sri -> file path.
	inner map[string]string
}

func NewCASStore() *CASStore {
	return &CASStore{
		inner: make(map[string]string),
	}
}

// Get returns the file path for the given sri.
func (c *CASStore) Get(sri string) (string, bool) {
	c.mux.RLock()
	defer c.mux.RUnlock()
	path, ok := c.inner[sri]
	return path, ok
}

// Set sets the file path for the given sri.
// If the sri already exists, the old file path will be kept.
func (c *CASStore) Set(sri, path string) {
	c.mux.Lock()
	defer c.mux.Unlock()
	_, ok := c.inner[sri]
	if ok {
		return
	}
	c.inner[sri] = path
}
