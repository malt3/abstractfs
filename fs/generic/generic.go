package generic

import (
	iofs "io/fs"

	"github.com/malt3/abstractfs-core/api"
)

type FS struct {
	inner iofs.FS
}

func New(inner iofs.FS) *FS {
	return &FS{inner: inner}
}

func (f *FS) Source(opts Options) (api.Source, closeWaitFunc) {
	return NewSource(f.inner, opts)
}
