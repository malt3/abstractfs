package dir

import "github.com/malt3/abstractfs-core/api"

// FS is a os fs wrapper that is can return a api.Source.
type FS struct {
	// dir is the directory to walk.
	dir string
}

func New(dir string) *FS {
	return &FS{dir: dir}
}

func (f *FS) Source(opts Options) (api.Source, closeWaitFunc) {
	return NewSource(f.dir, opts)
}
