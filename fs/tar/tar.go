package tar

import (
	archivetar "archive/tar"
	"io"

	"github.com/malt3/abstractfs-core/api"
)

type ReadFS struct {
	reader io.Reader
}

func NewReadFS(reader io.Reader) *ReadFS {
	return &ReadFS{reader: reader}
}

func (f *ReadFS) Source(opts Options) (api.Source, closeWaitFunc) {
	return NewSource(f.reader, opts)
}

type Reader interface {
	Next() (*archivetar.Header, error)
	io.Reader
}
