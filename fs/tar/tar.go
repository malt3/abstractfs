package tar

import (
	archivetar "archive/tar"
	"io"

	"github.com/malt3/abstractfs-core/api"
	"github.com/malt3/abstractfs-core/provider"
)

type Provider struct{}

func (p Provider) Name() string {
	return "dir"
}

func (p Provider) SourceBuilder() provider.SourceBuilder {
	return &SourceBuilder{}
}

func (p Provider) SinkBuilder() provider.SinkBuilder {
	return &SinkBuilder{}
}

func (p Provider) CAS() (api.CAS, api.CloseWaitFunc, error) {
	return nil, nil, provider.ErrUnsupported
}

func (p Provider) CASReader() (api.CASReader, api.CloseWaitFunc, error) {
	return nil, nil, provider.ErrUnsupported
}

func (p Provider) CASWriter() (api.CASWriter, api.CloseWaitFunc, error) {
	return nil, nil, provider.ErrUnsupported
}

var _ provider.Provider = (*Provider)(nil)

type Reader interface {
	Next() (*archivetar.Header, error)
	io.Reader
}

type Writer interface {
	Close() error
	Flush() error
	WriteHeader(hdr *archivetar.Header) error
	io.Writer
}
