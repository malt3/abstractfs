package generic

import (
	"github.com/malt3/abstractfs-core/api"
	"github.com/malt3/abstractfs-core/provider"
)

type Provider struct{}

func (p Provider) Name() string {
	return "generic"
}

func (p Provider) SourceBuilder() provider.SourceBuilder {
	return &SourceBuilder{}
}

func (p Provider) SinkBuilder() provider.SinkBuilder {
	return &provider.UnsupportedSinkBuilder{}
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
