package memory

import (
	"github.com/malt3/abstractfs-core/api"
	"github.com/malt3/abstractfs-core/provider"
)

type Provider struct{}

func (p Provider) Name() string {
	return "memory"
}

func (p Provider) SourceBuilder() provider.SourceBuilder {
	return &provider.UnsupportedSourceBuilder{}
}

func (p Provider) SinkBuilder() provider.SinkBuilder {
	return &provider.UnsupportedSinkBuilder{}
}

func (p Provider) CAS() (api.CAS, api.CloseWaitFunc, error) {
	return NewCAS(modeReadWrite), func() error { return nil }, nil
}

func (p Provider) CASReader() (api.CASReader, api.CloseWaitFunc, error) {
	return NewCAS(modeReadOnly), nil, nil
}

func (p Provider) CASWriter() (api.CASWriter, api.CloseWaitFunc, error) {
	return NewCAS(modeReadWrite), nil, nil
}

var _ provider.Provider = (*Provider)(nil)
