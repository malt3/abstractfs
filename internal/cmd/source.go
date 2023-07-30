package cmd

import (
	"fmt"

	"github.com/malt3/abstractfs-core/api"
	coreprovider "github.com/malt3/abstractfs-core/provider"
	"github.com/malt3/abstractfs/internal/providers"
)

func getSource(sourceRef, sourceType string, opts map[string]string) (api.Source, api.CloseWaitFunc, error) {
	provider, ok := providers.All[sourceType]
	if !ok {
		return nil, nil, fmt.Errorf("unknown source type %q", sourceType)
	}
	builder := provider.SourceBuilder().WithSourceRef(sourceRef)
	if err := coreprovider.SetOptions(builder, opts); err != nil {
		return nil, nil, fmt.Errorf("setting options: %w", err)
	}
	source, closer, err := builder.Build()
	if err != nil {
		return nil, nil, fmt.Errorf("building source: %w", err)
	}
	return source, closer, nil
}

func getSink(sinkRef, sinkType string, opts map[string]string) (api.Sink, api.CloseWaitFunc, error) {
	provider, ok := providers.All[sinkType]
	if !ok {
		return nil, nil, fmt.Errorf("unknown sink type %q", sinkType)
	}
	builder := provider.SinkBuilder().WithSinkRef(sinkRef)
	if err := coreprovider.SetOptions(builder, opts); err != nil {
		return nil, nil, fmt.Errorf("setting options: %w", err)
	}
	sink, closer, err := builder.Build()
	if err != nil {
		return nil, nil, fmt.Errorf("building sink: %w", err)
	}
	return sink, closer, nil
}

func getCASBackend(backendType string, opts map[string]string) (api.CAS, api.CloseWaitFunc, error) {
	provider, ok := providers.CAS[backendType]
	if !ok {
		return nil, nil, fmt.Errorf("unknown backend type %q", backendType)
	}
	cas, closer, err := provider.CAS()
	if err != nil {
		return nil, nil, fmt.Errorf("building backend: %w", err)
	}
	return cas, closer, nil
}
