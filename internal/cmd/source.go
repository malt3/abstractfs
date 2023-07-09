package cmd

import (
	"fmt"

	"github.com/malt3/abstractfs-core/api"
	"github.com/malt3/abstractfs/internal/providers"
)

func getSource(sourceRef, sourceType string) (api.Source, api.CloseWaitFunc, error) {
	provider, ok := providers.All[sourceType]
	if !ok {
		return nil, nil, fmt.Errorf("unknown source type %q", sourceType)
	}
	source, closer, err := provider.SourceBuilder().WithSourceRef(sourceRef).Build()
	if err != nil {
		return nil, nil, fmt.Errorf("building source: %w", err)
	}
	return source, closer, nil
}

func getSink(sinkRef, sinkType string) (api.Sink, api.CloseWaitFunc, error) {
	provider, ok := providers.All[sinkType]
	if !ok {
		return nil, nil, fmt.Errorf("unknown sink type %q", sinkType)
	}
	sink, closer, err := provider.SinkBuilder().WithSinkRef(sinkRef).Build()
	if err != nil {
		return nil, nil, fmt.Errorf("building sink: %w", err)
	}
	return sink, closer, nil
}
