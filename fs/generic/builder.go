package generic

import (
	"fmt"
	iofs "io/fs"
	"strings"

	"github.com/malt3/abstractfs-core/api"
	"github.com/malt3/abstractfs-core/provider"
	"github.com/malt3/abstractfs-core/sri"
)

type SourceBuilder struct {
	SRIAlgorithm   sri.Algorithm `abstractfs:"cas-algorithm"`
	NodeAttributes func(iofs.FileInfo) api.NodeAttributes
	StripPrefix    string `abstractfs:"strip-prefix"`
	FS             iofs.FS
	invalidOptions []string
}

// WithSourceRef sets the source reference.
// The generic provider does not support source references.
// It always needs a io.FS.
func (b *SourceBuilder) WithSourceRef(_ string) provider.SourceBuilder {
	return b
}

func (b *SourceBuilder) WithFS(fs iofs.FS) provider.SourceBuilder {
	b.FS = fs
	return b
}

func (b *SourceBuilder) WithNodeAttributes(nodeAttributes func(iofs.FileInfo) api.NodeAttributes) provider.SourceBuilder {
	b.NodeAttributes = nodeAttributes
	return b
}

// Build builds the options.
func (b *SourceBuilder) Build() (api.Source, api.CloseWaitFunc, error) {
	b.applyDefaults()
	if err := b.check(); err != nil {
		return nil, nil, err
	}
	source := &Source{
		inner:          b.FS,
		casStore:       NewCASStore(),
		sriAlgorithm:   b.SRIAlgorithm,
		nodeAttributes: b.NodeAttributes,
		stripPrefix:    b.StripPrefix,
		nodes:          make(chan next),
		stop:           make(chan struct{}, 1),
	}
	source.wg.Add(1)
	go source.walk()
	return source, func() error {
		source.stop <- struct{}{}
		source.wg.Wait()
		return nil
	}, nil
}

func (b *SourceBuilder) applyDefaults() {
	if b.SRIAlgorithm == "" {
		b.SRIAlgorithm = sri.SHA256
	}
	if b.NodeAttributes == nil {
		b.NodeAttributes = defaultNodeAttributes
	}
	if strings.HasSuffix(b.StripPrefix, "/") {
		b.StripPrefix = b.StripPrefix[:len(b.StripPrefix)-1]
	}
}

func (b *SourceBuilder) check() error {
	if len(b.invalidOptions) > 0 {
		return fmt.Errorf("invalid options: %s", strings.Join(b.invalidOptions, ","))
	}
	if b.FS == nil {
		return fmt.Errorf("missing fs")
	}
	return nil
}
