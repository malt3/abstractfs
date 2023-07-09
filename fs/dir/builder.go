package dir

import (
	"fmt"
	"strings"

	"github.com/malt3/abstractfs-core/api"
	"github.com/malt3/abstractfs-core/provider"
	"github.com/malt3/abstractfs-core/sri"
	"github.com/malt3/abstractfs/fs/generic"
)

type SourceBuilder struct {
	Dir          string
	SRIAlgorithm sri.Algorithm
	// KeepPrefix will keep the prefix of the dir.
	// If set, the dir path prefix will be removed from the node path.
	// If not set, the node path will be the real path of the node.
	// For example, if the node has the real path /foo/bar and the dir is /foo,
	// the node path will be /foo/bar if KeepPrefix is set and /bar if not.
	KeepPrefix     bool
	invalidOptions []string
}

// WithSourceRef sets the source reference.
func (b *SourceBuilder) WithSourceRef(ref string) provider.SourceBuilder {
	b.Dir = ref
	return b
}

// Set sets a option.
func (b *SourceBuilder) Set(key string, value any) provider.SourceBuilder {
	switch key {
	case provider.OptionCASAlgorithm:
		algorithm, ok := value.(sri.Algorithm)
		if !ok {
			b.invalidOptions = append(b.invalidOptions, key)
			return b
		}
		b.SRIAlgorithm = algorithm
	case "keep-prefix":
		if stripPrefix, ok := value.(bool); ok {
			b.KeepPrefix = stripPrefix
			return b
		}
		stripPrefix, ok := value.(string)
		if !ok {
			b.invalidOptions = append(b.invalidOptions, key)
			return b
		}
		switch strings.ToLower(stripPrefix) {
		case "true":
			b.KeepPrefix = true
		case "1":
			b.KeepPrefix = true
		case "false":
			b.KeepPrefix = false
		case "0":
			b.KeepPrefix = false
		default:
			b.invalidOptions = append(b.invalidOptions, key)
		}
	default:
		b.invalidOptions = append(b.invalidOptions, key)
	}
	return b
}

func (b *SourceBuilder) WithSRIAlgorithm(alg sri.Algorithm) *SourceBuilder {
	b.SRIAlgorithm = alg
	return b
}

func (b *SourceBuilder) WithKeepPrefix(keepPrefix bool) *SourceBuilder {
	b.KeepPrefix = keepPrefix
	return b
}

// Build builds the options.
func (b *SourceBuilder) Build() (api.Source, api.CloseWaitFunc, error) {
	b.applyDefaults()
	dir := b.Dir
	if strings.HasSuffix(dir, "/") {
		dir = dir[:len(dir)-1]
	}
	if err := b.check(); err != nil {
		return nil, nil, err
	}
	source := &Source{
		dir:          dir,
		casStore:     generic.NewCASStore(),
		sriAlgorithm: b.SRIAlgorithm,
		keepPrefix:   b.KeepPrefix,
		nodes:        make(chan next),
		stop:         make(chan struct{}, 1),
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
}

func (b *SourceBuilder) check() error {
	if len(b.invalidOptions) > 0 {
		return fmt.Errorf("invalid options: %s", strings.Join(b.invalidOptions, ","))
	}
	return nil
}
