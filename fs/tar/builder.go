package tar

import (
	archivetar "archive/tar"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/malt3/abstractfs-core/api"
	"github.com/malt3/abstractfs-core/provider"
	"github.com/malt3/abstractfs-core/sri"
)

type SourceBuilder struct {
	SRIAlgorithm sri.Algorithm `abstractfs:"cas-algorithm"`
	NewReader    func(io.Reader) Reader
	// XAttrPaxPrefixes is a list of prefixes that are used to identify xattrs
	// later prefixes override earlier ones if the same xattr is set multiple times.
	XAttrPaxPrefixes []string `abstractfs:"xattr-prefixes"`
	Path             string
	IOReader         io.Reader
	invalidOptions   []string
}

// WithSourceRef sets the source reference.
// For the tar provider, the source reference is the path to the tar file.
func (b *SourceBuilder) WithSourceRef(ref string) provider.SourceBuilder {
	b.Path = ref
	return b
}

func (b *SourceBuilder) WithSRIAlgorithm(alg sri.Algorithm) *SourceBuilder {
	b.SRIAlgorithm = alg
	return b
}

func (b *SourceBuilder) WithNewReader(f func(io.Reader) Reader) *SourceBuilder {
	b.NewReader = f
	return b
}

// WithXAttrPaxPrefixes sets the xattr prefixes that are used to identify xattrs.
// Later prefixes override earlier ones if the same xattr is set multiple times.
// By default, the "SHILY.xattr." prefix is used.
// If set to []string{}, no xattrs are read.
func (b *SourceBuilder) WithXAttrPaxPrefixes(prefixes []string) *SourceBuilder {
	b.XAttrPaxPrefixes = prefixes
	return b
}

func (b *SourceBuilder) WithIOReader(r io.Reader) *SourceBuilder {
	b.IOReader = r
	return b
}

// Build builds the options.
func (b *SourceBuilder) Build() (api.Source, api.CloseWaitFunc, error) {
	b.applyDefaults()
	if err := b.check(); err != nil {
		return nil, nil, err
	}
	var fileCloser func() error
	if b.IOReader == nil {
		file, err := os.Open(b.Path)
		if err != nil {
			return nil, nil, err
		}
		fileCloser = file.Close
		b.IOReader = file
	}
	source := &Source{
		reader:           b.NewReader(b.IOReader),
		casStore:         NewCAS(b.IOReader),
		sriAlgorithm:     b.SRIAlgorithm,
		xattrPaxPrefixes: b.XAttrPaxPrefixes,
	}
	return source, func() error {
		if fileCloser != nil {
			return fileCloser()
		}
		return nil
	}, nil
}

func (o *SourceBuilder) applyDefaults() {
	if o.SRIAlgorithm == "" {
		o.SRIAlgorithm = sri.SHA256
	}
	if o.NewReader == nil {
		o.NewReader = newDefaultReader
	}
	if o.XAttrPaxPrefixes == nil {
		o.XAttrPaxPrefixes = []string{"SCHILY.xattr."}
		// TODO: support libarchive xattrs
		// They are encoded as urlencode(key), base64_encode(value)
		// o.XAttrPaxPrefixes = []string{"LIBARCHIVE.xattr.", "SCHILY.xattr."}
	}
}

func (b *SourceBuilder) check() error {
	if len(b.invalidOptions) > 0 {
		return fmt.Errorf("invalid options: %s", strings.Join(b.invalidOptions, ","))
	}
	if b.Path != "" && b.IOReader != nil {
		return errors.New("cannot set both path and io.Reader")
	}
	if b.Path == "" && b.IOReader == nil {
		return errors.New("must set either path or io.Reader")
	}
	return nil
}

type SinkBuilder struct {
	NewWriter func(io.Writer) Writer
	// Format is the tar format to use.
	// Valid values are FormatPAX, FormatGNU, FormatUSTAR.
	// By default, FormatPAX is used.
	// Other formats are not able to store all metadata and are thus lossy.
	Format archivetar.Format `abstractfs:"tar-format"`
	// Root is the root directory of the tar archive.
	// Common values are "" (skip over root directory), "/" or "." (write a root directory).
	Root string `abstractfs:"root"`
	// XattrPaxPrefix is the prefix to use for xattrs.
	// By default, the "SHILY.xattr." prefix is used.
	XAttrPaxPrefix string `abstractfs:"xattr-prefix"`
	// Path is the path to write the tar to.
	// If Path is set, the tar is written to the file.
	// Otherwise, the tar is written to the io.Writer.
	Path string
	// IOWriter is the io.Writer to write the tar to.
	// If IOWriter is set, the tar is written to the io.Writer.
	// Otherwise, the tar is written to the file specified by Path.
	IOWriter       io.Writer
	invalidOptions []string
}

// WithSinkRef sets the sink reference.
// For the tar provider, the source reference is the path to the tar file.
func (b *SinkBuilder) WithSinkRef(ref string) provider.SinkBuilder {
	b.Path = ref
	return b
}

// Set sets a option.
func (b *SinkBuilder) Set(key string, value any) provider.SinkBuilder {
	switch key {
	case "tar-format":
		format, ok := value.(string)
		if !ok {
			b.invalidOptions = append(b.invalidOptions, key)
			return b
		}
		switch strings.ToLower(format) {
		case "pax":
			b.Format = archivetar.FormatPAX
		case "gnu":
			b.Format = archivetar.FormatGNU
		case "ustar":
			b.Format = archivetar.FormatUSTAR
		default:
			b.invalidOptions = append(b.invalidOptions, key)
		}
	case "root":
		root, ok := value.(string)
		if !ok {
			b.invalidOptions = append(b.invalidOptions, key)
			return b
		}
		b.Root = root
	case "xattr-prefix":
		xattrPrefix, ok := value.(string)
		if !ok {
			b.invalidOptions = append(b.invalidOptions, key)
			return b
		}
		b.XAttrPaxPrefix = xattrPrefix
	default:
		b.invalidOptions = append(b.invalidOptions, key)
	}
	return b
}

func (b *SinkBuilder) WithNewWriter(f func(io.Writer) Writer) *SinkBuilder {
	b.NewWriter = f
	return b
}

func (b *SinkBuilder) WithFormat(format archivetar.Format) *SinkBuilder {
	b.Format = format
	return b
}

func (b *SinkBuilder) WithRoot(root string) *SinkBuilder {
	b.Root = root
	return b
}

func (b *SinkBuilder) WithXAttrPaxPrefix(prefix string) *SinkBuilder {
	b.XAttrPaxPrefix = prefix
	return b
}

func (b *SinkBuilder) WithIOWriter(w io.Writer) *SinkBuilder {
	b.IOWriter = w
	return b
}

// Build builds the options.
func (b *SinkBuilder) Build() (api.Sink, api.CloseWaitFunc, error) {
	b.applyDefaults()
	if err := b.check(); err != nil {
		return nil, nil, err
	}
	var fileCloser func() error
	if b.IOWriter == nil {
		file, err := os.Create(b.Path)
		if err != nil {
			return nil, nil, err
		}
		fileCloser = file.Close
		b.IOWriter = file
	}
	sink := &Sink{
		writer:         b.NewWriter(b.IOWriter),
		format:         b.Format,
		root:           b.Root,
		xattrPaxPrefix: b.XAttrPaxPrefix,
	}
	return sink, func() error {
		sink.writer.Close()
		if fileCloser != nil {
			return fileCloser()
		}
		return nil
	}, nil
}

func (o *SinkBuilder) applyDefaults() {
	if o.NewWriter == nil {
		o.NewWriter = newDefaultWriter
	}
	if o.Format == archivetar.FormatUnknown {
		o.Format = archivetar.FormatPAX
	}
	if o.XAttrPaxPrefix == "" {
		o.XAttrPaxPrefix = "SCHILY.xattr."
	}

}

func (b *SinkBuilder) check() error {
	if len(b.invalidOptions) > 0 {
		return fmt.Errorf("invalid options: %s", strings.Join(b.invalidOptions, ","))
	}
	if b.Path != "" && b.IOWriter != nil {
		return errors.New("cannot set both path and io.Writer")
	}
	if b.Path == "" && b.IOWriter == nil {
		return errors.New("must set either path or io.Writer")
	}
	return nil
}
