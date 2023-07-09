package generic

import (
	"errors"
	"io"
	"io/fs"
	iofs "io/fs"
	"strconv"
	"strings"
	"sync"

	"github.com/malt3/abstractfs-core/api"
	"github.com/malt3/abstractfs-core/kind"
	"github.com/malt3/abstractfs-core/sri"
)

type Options struct {
	SRIAlgorithm   sri.Algorithm
	NodeAttributes func(iofs.FileInfo) api.NodeAttributes
	StripPrefix    string
}

func NewDefaultOptions() Options {
	opts := Options{}
	opts.applyDefaults()
	return opts
}

func (o *Options) applyDefaults() {
	if o.SRIAlgorithm == "" {
		o.SRIAlgorithm = sri.SHA256
	}
	if o.NodeAttributes == nil {
		o.NodeAttributes = defaultNodeAttributes
	}
	if strings.HasSuffix(o.StripPrefix, "/") {
		o.StripPrefix = o.StripPrefix[:len(o.StripPrefix)-1]
	}
}

type Source struct {
	wg             sync.WaitGroup
	inner          iofs.FS
	casStore       *CASStore
	sriAlgorithm   sri.Algorithm
	nodeAttributes func(iofs.FileInfo) api.NodeAttributes
	stripPrefix    string
	nodes          chan next
	stop           chan struct{}
}

func NewSource(inner iofs.FS, opts Options) (api.Source, closeWaitFunc) {
	opts.applyDefaults()
	source := &Source{
		inner:          inner,
		casStore:       NewCASStore(),
		sriAlgorithm:   opts.SRIAlgorithm,
		nodeAttributes: opts.NodeAttributes,
		stripPrefix:    opts.StripPrefix,
		nodes:          make(chan next),
		stop:           make(chan struct{}, 1),
	}
	source.wg.Add(1)
	go source.walk()
	return source, func() {
		source.stop <- struct{}{}
		source.wg.Wait()
	}
}

func (s *Source) Next() (api.SourceNode, error) {
	next, ok := <-s.nodes
	if !ok {
		return api.SourceNode{}, io.EOF
	}

	return next.Node, next.Err
}

func (s *Source) Open(sri string) (io.ReadCloser, error) {
	path, ok := s.casStore.Get(sri)
	if !ok {
		return nil, fs.ErrNotExist
	}
	return s.inner.Open(path)
}

func (s *Source) walk() {
	defer s.wg.Done()
	defer close(s.nodes)

	err := iofs.WalkDir(s.inner, ".", func(path string, d iofs.DirEntry, err error) error {
		next := s.prepareNext(path, d, err)

		select {
		case <-s.stop:
			return io.EOF
		case s.nodes <- next:
		}

		return nil
	})

	if err != nil {
		s.nodes <- next{Err: err}
	}
}

func (s *Source) prepareNext(path string, d iofs.DirEntry, err error) next {
	if err != nil {
		return next{Err: err}
	}

	var stat iofs.FileInfo
	if lstatFS, ok := s.inner.(lstatFS); ok {
		stat, err = lstatFS.Lstat(path)
	} else {
		stat, err = d.Info()
	}

	if err != nil {
		return next{Err: err}
	}

	kind := kind.FromMode(stat.Mode())

	payload, err := s.payload(path, kind)
	if err != nil {
		return next{Err: err}
	}

	if kind == api.KindRegular {
		s.addToCAS(payload, path)
	}

	node := api.SourceNode{
		Stat: api.Stat{
			Name:       normalizePath(path, s.stripPrefix),
			Size:       stat.Size(),
			Kind:       kind,
			Attributes: s.nodeAttributes(stat),
			Payload:    payload,
		},
		Open: func() (io.ReadCloser, error) {
			return s.inner.Open(path)
		},
	}

	return next{Node: node}
}

func (s *Source) payload(path, kind string) (string, error) {
	switch kind {
	case api.KindSymlink:
		symlinkFS, ok := s.inner.(readLinkRawFS)
		if !ok {
			return "", errors.New("fs does not support readlink")
		}
		target, err := symlinkFS.ReadLinkRaw(path)
		if err != nil {
			return "", err
		}
		return normalizeSymlinkTarget(target, s.stripPrefix), nil
	case api.KindRegular:
		f, err := s.inner.Open(path)
		if err != nil {
			return "", err
		}
		defer f.Close()
		integrity, err := sri.FromReader(s.sriAlgorithm, f)
		if err != nil {
			return "", err
		}
		return integrity.String(), nil
	}
	return "", nil
}

func (s *Source) addToCAS(sri, path string) {
	s.casStore.Set(sri, path)
}

type next struct {
	Node api.SourceNode
	Err  error
}

type closeWaitFunc func()

func normalizePath(path, stripPrefix string) string {
	if path == "." {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if strings.HasPrefix(path, stripPrefix) {
		return path[len(stripPrefix):]
	}
	return path
}

func normalizeSymlinkTarget(target, stripPrefix string) string {
	// for symlinks, we need to preserve relative paths
	if !strings.HasPrefix(target, "/") {
		return target
	}
	// strip prefix from absolute paths
	if strings.HasPrefix(target, stripPrefix) {
		return target[len(stripPrefix):]
	}
	return target
}

func defaultNodeAttributes(stat iofs.FileInfo) api.NodeAttributes {
	return api.NodeAttributes{
		Mtime: stat.ModTime().UTC(),
		Mode:  "0o" + strconv.FormatInt(int64(stat.Mode()), 8),
	}
}

type readLinkRawFS interface {
	iofs.FS
	// ReadLinkRaw returns the destination of the named symbolic link.
	// It allows for absolute and relative symlinks.
	// The target name may point anywhere and is not guaranteed to be normalized or even
	// to exist.
	ReadLinkRaw(name string) (string, error)
}

type lstatFS interface {
	iofs.FS
	Lstat(name string) (iofs.FileInfo, error)
}
