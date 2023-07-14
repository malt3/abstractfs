package dir

import (
	"io"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/malt3/abstractfs-core/api"
	"github.com/malt3/abstractfs-core/kind"
	"github.com/malt3/abstractfs-core/sri"
	"github.com/malt3/abstractfs/fs/generic"
)

type Source struct {
	wg           sync.WaitGroup
	dir          string
	casStore     *generic.CASStore
	sriAlgorithm sri.Algorithm
	keepPrefix   bool
	nodes        chan next
	stop         chan struct{}
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
	return os.Open(path)
}

func (s *Source) walk() {
	defer s.wg.Done()
	defer close(s.nodes)

	relativeDir := s.dir
	root := "."
	if strings.HasPrefix(relativeDir, "/") {
		relativeDir = relativeDir[1:]
		root = "/"
	}
	err := fs.WalkDir(os.DirFS(root), relativeDir, func(path string, d fs.DirEntry, err error) error {
		next := s.prepareNext(path, err)

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

func (s *Source) prepareNext(path string, err error) next {
	if strings.HasPrefix(s.dir, "/") && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if err != nil {
		return next{Err: err}
	}

	stat, err := os.Lstat(path)
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

	attributes, err := s.nodeAttributes(path, stat)
	if err != nil {
		return next{Err: err}
	}

	node := api.SourceNode{
		Stat: api.Stat{
			Name:       normalizePath(path, s.dir, s.keepPrefix),
			Kind:       kind,
			Attributes: attributes,
			Payload:    payload,
			Size:       stat.Size(),
		},
		Open: func() (io.ReadCloser, error) {
			return os.Open(path)
		},
	}

	return next{Node: node}
}

func (s *Source) payload(path, kind string) (string, error) {
	switch kind {
	case api.KindSymlink:
		target, err := os.Readlink(path)
		if err != nil {
			return "", err
		}
		return normalizeSymlinkTarget(target, s.dir, s.keepPrefix), nil
	case api.KindRegular:
		f, err := os.Open(path)
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

func (s *Source) nodeAttributes(path string, stat fs.FileInfo) (api.NodeAttributes, error) {
	// important: also implement OS specific xattrs (at least linux)
	mtime := stat.ModTime().UTC()
	userID := userID(stat)
	groupID := groupID(stat)
	userName, err := userName(stat)
	if err != nil {
		return api.NodeAttributes{}, err
	}
	groupName, err := groupName(stat)
	if err != nil {
		return api.NodeAttributes{}, err
	}
	mode := "0o" + strconv.FormatInt(int64(stat.Mode().Perm()), 8)
	// TODO: implement xattrs
	return api.NodeAttributes{
		Mtime:     mtime,
		UserID:    userID,
		GroupID:   groupID,
		UserName:  userName,
		GroupName: groupName,
		Mode:      mode,
	}, nil
}

func (s *Source) addToCAS(sri, path string) {
	s.casStore.Set(sri, path)
}

type next struct {
	Node api.SourceNode
	Err  error
}

type closeWaitFunc func()

func normalizePath(path, dir string, keepPrefix bool) string {
	if path == "." {
		return "/"
	}
	if !strings.HasSuffix(dir, "/") {
		dir += "/"
	}
	if !keepPrefix && strings.HasPrefix(path, dir) {
		path = path[len(dir):]
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path
}

func normalizeSymlinkTarget(target, dir string, keepPrefix bool) string {
	// for symlinks, we need to preserve relative paths
	if !strings.HasPrefix(target, "/") {
		return target
	}
	if !strings.HasSuffix(dir, "/") {
		dir += "/"
	}
	// strip prefix from absolute paths
	if !keepPrefix && strings.HasPrefix(target, dir) {
		return target[len(dir):]
	}
	return target
}

var (
	_ api.Source    = (*Source)(nil)
	_ api.CASReader = (*Source)(nil)
)
