package tar

import (
	"io"
	"io/fs"
	"strconv"
	"strings"

	archivetar "archive/tar"

	"github.com/malt3/abstractfs-core/api"
	"github.com/malt3/abstractfs-core/sri"
)

type Source struct {
	casStore
	reader           Reader
	sriAlgorithm     sri.Algorithm
	xattrPaxPrefixes []string
}

func (s *Source) Next() (api.SourceNode, error) {
	header, err := s.reader.Next()
	if err != nil {
		return api.SourceNode{}, err
	}
	return s.prepareNext(header)
}

func (s *Source) prepareNext(header *archivetar.Header) (api.SourceNode, error) {
	name := header.Name
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}
	kind := kindFromTarType(header.Typeflag)

	payload, err := s.payload(header, kind)
	if err != nil {
		return api.SourceNode{}, err
	}

	attributes := s.nodeAttributes(header)
	if err != nil {
		return api.SourceNode{}, err
	}

	node := api.SourceNode{
		Stat: api.Stat{
			Name:       name,
			Kind:       kind,
			Attributes: attributes,
			Payload:    payload,
			Size:       header.Size,
		},
		Open: s.openFunc(kind, payload),
	}

	return node, nil
}

func (s *Source) payload(header *archivetar.Header, kind string) (string, error) {
	switch kind {
	case api.KindSymlink:
		return header.Linkname, nil
	case api.KindRegular:
		// handled below
	default:
		return "", nil
	}
	integrity, err := s.Record(s.reader, header.Size, s.sriAlgorithm)
	if err != nil {
		return "", err
	}
	return integrity, nil
}

func (s *Source) nodeAttributes(header *archivetar.Header) api.NodeAttributes {
	mtime := header.ModTime.UTC()
	userID := strconv.Itoa(header.Uid)
	groupID := strconv.Itoa(header.Gid)
	userName := header.Uname
	groupName := header.Gname
	mode := "0o" + strconv.FormatInt(int64(header.Mode), 8)
	xattrs := make(map[string]string)
	// TODO: support different encodings
	// including libarchive xattrs
	for _, xattrPaxPrefix := range s.xattrPaxPrefixes {
		for key, value := range header.PAXRecords {
			// TODO: find out if null bytes should be preserved
			// if strings.HasSuffix(value, "\x00") {
			// 	value = value[:len(value)-1]
			// }
			if strings.HasPrefix(key, xattrPaxPrefix) {
				key = strings.TrimPrefix(key, xattrPaxPrefix)
				xattrs[key] = value
			}
		}
	}
	return api.NodeAttributes{
		Mtime:     mtime,
		UserID:    userID,
		GroupID:   groupID,
		UserName:  userName,
		GroupName: groupName,
		Mode:      mode,
		XAttrs:    xattrs,
	}
}

func (s *Source) openFunc(kind, payload string) func() (io.ReadCloser, error) {
	if kind != api.KindRegular {
		// TODO: check if this should return a valid dir entry instead
		return func() (io.ReadCloser, error) {
			return nil, fs.ErrNotExist
		}
	}
	return func() (io.ReadCloser, error) {
		return s.casStore.Open(payload)
	}
}

func newDefaultReader(r io.Reader) Reader {
	return archivetar.NewReader(r)
}

func kindFromTarType(tarType byte) string {
	// TODO: support other types
	switch tarType {
	case archivetar.TypeDir:
		return api.KindDirectory
	case archivetar.TypeReg:
		return api.KindRegular
	case archivetar.TypeSymlink:
		return api.KindSymlink
	}
	return ""
}

var (
	_ api.Source    = (*Source)(nil)
	_ api.CASReader = (*Source)(nil)
)
