package tar

import (
	"io"
	"strconv"
	"strings"

	archivetar "archive/tar"

	"github.com/malt3/abstractfs-core/api"
	"github.com/malt3/abstractfs-core/sri"
)

type Source struct {
	reader           Reader
	sriAlgorithm     sri.Algorithm
	xattrPaxPrefixes []string
	// idea: implement a tar CAS backend by storing the file offsets in a map
	// use something like tell to get the offset while streaming the tar
	// then if a CAS object is requested, use ReaderAt (if possible) to read the file
	// by returning a SectionReader.
	// alternatively, seek to the offset and read the file if ReaderAt is not available.
	// The second option is must be guarded by a mutex since multiple goroutines might
	// request CAS objects at the same time and seek around.
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
		// TODO: maybe support Open()
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
	// TODO: if possible, record the file offset before reading the file
	// and save it in the CAS store
	integrity, err := sri.FromReader(s.sriAlgorithm, s.reader)
	if err != nil {
		return "", err
	}
	return integrity.String(), nil
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
