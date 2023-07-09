package tar

import (
	archivetar "archive/tar"
	"errors"
	"io"
	"io/fs"
	stdpath "path"
	"strconv"
	"strings"

	"github.com/malt3/abstractfs-core/api"
)

type Sink struct {
	writer         Writer
	format         archivetar.Format
	root           string
	xattrPaxPrefix string
}

func (s *Sink) Consume(in fs.FS) error {
	return fs.WalkDir(in, ".", func(path string, d fs.DirEntry, err error) error {
		isRoot := path == "." || path == "/"
		if isRoot && s.root == "" {
			// skip root
			return nil
		}
		header, err := s.prepareHeader(path, d)
		if err != nil {
			return err
		}
		if err := s.writer.WriteHeader(header); err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		file, err := in.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		if _, err := io.Copy(s.writer, file); err != nil {
			return err
		}
		return nil
	})
}

func (s *Sink) prepareHeader(path string, d fs.DirEntry) (*archivetar.Header, error) {
	// if the dirEntry comes from a api.Tree, get the api.Stat from it
	// otherwise, use only the subset that is available in fs.FileInfo
	info, err := d.Info()
	if err != nil {
		return nil, err
	}
	if stat, hasStat := info.Sys().(api.Stat); hasStat {
		return s.prepareHeaderFromStat(path, stat)
	}
	return s.prepareHeaderFromFileInfo(path, d, info)
}

func (s *Sink) prepareHeaderFromStat(path string, stat api.Stat) (*archivetar.Header, error) {
	name := s.name(path, stat.Kind == api.KindDirectory)
	// TODO: reroot target of symlink
	// if needed
	var linkname string
	if stat.Kind == api.KindSymlink {
		linkname = stat.Payload
	}

	var mode int64
	if len(stat.Attributes.Mode) > 0 {
		var err error
		mode, err = strconv.ParseInt(stat.Attributes.Mode, 0, 64)
		if err != nil {
			return nil, err
		}
	}

	var uid int
	if len(stat.Attributes.UserID) > 0 {
		uid64, err := strconv.ParseInt(stat.Attributes.UserID, 0, 64)
		if err != nil {
			return nil, err
		}
		uid = int(uid64)
	}

	var gid int
	if len(stat.Attributes.GroupID) > 0 {
		gid64, err := strconv.ParseInt(stat.Attributes.GroupID, 0, 64)
		if err != nil {
			return nil, err
		}
		gid = int(gid64)
	}

	var paxRecords map[string]string
	if len(stat.Attributes.XAttrs) > 0 {
		paxRecords = make(map[string]string, len(stat.Attributes.XAttrs))
		for key, value := range stat.Attributes.XAttrs {
			paxRecords[s.xattrPaxPrefix+key] = value
		}
	}

	return &archivetar.Header{
		Typeflag:   tarTypeFromKind(stat.Kind),
		Name:       name,
		Linkname:   linkname,
		Size:       stat.Size,
		Mode:       mode,
		Uid:        uid,
		Gid:        gid,
		Uname:      stat.Attributes.UserName,
		Gname:      stat.Attributes.GroupName,
		ModTime:    stat.Attributes.Mtime,
		PAXRecords: paxRecords,
		Format:     s.format,
	}, nil
}

func (s *Sink) prepareHeaderFromFileInfo(path string, d fs.DirEntry, info fs.FileInfo) (*archivetar.Header, error) {
	name := s.name(path, info.IsDir())
	var link string
	if d.Type()&fs.ModeSymlink != 0 {
		readLinkFS, ok := d.(readLinkFS)
		if !ok {
			return nil, errors.New("symlink given but fs does not implement readLinkFS")
		}
		var err error
		link, err = readLinkFS.Readlink(path)
		if err != nil {
			return nil, err
		}
	}
	header, err := archivetar.FileInfoHeader(info, link)
	if err != nil {
		return nil, err
	}
	header.Name = name
	header.Format = s.format
	return header, nil
}

func (s *Sink) name(path string, isDir bool) string {
	// TODO: handle root correctly
	name := path
	if len(s.root) > 0 {
		name = stdpath.Join(s.root, name)
	}
	if isDir && !strings.HasSuffix(name, "/") {
		name += "/"
	}
	return name
}

func newDefaultWriter(w io.Writer) Writer {
	return archivetar.NewWriter(w)
}

func tarTypeFromKind(kind string) byte {
	// TODO: handle other kinds
	switch kind {
	case api.KindDirectory:
		return archivetar.TypeDir
	case api.KindSymlink:
		return archivetar.TypeSymlink
	case api.KindRegular:
		return archivetar.TypeReg
	}
	return 0
}

type readLinkFS interface {
	fs.FS
	Readlink(string) (string, error)
}
