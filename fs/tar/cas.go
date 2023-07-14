package tar

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"sync"

	"github.com/malt3/abstractfs-core/api"
	"github.com/malt3/abstractfs-core/sri"
)

func NewCAS(r io.Reader) casStore {
	randomReader, ok := r.(randomAccessReader)
	if !ok {
		panic("should use fallback cas store with in-memory cas backend once implemented")
	}
	return &CASSectionStore{
		reader: randomReader,
		inner:  make(map[string]struct{ offset, size int64 }),
	}

}

// CASSectionStore is a store for tar sections.
// It spies on the original reader and stores the offset and size of the section.
// While reading the tar file.
// Later, the sections can be opened by their sri.
type CASSectionStore struct {
	reader randomAccessReader
	mux    sync.RWMutex
	// inner is the lookup table for sri -> section of tar file (offset + size).
	inner map[string]struct{ offset, size int64 }
}

// Record records the given file and returns the sri.
// The headerSize is the file size reported by the tar header.
func (c *CASSectionStore) Record(fileReader io.Reader, headerSize int64, sriAlgorithm sri.Algorithm) (string, error) {
	// spy on the current offset of the reader
	offsetBefore, err := c.reader.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", fmt.Errorf("recording: failed to get file offset of reader: %w", err)
	}
	integrity, err := sri.FromReader(sriAlgorithm, fileReader)
	if err != nil {
		return "", fmt.Errorf("recording: failed to calculate sri: %w", err)
	}
	sri := integrity.String()
	offsetAfter, err := c.reader.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", fmt.Errorf("recording: failed to get file offset of reader: %w", err)
	}
	observedSize := offsetAfter - offsetBefore
	if offsetAfter < offsetBefore || observedSize != headerSize {
		return "", fmt.Errorf("recording: reader is not a random access reader, header size does not match real size or reading sparse file")
	}
	c.Set(sri, offsetBefore, observedSize)
	return sri, nil
}

// Open returns a reader for the given sri.
func (c *CASSectionStore) Open(sri string) (io.ReadCloser, error) {
	offset, size, ok := c.getSection(sri)
	if !ok {
		return nil, fs.ErrNotExist
	}
	return &nopCloser{io.NewSectionReader(c.reader, offset, size)}, nil
}

// Set sets the offset and size for the given sri.
// If the sri already exists, the old location will be kept.
func (c *CASSectionStore) Set(sri string, offset, size int64) {
	c.mux.Lock()
	defer c.mux.Unlock()
	_, ok := c.inner[sri]
	if ok {
		return
	}
	c.inner[sri] = struct{ offset, size int64 }{offset, size}
}

// getSection returns the tar section (offset, size) for the given sri.
func (c *CASSectionStore) getSection(sri string) (int64, int64, bool) {
	c.mux.RLock()
	defer c.mux.RUnlock()
	section, ok := c.inner[sri]
	return section.offset, section.size, ok
}

type fallbackCASStore struct {
	cas api.CAS
}

func (f *fallbackCASStore) Open(sri string) (io.ReadCloser, error) {
	return f.cas.Open(sri)
}

func (f *fallbackCASStore) Record(fileReader io.Reader, headerSize int64, sriAlgorithm sri.Algorithm) (string, error) {
	// since it is not guaranteed that we can rewind the reader, we need to copy the file
	// into a buffer in order to read it twice.
	// first read: calculate sri
	// second read: write to cas
	buf := new(bytes.Buffer)
	buf.Grow(int(headerSize))
	observedSize, err := io.Copy(buf, fileReader)
	if err != nil {
		return "", fmt.Errorf("recording: failed to copy file: %w", err)
	}
	if observedSize != headerSize {
		return "", fmt.Errorf("recording: header size does not match real size or reading sparse file")
	}
	integrity, err := sri.FromReader(sriAlgorithm, buf)
	if err != nil {
		return "", fmt.Errorf("recording: failed to calculate sri: %w", err)
	}
	sri := integrity.String()
	buf.Reset()
	if err := f.cas.Write(sri, buf); err != nil {
		return "", fmt.Errorf("recording: failed to write to cas: %w", err)
	}
	return sri, nil
}

type randomAccessReader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

type randomAccessReadCloser interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}

type nopCloser struct {
	randomAccessReader
}

func (n *nopCloser) Close() error { return nil }

type casStore interface {
	Record(fileReader io.Reader, headerSize int64, sriAlgorithm sri.Algorithm) (string, error)
	Open(sri string) (io.ReadCloser, error)
}

var _ casStore = (*CASSectionStore)(nil)
var _ casStore = (*fallbackCASStore)(nil)
