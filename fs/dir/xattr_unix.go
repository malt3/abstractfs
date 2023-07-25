//go:build unix

package dir

import (
	"errors"

	"golang.org/x/sys/unix"
)

func Flistxattr(file Fd) ([]string, error) {
	fd := int(file.Fd())

	// find size
	size, err := unix.Flistxattr(fd, nil)
	if err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, errors.New("negative size returned by listxattr")
	}
	if size > maxBufSize {
		return nil, errors.New("size returned by listxattr is too large")
	}

	dest := make([]byte, size)
	sizeRead, err := unix.Flistxattr(fd, dest)
	if err != nil {
		return nil, err
	}
	if sizeRead > size || sizeRead < 0 {
		return nil, errors.New("invalid size returned by listxattr")
	}
	return stringsFromByteSlice(dest[:sizeRead]), nil
}

func Fgetxattr(file Fd, attr string) ([]byte, error) {
	fd := int(file.Fd())

	// find size
	size, err := unix.Fgetxattr(fd, attr, nil)
	if err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, errors.New("negative size returned by listxattr")
	}
	if size > maxBufSize {
		return nil, errors.New("size returned by listxattr is too large")
	}

	dest := make([]byte, size)
	sizeRead, err := unix.Fgetxattr(fd, attr, dest)
	if err != nil {
		return nil, err
	}
	if sizeRead > size || sizeRead < 0 {
		return nil, errors.New("invalid size returned by listxattr")
	}
	return dest[:sizeRead], nil
}

func stringsFromByteSlice(buf []byte) []string {
	var result []string
	off := 0
	for i, b := range buf {
		if b == 0 {
			result = append(result, string(buf[off:i]))
			off = i + 1
		}
	}
	return result
}

type Fd interface {
	Fd() uintptr
}

const maxBufSize = 128 * 1024 * 1024 // 128 MiB
