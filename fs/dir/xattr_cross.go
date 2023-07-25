//go:build !unix

package dir

import (
	"errors"

	"golang.org/x/sys/unix"
)

// Flistxattr would return the list of extended attribute names of the file.
// This is not supported on non-unix systems.
func Flistxattr(_ any) ([]string, error) {
	return nil, nil
}

// Fgetxattr would return the extended attribute value for a given name.
// This is not supported on non-unix systems.
func Fgetxattr(_ any, _ string) ([]byte, error) {
	return nil, errors.New("xattr not supported")
}
