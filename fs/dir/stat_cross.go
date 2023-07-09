//go:build !unix

package dir

import (
	"io/fs"
)

func userID(info fs.FileInfo) string {
	return ""
}

func groupID(info fs.FileInfo) string {
	return ""
}

func userName(info fs.FileInfo) (string, error) {
	return "", nil
}

func groupName(info fs.FileInfo) (string, error) {
	return "", nil
}
