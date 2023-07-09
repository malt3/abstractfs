//go:build unix

package dir

import (
	"io/fs"
	osuser "os/user"
	"strconv"
	"syscall"
)

func userID(info fs.FileInfo) string {
	uid := info.Sys().(*syscall.Stat_t).Uid
	return strconv.Itoa(int(uid))
}

func groupID(info fs.FileInfo) string {
	gid := info.Sys().(*syscall.Stat_t).Gid
	return strconv.Itoa(int(gid))
}

func userName(info fs.FileInfo) (string, error) {
	user, err := osuser.LookupId(userID(info))
	if _, ok := err.(osuser.UnknownUserIdError); ok {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return user.Username, nil
}

func groupName(info fs.FileInfo) (string, error) {
	group, err := osuser.LookupGroupId(groupID(info))
	if _, ok := err.(osuser.UnknownGroupIdError); ok {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return group.Name, nil
}
