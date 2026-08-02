//go:build linux

package utils

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func AvailableDiskSize() (uint64, error) {
	wd, err := os.Getwd()
	if err != nil {
		return 0, err
	}

	var stat unix.Statfs_t
	if err := unix.Statfs(wd, &stat); err != nil {
		return 0, err
	}
	if stat.Bsize <= 0 {
		return 0, fmt.Errorf("invalid filesystem block size: %d", stat.Bsize)
	}

	return stat.Bavail * uint64(stat.Bsize), nil
}
