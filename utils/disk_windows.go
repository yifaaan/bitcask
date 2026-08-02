//go:build windows

package utils

import (
	"os"

	"golang.org/x/sys/windows"
)

// AvailableDiskSize returns the bytes available to the current user on the
// volume containing the current working directory.
func AvailableDiskSize() (uint64, error) {
	wd, err := os.Getwd()
	if err != nil {
		return 0, err
	}

	path, err := windows.UTF16PtrFromString(wd)
	if err != nil {
		return 0, err
	}

	var available uint64
	if err := windows.GetDiskFreeSpaceEx(path, &available, nil, nil); err != nil {
		return 0, err
	}

	return available, nil
}
