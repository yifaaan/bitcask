package utils

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

func Backup(srcDir, dstDir string, excludeDataFiles []string) (err error) {
	if _, err := os.Stat(dstDir); os.IsNotExist(err) {
		if err := os.MkdirAll(dstDir, os.ModePerm); err != nil {
			return err
		}
	}
	return filepath.Walk(srcDir, func(path string, info fs.FileInfo, err error) error {
		name := strings.Replace(path, srcDir, "", 1)
		if name == "" {
			return nil
		}

		for _, e := range excludeDataFiles {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}

		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dstDir, name), info.Mode())
		}

		data, err := os.ReadFile(filepath.Join(srcDir, name))
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dstDir, name), data, info.Mode())
	})
}
