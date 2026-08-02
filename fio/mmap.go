package fio

import "golang.org/x/exp/mmap"

type MMap struct {
	readerAt *mmap.ReaderAt
}

func NewMMap(name string) (*MMap, error) {
	readerAt, err := mmap.Open(name)
	if err != nil {
		return nil, err
	}
	return &MMap{
		readerAt: readerAt,
	}, nil
}

func (mmap *MMap) Read(b []byte, off int64) (int, error) {
	return mmap.readerAt.ReadAt(b, off)
}

// Write 写入字节到文件中
func (mmap *MMap) Write([]byte) (int, error) {
	panic("not implement!")
}

func (mmap *MMap) Sync() error {
	panic("not implement!")
}
func (mmap *MMap) Close() error {
	return mmap.readerAt.Close()
}
func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}
