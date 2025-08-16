package fio

import (
	"golang.org/x/exp/mmap"
)

// MMap加快文件读取速度，但是不支持写入
type MMap struct {
	m *mmap.ReaderAt
}

func NewMMapIOManager(name string) (*MMap, error) {
	m, err := mmap.Open(name)
	if err != nil {
		return nil, err
	}
	return &MMap{m: m}, nil
}

// 从文件的指定位置读取数据
func (m *MMap) Read(b []byte, offset int64) (int, error) {
	return m.m.ReadAt(b, offset)
}

// 写入字节数组,mmap不支持写入
func (m *MMap) Write([]byte) (int, error) {
	panic("Not implemented")
}

// 持久化到磁盘,mmap不支持持久化
func (m *MMap) Sync() error {
	panic("Not implemented")
}

// 关闭
func (m *MMap) Close() error {
	return m.m.Close()
}

// 获取文件大小
func (m *MMap) Size() (int64, error) {
	return int64(m.m.Len()), nil
}
