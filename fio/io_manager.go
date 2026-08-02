package fio

const DATA_FILE_PERM = 0644

type FileIOType = byte

const (
	STANDARD_FIO FileIOType = iota
	MMAP
)

type IOManager interface {
	// Read 从文件给定位置读取对应数据
	Read([]byte, int64) (int, error)

	// Write 写入字节到文件中
	Write([]byte) (int, error)

	Sync() error
	Close() error
	Size() (int64, error)
}

func NewIOManager(name string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case STANDARD_FIO:
		return NewFileIO(name)
	case MMAP:
		return NewMMap(name)
	}
	return nil, nil
}
