package fio

const DataFilePerm = 0644

type FileIOType = byte

const (
	StandardFileIO FileIOType = iota
	MemoryMap
)

// IO 管理器接口，目前支持文件IO
type IOManager interface {
	// 从文件的指定位置读取数据
	Read([]byte, int64) (int, error)

	// 写入字节数组
	Write([]byte) (int, error)

	// 持久化到磁盘
	Sync() error

	// 关闭
	Close() error

	// 获取文件大小
	Size() (int64, error)
}

func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFileIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("invalid io type")
	}
}
