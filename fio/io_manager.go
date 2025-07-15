package fio

const DataFilePerm = 0644

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
}
