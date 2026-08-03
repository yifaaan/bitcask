package redis

import (
	"encoding/binary"
	"math"
)

const (
	maxMetadataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetaSize = binary.MaxVarintLen64 * 2

	initialListMark = math.MaxUint64 / 2
)

type metadata struct {
	dataType byte   // 类型: LIST HASH SET ZSET
	expire   int64  // 过期时间
	version  int64  // 版本号：用于删除
	size     uint32 // 数据个数
	head     uint64 // LIST 专用
	tail     uint64 // LIST 专用
}

func (md *metadata) encode() []byte {
	var size = maxMetadataSize
	if md.dataType == LIST {
		size += extraListMetaSize
	}
	buf := make([]byte, size)
	buf[0] = md.dataType
	var idx = 1
	idx += binary.PutVarint(buf[idx:], md.expire)
	idx += binary.PutVarint(buf[idx:], md.version)
	idx += binary.PutVarint(buf[idx:], int64(md.size))

	if md.dataType == LIST {
		idx += binary.PutUvarint(buf[idx:], md.head)
		idx += binary.PutUvarint(buf[idx:], md.tail)
	}
	return buf[:idx]
}

func decodeMetadata(buf []byte) *metadata {
	dataType := buf[0]

	var idx = 1
	expire, n := binary.Varint(buf[idx:])
	idx += n
	version, n := binary.Varint(buf[idx:])
	idx += n
	size, n := binary.Varint(buf[idx:])
	idx += n

	var head uint64 = 0
	var tail uint64 = 0
	if dataType == LIST {
		head, n = binary.Uvarint(buf[idx:])
		idx += n
		tail, _ = binary.Uvarint(buf[idx:])
	}

	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}
