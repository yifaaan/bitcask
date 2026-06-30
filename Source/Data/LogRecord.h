#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "Varint.h"

namespace bitcask
{

	enum class LogRecordType : uint8_t
	{
		Normal = 0,
		Deleted = 1,
		TxnFinished = 2,
	};

	struct LogRecord
	{
		std::string key;
		std::string value;
		LogRecordType type = LogRecordType::Normal;
	};

	// 内存索引
	struct LogRecordPos
	{
		uint32_t fid = 0;
		int64_t offset = 0;
		int64_t size = 0;
	};

	struct LogRecordHeader
	{
		uint32_t crc = 0;
		LogRecordType type = LogRecordType::Normal;
		uint32_t keySize = 0;
		uint32_t valueSize = 0;
	};

	constexpr size_t MaxLogRecordHeaderSize = 4 + 1 + 2 * MaxVarintLen32; // CRC32 + type + keySize + valueSize
	std::vector<std::byte> EncodeLogRecord(const LogRecord& record);
	std::pair<std::optional<LogRecordHeader>, int64_t> DecodeLogRecordHeader(std::span<const std::byte> buf);
	uint32_t CalcLogRecordCRC(const LogRecord& record, const LogRecordHeader& header);

} // namespace bitcask
