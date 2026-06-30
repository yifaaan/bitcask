#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

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

	std::vector<std::byte> EncodeLogRecord(const LogRecord& record);
} // namespace bitcask
