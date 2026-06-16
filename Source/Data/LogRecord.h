#pragma once

#include "Core/Varint.h"

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
		kNormal = 0,
		kDeleted = 1,
		kTxnFinished = 2,
	};

	struct LogRecord
	{
		std::string key;
		std::string value;
		LogRecordType type = LogRecordType::kNormal;
	};

	struct LogRecordPos
	{
		uint32_t fid = 0;
		int64_t offset = 0;
		int64_t size = 0;
	};

	struct LogRecordHeader
	{
		uint32_t crc = 0;
		LogRecordType type = LogRecordType::kNormal;
		int64_t keySize = 0;
		int64_t valueSize = 0;
	};

	constexpr size_t kMaxLogRecordHeaderSize = 1 + 2 * kMaxVarintLength;

	std::pair<std::vector<std::byte>, int64_t> EncodeLogRecord(const LogRecord& record);
	std::pair<std::optional<LogRecordHeader>, int64_t> DecodeLogRecordHeader(std::span<const std::byte> buf);
	uint32_t CalcLogRecordCRC(const LogRecord& record, std::span<const std::byte> headerWithoutCRC);
	std::pair<std::vector<std::byte>, int64_t> EncodeLogRecordPos(const LogRecordPos& pos);
	std::pair<std::optional<LogRecordPos>, int64_t> DecodeLogRecordPos(std::span<const std::byte> buf);

} // namespace bitcask
