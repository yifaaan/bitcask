#pragma once

#include "FIO/IOManager.h"
#include "LogRecord.h"

#include <absl/status/status.h>
#include <absl/status/statusor.h>

#include <cstdint>
#include <memory>
#include <span>
#include <string>

namespace bitcask
{

	struct DataFile
	{
		uint32_t fid = 0;
		int64_t writeOffset = 0;
		std::unique_ptr<IOManager> io;

		static absl::StatusOr<std::unique_ptr<DataFile>> Open(const std::string& dirPath, uint32_t fid, IOType ioType);

		absl::StatusOr<int64_t> Write(std::span<const std::byte> data);
		absl::StatusOr<LogRecord> ReadLogRecord(int64_t offset);
		absl::Status Sync();
		absl::StatusOr<int64_t> AppendHintRecord(const std::string& key, const LogRecordPos& pos);
	};

	constexpr std::string_view DataFileNameSuffix = ".data";
	std::string DataFileName(uint32_t fid);

} // namespace bitcask
