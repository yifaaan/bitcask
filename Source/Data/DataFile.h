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

		static absl::StatusOr<std::unique_ptr<DataFile>> Open(std::string_view dirPath, uint32_t fid, IOType ioType);

		absl::StatusOr<int64_t> Write(std::span<const std::byte> data);
		// Returns {record_size, record}. The caller uses record_size to advance the scan offset.
		absl::StatusOr<std::pair<int64_t, LogRecord>> ReadLogRecord(int64_t offset) const;
		absl::Status Sync();
		absl::Status Close();
	};

	constexpr std::string_view DataFileNameSuffix = ".data";
	std::string DataFileName(uint32_t fid);

} // namespace bitcask
