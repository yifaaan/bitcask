#pragma once

#include "FIO/IOManager.h"
#include "LogRecord.h"

#include <absl/status/status.h>
#include <absl/status/statusor.h>

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>

namespace bitcask
{

	struct DataFile
	{
		uint32_t fid = 0;
		int64_t writeOffset = 0;
		std::unique_ptr<IOManager> io;

		static absl::StatusOr<std::unique_ptr<DataFile>> Open(std::string_view dirPath, uint32_t fid, IOType ioType);
		// Open the hint file for the given fid. The hint file is used to store the index of the data file.
		static absl::StatusOr<std::unique_ptr<DataFile>> OpenHint(std::string_view dirPath, uint32_t fid);
		// Open the merge finished file for the given fid. The merge finished file is used to indicate that the merge operation has completed.
		static absl::StatusOr<std::unique_ptr<DataFile>> OpenMergeFinishedFile(std::string_view dirPath);

		absl::StatusOr<int64_t> Write(std::span<const std::byte> data);
		// Returns {record_size, record}. The caller uses record_size to advance the scan offset.
		absl::StatusOr<std::pair<int64_t, LogRecord>> ReadLogRecord(int64_t offset) const;
		absl::Status Sync();
		absl::Status Close();

		absl::Status WriteHintRecord(std::string_view key, const LogRecordPos& pos);
		// 读取并解码 offset 处的一条 hint 记录; 返回 {记录总长度, HintRecord}。
		absl::StatusOr<std::pair<int64_t, HintRecord>> ReadHintRecord(int64_t offset) const;
	};

	constexpr std::string_view DataFileNameSuffix = ".data";
	constexpr std::string_view HintFileName = "hint-index";
	constexpr std::string_view MergeFinishedFileName = "merge-finished";
	std::string DataFileName(uint32_t fid);

} // namespace bitcask
