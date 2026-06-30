#pragma once

#include "Data/DataFile.h"
#include "Index/Index.h"

#include <cstdint>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>

#include <absl/status/status.h>
#include <absl/status/statusor.h>

namespace bitcask
{

	struct Options
	{
		// 数据目录
		std::string dataDir = "./bitcask_data";
		uint64_t maxDataFileSize = 10 * 1024 * 1024;
		bool syncOnWrite = false;
		uint64_t bytesPerSync = 0;
		IndexType indexType = IndexType::BTree;
	};

	class DB
	{
	public:
		static absl::StatusOr<std::unique_ptr<DB>> Open(const Options& options);

		absl::Status Put(std::string_view key, std::string_view value);

	private:
		explicit DB(Options options);

		absl::Status Initialize();
		absl::StatusOr<LogRecordPos> AppendLogRecord(const LogRecord& record);
		
		// 设置当前活跃数据文件
		// 需要加锁然后再调用
		absl::Status SetActiveFile();

		Options options;
		mutable std::shared_mutex mutex;
		std::unique_ptr<Index> index;
		// 当前活跃数据文件，用于写入
		std::unique_ptr<DataFile> activeFile;
		uint32_t activeFid;
		// 旧数据文件，只读
		std::map<uint32_t, std::unique_ptr<DataFile>> olderFiles;
	};

} // namespace bitcask
