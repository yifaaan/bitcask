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
		// 是否在每次写入后进行同步
		bool syncOnWrite = false;
		IndexType indexType = IndexType::BTree;
	};

	class DB
	{
	public:
		// 打开或创建数据库
		// 1. 扫描并打开数据目录下的所有数据文件
		// 2. 加载索引到内存
		static absl::StatusOr<std::unique_ptr<DB>> Open(const Options& options);

		absl::Status Put(std::string_view key, std::string_view value);
		absl::StatusOr<std::string> Get(std::string_view key);
		absl::Status Delete(std::string_view key);

	private:
		explicit DB(Options options);

		// 1. 扫描并打开数据目录下的所有数据文件
		// 2. 加载索引到内存
		absl::Status Initialize();
		absl::StatusOr<LogRecordPos> AppendLogRecord(const LogRecord& record);
		
		// 设置当前活跃数据文件
		// 需要加锁然后再调用
		absl::Status SetActiveFile();

		// 从磁盘加载数据文件
		absl::Status LoadDataFiles();
		// 从数据文件加载索引到内存
		absl::Status LoadIndexFromDataFiles();

		Options options;
		mutable std::shared_mutex mutex;
		// 内存索引
		std::unique_ptr<Index> index;
		// 当前活跃数据文件，用于写入
		std::unique_ptr<DataFile> activeFile;
		// 旧数据文件，只读
		std::map<uint32_t, std::unique_ptr<DataFile>> olderFiles;
	};

} // namespace bitcask
