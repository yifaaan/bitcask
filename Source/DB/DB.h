#pragma once

#include "Data/DataFile.h"
#include "DB/WriteBatch.h"
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

	constexpr std::string_view MergeDirSuffix = "_merge";
	constexpr std::string_view MergeFinishedKey = "merge_finished";
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

		// 创建一个批量写事务;同一 DB 上的多个 WriteBatch 串行提交
      	std::unique_ptr<WriteBatch> NewWriteBatch(const WriteBatchOptions& options = {});

		std::unique_ptr<Iterator> NewIterator(const IteratorOptions& options);
		std::vector<std::string> ListKeys();
		absl::Status Fold(const std::function<bool(std::string_view key, std::string_view value)>& fn);

		// 将活跃数据文件同步到磁盘
		absl::Status Sync();
		// 关闭数据库，同步并关闭所有数据文件；关闭后其余方法均返回错误。可重复调用。
		absl::Status Close();

		absl::Status Merge();
	private:
		friend class WriteBatch;
		explicit DB(Options options);

		// 1. 扫描并打开数据目录下的所有数据文件
		// 2. 加载索引到内存
		absl::Status Initialize();

		// 将 LogRecord 追加到当前活跃数据文件中
		// 需要加锁然后再调用
		absl::StatusOr<LogRecordPos> AppendLogRecordWithLock(const LogRecord& record);
		absl::StatusOr<LogRecordPos> AppendLogRecord(const LogRecord& record);
		
		// 设置当前活跃数据文件
		// 需要加锁然后再调用
		absl::Status SetActiveFile();

		// 从磁盘加载数据文件
		absl::Status LoadDataFiles();
		// 根据 LogRecordPos 读取对应的 value
		absl::StatusOr<std::string> ReadValueFromPos(const LogRecordPos& pos) const;

		// 从数据文件加载索引到内存
		absl::Status LoadIndexFromDataFiles();

		std::string GetMergePath() const;

		absl::Status LoadMergeFiles();

		absl::StatusOr<uint32_t> GetNonMergedFid();

		// 从hint文件加载索引到内存
		absl::Status LoadIndexFromHintFile();

		Options options;
		mutable std::shared_mutex mutex;
		// 内存索引
		std::unique_ptr<Index> index;
		// 当前活跃数据文件，用于写入
		std::unique_ptr<DataFile> activeFile;
		// 旧数据文件，只读
		std::map<uint32_t, std::unique_ptr<DataFile>> olderFiles;
		// Close() 之后置为 true，后续读写操作均返回错误
		bool closed = false;
		// 当前最新的事务序列号, 全局递增
		std::atomic_uint64_t currentSeqNum = 0;
		// 是否正在进行 Merge 操作
		std::atomic_bool merging = false;
	};

} // namespace bitcask
