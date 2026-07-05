#pragma once

#include "DB/WriteBatch.h"
#include "Data/DataFile.h"
#include "Data/LogRecord.h"
#include "Index/Index.h"


#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>

#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <boost/interprocess/sync/file_lock.hpp>

#include <string_view>
#include <vector>

namespace bitcask
{

	constexpr std::string_view MergeDirSuffix = "_merge";
	constexpr std::string_view MergeFinishedKey = "merge_finished";
	constexpr std::string_view FileLockName = "bitcask.lock";

	struct Options
	{
		// 数据目录
		std::string dataDir = "./bitcask_data";
		uint64_t maxDataFileSize = 10 * 1024 * 1024;
		// 是否在每次写入后进行同步
		bool syncOnWrite = false;
		// 累计写入多少字节后自动对活跃数据文件进行 Sync; 0 表示关闭该机制。
		// 仅在 syncOnWrite == false 时生效; syncOnWrite 优先级更高 (每次写都同步)。
		uint64_t bytesPerSync = 0;
		IndexType indexType = IndexType::BTree;
		// 可回收空间占比阈值 [0, 1]: 当 可回收字节 / 数据总大小 达到该比例时 Merge 才执行; 0 表示总是合并。
		double mergeThreshold = 0.0;
	};

	// 数据库运行时统计信息
	struct Stat
	{
		// 内存索引中的 key 数量
		size_t keyCount = 0;
		// 数据文件数量 (活跃文件 + 旧文件)
		size_t dataFileCount = 0;
		// 可被 Merge 回收的字节数 (Put 覆盖 / Delete 产生的旧记录)
		int64_t reclaimableSize = 0;
		// 数据文件占用的总大小 (各数据文件已写入字节数 writeOffset 之和, 逻辑大小)
		int64_t diskSize = 0;
		// 距离上次 Sync 累计的未同步字节数 (bytesPerSync 自动 sync / 手动 Sync / 旋转 sync 均会清零)
		int64_t bytesWrite = 0;
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

		// 返回数据库运行时统计信息 (key 数量、数据文件数量、可回收字节、磁盘占用)
		absl::StatusOr<Stat> GetStat() const;

		// 当前可被 Merge 回收的字节数: 每次 Put 覆盖 / Delete 旧 key 时累加旧记录大小, 启动时由数据文件回放重建。
		// 仅为粗略的统计计数, 读取用 relaxed 内存序。
		int64_t ReclaimSize() const noexcept
		{
			return reclaimSize.load(std::memory_order_relaxed);
		}

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
		// 合并过程中需要回收的空间大小
		std::atomic<int64_t> reclaimSize{0};
		// 距离上次 Sync 累计的未同步字节数; 由 bytesPerSync 阈值消费, 仅在写入路径变更
		std::atomic<int64_t> bytesWrite{0};
		// 文件锁，确保同一数据目录只能被一个进程使用
		std::unique_ptr<boost::interprocess::file_lock> flock;
	};

} // namespace bitcask
