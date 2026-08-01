#include "DB.h"

#include "DB/WriteBatch.h"
#include "Data/DataFile.h"
#include "Data/LogRecord.h"
#include "Data/Varint.h"
#include "FIO/IOManager.h"
#include "Index/Index.h"

#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <boost/interprocess/sync/file_lock.hpp>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>
#include <fstream>

namespace
{
	// 解析 LogRecord 的 key，提取事务序列号和原始 key
	std::pair<uint64_t, std::string> ParseLogRecordKey(std::string_view key)
	{
		auto buf = std::span<const std::byte>(reinterpret_cast<const std::byte*>(key.data()), key.size());
		auto [seqNum, nextIdx] = *bitcask::GetVarint(buf);
		return {seqNum, std::string(key.substr(nextIdx))};
	}

	absl::Status LoadIndexFromOneFile(bitcask::Index* index, const bitcask::DataFile& file, uint32_t fid, std::unordered_map<uint64_t, std::vector<bitcask::TransactionLogRecord>>& txnRecords, uint64_t& maxSeqNum, int64_t& reclaimSize, int64_t* writeOffset = nullptr)
	{
		auto updateIndex = [&](std::string_view key, bitcask::LogRecordPos pos, bitcask::LogRecordType type) -> absl::Status {
			if (type == bitcask::LogRecordType::Deleted)
			{
				if (auto oldPos = index->Delete(key); oldPos.has_value())
				{
					reclaimSize += oldPos->size;
				}
			}
			else
			{
				if (auto oldPos = index->Put(key, pos); oldPos.has_value())
				{
					reclaimSize += oldPos->size;
				}
			}
			return absl::OkStatus();
		};
		int64_t offset = 0;
		while (true)
		{
			auto recordOr = file.ReadLogRecord(offset);
			if (!recordOr.ok())
			{
				if (recordOr.status().code() == absl::StatusCode::kOutOfRange)
				{
					break;
				}
				return recordOr.status();
			}
			auto& [size, record] = *recordOr;
			auto pos = bitcask::LogRecordPos{
				.fid = file.fid,
				.offset = offset,
				.size = size,
			};

			// 解析读到的record 是否包含事务序列号
			auto [seqNum, originalKey] = ParseLogRecordKey(record.key);

			if (seqNum == bitcask::NoTxnSeqNum)
			{
				// 非事务写入，直接更新索引
				if (auto status = updateIndex(originalKey, pos, record.type); !status.ok())
				{
					return status;
				}
			}
			else
			{
				// 有事务完成的标记
				if (record.type == bitcask::LogRecordType::TxnFinished)
				{
					// 将该事务的所有记录更新到索引中
					for (auto& txnRecord : txnRecords[seqNum])
					{
						if (auto status = updateIndex(txnRecord.record.key, txnRecord.pos, txnRecord.record.type); !status.ok())
						{
							return status;
						}
					}
					// 删除该事务的记录
					txnRecords.erase(seqNum);
				}
				else
				{
					record.key = originalKey;
					// 暂存该事务的记录
					txnRecords[seqNum].push_back({record, pos});
				}
			}
			// 更新最大事务序列号
			maxSeqNum = std::max(maxSeqNum, seqNum);
			offset += size;
		}

		if (writeOffset)
		{
			*writeOffset = offset;
		}
		return absl::OkStatus();
	}
} // namespace

namespace bitcask
{

	DB::DB(Options options)
		: options(std::move(options)),
		  index(NewIndex(this->options.indexType))
	{
	}

	absl::StatusOr<std::unique_ptr<DB>> DB::Open(const Options& options)
	{
		auto db = std::unique_ptr<DB>(new DB(options));
		auto status = db->Initialize();
		if (!status.ok())
		{
			return status;
		}
		// 整合上一次未完成的 merge(若存在), 必须在加载数据文件之前完成
		if (auto status = db->LoadMergeFiles(); !status.ok())
		{
			return status;
		}

		// 加载数据文件
		if (auto status = db->LoadDataFiles(); !status.ok())
		{
			return status;
		}

		// 加载 hint 文件索引到内存
		if (auto status = db->LoadIndexFromHintFile(); !status.ok())
		{
			return status;
		}
		// 加载内存索引
		if (auto status = db->LoadIndexFromDataFiles(); !status.ok())
		{
			return status;
		}

		// 索引构建完成后，将启动阶段使用的只读 MmapIO 切换为可写 FileIO
		if (auto status = db->SwitchDataFilesToStandardIO(); !status.ok())
		{
			return status;
		}
		return db;
	}

	absl::Status DB::Initialize()
	{
		if (options.dataDir.empty())
		{
			return absl::InvalidArgumentError("dataDir is empty");
		}
		if (!index)
		{
			return absl::InternalError("failed to create index");
		}

		std::error_code ec;
		std::filesystem::create_directories(options.dataDir, ec);
		if (ec)
		{
			return absl::InternalError("failed to create data directory: " + ec.message());
		}

		// 判断当前目录是否可用
		auto lockPath = std::filesystem::path(options.dataDir) / FileLockName;
		{
			std::ofstream ofs(lockPath, std::ios::app);
			if (!ofs)
			{
				return absl::InternalError("failed to create lock file: " + lockPath.string());
			}
		}

		try
		{
			flock = std::make_unique<boost::interprocess::file_lock>(lockPath.string().c_str());
		}
		catch (const std::exception& e)
		{
			return absl::InternalError(std::string("failed to acquire file lock: ") + e.what());
		}
		if (!flock->try_lock())
		{
			return absl::FailedPreconditionError("data directory is already in use by another process");
		}

		return absl::OkStatus();
	}

	absl::Status DB::LoadDataFiles()
	{
		std::vector<uint32_t> fids;

		// 读取目录项
		std::error_code ec;
		for (const auto& entry : std::filesystem::directory_iterator(options.dataDir, ec))
		{
			if (ec)
			{
				return absl::InternalError("failed to read data directory: " + ec.message());
			}
			if (!entry.is_regular_file())
			{
				continue;
			}

			const auto path = entry.path();
			if (path.extension() != DataFileNameSuffix)
			{
				continue;
			}

			const auto stem = path.stem().string();
			uint32_t fid = 0;
			auto [ptr, err] = std::from_chars(stem.data(), stem.data() + stem.size(), fid);
			if (err != std::errc() || ptr != stem.data() + stem.size())
			{
				continue;
			}
			fids.push_back(fid);
		}

		if (ec)
		{
			return absl::InternalError("failed to iterate data directory: " + ec.message());
		}
		if (fids.empty())
		{
			return absl::OkStatus();
		}

		std::ranges::sort(fids);
		for (auto fid : fids)
		{
			auto fileOr = DataFile::Open(options.dataDir, fid, IOType::MMap);
			if (!fileOr.ok())
			{
				return fileOr.status();
			}

			if (fid == fids.back())
			{
				activeFile = std::move(*fileOr);
			}
			else
			{
				olderFiles[fid] = std::move(*fileOr);
			}
		}
		return absl::OkStatus();
	}

	absl::Status DB::LoadIndexFromDataFiles()
	{
		if (!activeFile && olderFiles.empty())
		{
			return absl::OkStatus();
		}

		// 如果发生过merge, 则需要从 merge-finished 记录中获取未合并的起始 fid, 以便在加载索引时跳过已合并的文件(因为已合并的文件的索引已经通过hint文件加载到内存了)
		auto hasMerged = false;
		auto nonMergedFid = uint32_t(0);
		if (auto fidOr = GetNonMergedFid(); fidOr.ok())
		{
			nonMergedFid = *fidOr;
			hasMerged = true;
		}
		else if (fidOr.status().code() != absl::StatusCode::kNotFound)
		{
			return fidOr.status();
		}

		// 暂存事务数据
		std::unordered_map<uint64_t, std::vector<TransactionLogRecord>> txnRecords;
		// 扫描过程中更新最大事务序列号
		uint64_t maxSeqNum = 0;
		// 回放过程中累加可回收空间(覆盖/删除产生的旧记录), 末尾一次性写入成员
		int64_t reclaimable = 0;
		// 遍历所有数据文件，加载索引到内存
		for (const auto& [fid, file] : olderFiles)
		{
			if (hasMerged && fid < nonMergedFid)
			{
				// 已合并的文件的索引已经通过hint文件加载到内存了, 跳过
				continue;
			}
			if (auto status = LoadIndexFromOneFile(index.get(), *file, fid, txnRecords, maxSeqNum, reclaimable); !status.ok())
			{
				return status;
			}
		}

		// Load active file
		if (activeFile)
		{
			if (auto status = LoadIndexFromOneFile(index.get(), *activeFile, activeFile->fid, txnRecords, maxSeqNum, reclaimable, &activeFile->writeOffset); !status.ok())
			{
				return status;
			}
		}

		// 更新DB的当前事务序列号
		currentSeqNum = maxSeqNum;
		// 回放重建可回收空间统计
		reclaimSize = reclaimable;

		return absl::OkStatus();
	}

	absl::Status DB::SwitchDataFilesToStandardIO()
	{
		for (auto& [fid, file] : olderFiles)
		{
			if (auto status = ReopenDataFileWithIOType(file, IOType::Standard); !status.ok())
			{
				return status;
			}
		}

		return ReopenDataFileWithIOType(activeFile, IOType::Standard);
	}

	absl::Status DB::ReopenDataFileWithIOType(std::unique_ptr<DataFile>& file, IOType ioType)
	{
		if (!file)
		{
			return absl::OkStatus();
		}

		const uint32_t fid = file->fid;
		const int64_t writeOffset = file->writeOffset;

		if (auto status = file->Close(); !status.ok())
		{
			return status;
		}
		file.reset();

		auto newFileOr = DataFile::Open(options.dataDir, fid, ioType);
		if (!newFileOr.ok())
		{
			return newFileOr.status();
		}

		file = std::move(*newFileOr);
		file->writeOffset = writeOffset;
		return absl::OkStatus();
	}

	absl::Status DB::Put(std::string_view key, std::string_view value)
	{
		{
			std::shared_lock lock(mutex);
			if (closed)
			{
				return absl::FailedPreconditionError("db is closed");
			}
		}
		if (key.empty())
		{
			return absl::InvalidArgumentError("key is empty");
		}

		// Create a log record
		LogRecord record{
			.key = LogRecordKeyWithSeqNum(key, NoTxnSeqNum),
			.value = std::string(value),
			.type = LogRecordType::Normal,
		};

		auto posOr = AppendLogRecordWithLock(record);
		if (!posOr.ok())
		{
			return posOr.status();
		}

		// Update the index
		if (auto oldPos = index->Put(key, *posOr); oldPos.has_value())
		{
			reclaimSize.fetch_add(oldPos->size, std::memory_order_relaxed);
		}

		return absl::OkStatus();
	}

	absl::StatusOr<LogRecordPos> DB::AppendLogRecordWithLock(const LogRecord& record)
	{
		std::unique_lock lock(mutex);
		return AppendLogRecord(record);
	}

	absl::StatusOr<LogRecordPos> DB::AppendLogRecord(const LogRecord& record)
	{
		if (closed)
		{
			return absl::FailedPreconditionError("db is closed");
		}

		// Active data file exists?
		if (!activeFile)
		{
			if (auto status = SetActiveFile(); !status.ok())
			{
				return status;
			}
		}

		// 写入编码后的数据

		auto encodedRecord = EncodeLogRecord(record);
		auto len = encodedRecord.size();
		// 如果到达文件大小限制，则切换到新的数据文件
		if (activeFile->writeOffset + len > options.maxDataFileSize)
		{
			if (auto status = activeFile->Sync(); !status.ok())
			{
				return status;
			}
			// 旋转前已 Sync 落盘, 累计的未同步字节清零
			bytesWrite.store(0, std::memory_order_relaxed);
			olderFiles[activeFile->fid] = std::move(activeFile);
			if (auto status = SetActiveFile(); !status.ok())
			{
				return status;
			}
		}
		auto writeOffset = activeFile->writeOffset;
		auto writeResult = activeFile->Write(encodedRecord);
		if (!writeResult.ok())
		{
			return writeResult.status();
		}
		if (options.syncOnWrite)
		{
			auto status = activeFile->Sync();
			if (!status.ok())
			{
				return status;
			}
			bytesWrite.store(0, std::memory_order_relaxed);
		}
		else if (options.bytesPerSync > 0)
		{
			// 累计本次写入字节, 达到 bytesPerSync 阈值则自动 Sync 并清零
			auto cur = bytesWrite.fetch_add(int64_t(len), std::memory_order_relaxed) + int64_t(len);
			if (uint64_t(cur) >= options.bytesPerSync)
			{
				auto status = activeFile->Sync();
				if (!status.ok())
				{
					return status;
				}
				bytesWrite.store(0, std::memory_order_relaxed);
			}
		}
		// 构造内存索引
		auto pos = LogRecordPos{
			.fid = activeFile->fid,
			.offset = writeOffset,
			.size = int64_t(len),
		};
		return pos;
	}

	absl::Status DB::SetActiveFile()
	{
		uint32_t initialFileId = 0;
		if (activeFile)
		{
			initialFileId = activeFile->fid + 1;
		}
		else if (!olderFiles.empty())
		{
			// activeFile 已被移走(例如 Merge 中), 从 olderFiles 取最大 fid 续号,
			// 否则会重新打开已存在的 fid 0 文件
			initialFileId = olderFiles.rbegin()->first + 1;
		}

		auto newFile = DataFile::Open(options.dataDir, initialFileId, IOType::Standard);
		if (!newFile.ok())
		{
			return newFile.status();
		}
		activeFile = std::move(*newFile);
		return absl::OkStatus();
	}

	absl::StatusOr<std::string> DB::ReadValueFromPos(const LogRecordPos& pos) const
	{
		DataFile* file = nullptr;
		if (activeFile && pos.fid == activeFile->fid)
		{
			file = activeFile.get();
		}
		else
		{
			auto it = olderFiles.find(pos.fid);
			if (it == olderFiles.end())
			{
				return absl::NotFoundError("data file not found for fid: " + std::to_string(pos.fid));
			}
			file = it->second.get();
		}
		if (!file)
		{
			return absl::InternalError("data file not found for fid: " + std::to_string(pos.fid));
		}

		auto recordOr = file->ReadLogRecord(pos.offset);
		if (!recordOr.ok())
		{
			return recordOr.status();
		}
		auto& record = (*recordOr).second;
		if (record.type == LogRecordType::Deleted)
		{
			return absl::NotFoundError("key has been deleted");
		}
		return record.value;
	}

	absl::StatusOr<std::string> DB::Get(std::string_view key)
	{
		std::shared_lock lock(mutex);
		if (closed)
		{
			return absl::FailedPreconditionError("db is closed");
		}
		if (key.empty())
		{
			return absl::InvalidArgumentError("key is empty");
		}

		auto posOr = index->Get(key);
		if (!posOr.ok())
		{
			return posOr.status();
		}
		return ReadValueFromPos(*posOr);
	}

	std::unique_ptr<Iterator> DB::NewIterator(const IteratorOptions& options)
	{
		auto raw = index->NewIterator();
		return std::make_unique<Iterator>(std::move(raw), options,
										  [this](const LogRecordPos& pos) -> absl::StatusOr<std::string> {
											  std::shared_lock lock(mutex);
											  return ReadValueFromPos(pos);
										  });
	}

	std::vector<std::string> DB::ListKeys()
	{
		std::vector<std::string> keys;
		keys.reserve(index->Size());
		auto it = NewIterator(IteratorOptions{});
		for (it->Rewind(); it->Valid(); it->Next())
		{
			keys.emplace_back(it->Key());
		}
		return keys;
	}

	absl::Status DB::Fold(const std::function<bool(std::string_view key, std::string_view value)>& fn)
	{
		auto it = NewIterator(IteratorOptions{});
		for (it->Rewind(); it->Valid(); it->Next())
		{
			auto valueOr = it->Value();
			if (!valueOr.ok())
			{
				return valueOr.status();
			}
			if (!fn(it->Key(), *valueOr))
			{
				break;
			}
		}
		return absl::OkStatus();
	}

	absl::StatusOr<Stat> DB::GetStat() const
	{
		Stat stat;
		stat.reclaimableSize = reclaimSize.load(std::memory_order_relaxed);
		std::shared_lock lock(mutex);
		if (closed)
		{
			return absl::FailedPreconditionError("db is closed");
		}
		stat.keyCount = index->Size();
		stat.dataFileCount = olderFiles.size() + (activeFile ? 1 : 0);
		// 磁盘占用 = 各数据文件已写入字节数 (writeOffset) 之和, 反映 DB 当前持有的数据量。
		// 用逻辑大小而非遍历文件系统, 避免 stdio 缓冲导致未 flush 的写入被漏算。
		int64_t total = 0;
		if (activeFile)
		{
			total += activeFile->writeOffset;
		}
		for (const auto& [fid, file] : olderFiles)
		{
			total += file->writeOffset;
		}
		stat.diskSize = total;
		stat.bytesWrite = bytesWrite.load(std::memory_order_relaxed);
		return stat;
	}

	absl::Status DB::Delete(std::string_view key)
	{
		{
			std::shared_lock lock(mutex);
			if (closed)
			{
				return absl::FailedPreconditionError("db is closed");
			}
		}
		if (key.empty())
		{
			return absl::InvalidArgumentError("key is empty");
		}

		if (auto pos = index->Get(key); !pos.ok())
		{
			return absl::OkStatus();
		}

		// Create a log record for deletion
		auto record = LogRecord{
			.key = LogRecordKeyWithSeqNum(key, NoTxnSeqNum),
			.type = LogRecordType::Deleted,
		};
		auto posOr = AppendLogRecordWithLock(record);
		if (!posOr.ok())
		{
			return posOr.status();
		}
		if (auto oldPos = index->Delete(key); oldPos.has_value())
		{
			reclaimSize.fetch_add(oldPos->size, std::memory_order_relaxed);
		}
		return absl::OkStatus();
	}

	std::unique_ptr<WriteBatch> DB::NewWriteBatch(const WriteBatchOptions& options)
	{
		return std::unique_ptr<WriteBatch>(new WriteBatch(this, options));
	}

	absl::Status DB::Sync()
	{
		std::shared_lock lock(mutex);
		if (closed)
		{
			return absl::FailedPreconditionError("db is closed");
		}
		if (!activeFile)
		{
			return absl::OkStatus();
		}
		auto status = activeFile->Sync();
		if (status.ok())
		{
			// 手动 Sync 落盘后, 累计的未同步字节清零
			bytesWrite.store(0, std::memory_order_relaxed);
		}
		return status;
	}

	absl::Status DB::Close()
	{
		std::unique_lock lock(mutex);
		if (closed)
		{
			return absl::OkStatus();
		}

		absl::Status result = absl::OkStatus();
		if (activeFile)
		{
			if (auto status = activeFile->Close(); !status.ok() && result.ok())
			{
				result = status;
			}
		}
		for (auto& [fid, file] : olderFiles)
		{
			if (auto status = file->Close(); !status.ok() && result.ok())
			{
				result = status;
			}
		}

		flock->unlock();
		flock.reset();

		closed = true;
		return result;
	}

	absl::Status DB::Merge()
	{
		std::vector<DataFile*> filesToMerge;
		uint32_t unMergedFid = 0;
		int64_t reclaimAtStart = 0; // 合并开始时可回收字节数; 合并成功后据此扣减 (并发期间新增的不扣)
		{
			std::unique_lock lock(mutex);
			if (closed)
			{
				return absl::FailedPreconditionError("db is closed");
			}
			if (!activeFile)
			{
				return absl::OkStatus();
			}
			if (merging.exchange(true))
			{
				return absl::FailedPreconditionError("merge is already in progress");
			}

			// 前置检查 1: 可回收空间是否达到阈值 (可回收字节 / 数据总大小)
			int64_t totalDiskSize = activeFile->writeOffset;
			for (const auto& [fid, file] : olderFiles)
			{
				totalDiskSize += file->writeOffset;
			}
			int64_t reclaimable = reclaimSize.load(std::memory_order_relaxed);
			reclaimAtStart = reclaimable; // 快照: 这部分垃圾由本次合并回收, 末尾从 reclaimSize 扣减
			if (options.mergeThreshold > 0.0 &&
				static_cast<double>(reclaimable) < static_cast<double>(totalDiskSize) * options.mergeThreshold)
			{
				merging = false;
				return absl::OkStatus(); // 未达阈值, 无需合并
			}

			// 前置检查 2: 磁盘剩余空间能否容纳合并后的副本 (大小 ≈ 数据总大小 - 可回收字节)
			int64_t liveSize = totalDiskSize - reclaimable;
			std::error_code spaceEc;
			auto spaceInfo = std::filesystem::space(options.dataDir, spaceEc);
			if (spaceEc)
			{
				merging = false;
				return absl::InternalError("failed to query disk space for merge: " + spaceEc.message());
			}
			if (liveSize > static_cast<int64_t>(spaceInfo.available))
			{
				merging = false;
				return absl::ResourceExhaustedError(
					"insufficient disk space for merge: need " + std::to_string(liveSize) +
					" bytes, available " + std::to_string(spaceInfo.available));
			}

			// 创建新的活跃文件用于写入
			if (auto status = activeFile->Sync(); !status.ok())
			{
				merging = false;
				return status;
			}
			// 旋转前已 Sync 落盘, 累计的未同步字节清零
			bytesWrite.store(0, std::memory_order_relaxed);
			olderFiles[activeFile->fid] = std::move(activeFile);
			if (auto status = SetActiveFile(); !status.ok())
			{
				merging = false;
				return status;
			}

			filesToMerge.reserve(olderFiles.size());
			for (auto& [fid, file] : olderFiles)
			{
				filesToMerge.push_back(file.get());
			}
			unMergedFid = filesToMerge.back()->fid + 1;
		}

		auto mergePath = GetMergePath();
		if (std::filesystem::exists(mergePath))
		{
			std::filesystem::remove_all(mergePath);
		}

		std::filesystem::create_directories(mergePath);
		// 打开新的DB实例
		auto mergeOptions = options;
		mergeOptions.dataDir = mergePath;
		mergeOptions.syncOnWrite = false;
		auto mergeDbOr = DB::Open(mergeOptions);
		if (!mergeDbOr.ok())
		{
			merging = false;
			return mergeDbOr.status();
		}
		auto mergeDb = std::move(*mergeDbOr);

		// 打开hint文件用于写入索引
		auto hintFileOr = DataFile::OpenHint(mergePath, 0);
		if (!hintFileOr.ok())
		{
			merging = false;
			return hintFileOr.status();
		}
		auto hintFile = std::move(*hintFileOr);

		// 遍历所有旧数据文件，读取每条记录并写入到新的DB中
		for (auto* file : filesToMerge)
		{
			int64_t offset = 0;
			while (true)
			{
				auto recordOr = file->ReadLogRecord(offset);
				if (!recordOr.ok())
				{
					if (recordOr.status().code() == absl::StatusCode::kOutOfRange)
					{
						break;
					}
					merging = false;
					return recordOr.status();
				}
				auto& [size, record] = *recordOr;

				auto originalKey = ParseLogRecordKey(record.key).second;
				auto posOr = index->Get(originalKey);
				if (!posOr.ok())
				{
					if (posOr.status().code() == absl::StatusCode::kNotFound)
					{
						offset += size;
						continue;
					}
					merging = false;
					return posOr.status();
				}
				auto pos = std::move(*posOr);
				if (pos.fid == file->fid && pos.offset == offset)
				{
					// 有效记录, 不需要seqNum了
					record.key = LogRecordKeyWithSeqNum(originalKey, NoTxnSeqNum);
					auto writePosOr = mergeDb->AppendLogRecord(record);
					if (!writePosOr.ok())
					{
						merging = false;
						return writePosOr.status();
					}
					auto writePos = std::move(*writePosOr);
					// 将索引写入hint文件
					if (auto status = hintFile->WriteHintRecord(originalKey, writePos); !status.ok())
					{
						merging = false;
						return status;
					}
				}
				offset += size;
			}
		}

		// 持久化
		if (auto status = hintFile->Sync(); !status.ok())
		{
			merging = false;
			return status;
		}
		if (auto status = mergeDb->Sync(); !status.ok())
		{
			merging = false;
			return status;
		}
		// 标识merge完成的文件
		auto mergeFinishedFileOr = DataFile::OpenMergeFinishedFile(mergePath);
		if (!mergeFinishedFileOr.ok())
		{
			merging = false;
			return mergeFinishedFileOr.status();
		}
		auto mergeFinishedFile = std::move(*mergeFinishedFileOr);
		LogRecord mergeFinishedRecord{
			.key = std::string(MergeFinishedKey),
			.value = std::to_string(unMergedFid),
			.type = LogRecordType::Normal,
		};
		auto encodedRecord = EncodeLogRecord(mergeFinishedRecord);
		if (auto status = mergeFinishedFile->Write(encodedRecord); !status.ok())
		{
			merging = false;
			return status.status();
		}
		if (auto status = mergeFinishedFile->Sync(); !status.ok())
		{
			merging = false;
			return status;
		}
		// 合并成功: 扣减本次合并开始时已存在的可回收字节 (并发期间新增的仍计入, 不扣)
		reclaimSize.fetch_sub(reclaimAtStart, std::memory_order_relaxed);
		merging = false;
		return absl::OkStatus();
	}

	std::string DB::GetMergePath() const
	{
		auto dataPath = std::filesystem::path(options.dataDir);
		auto dirName = dataPath.filename();
		if (dirName.empty())
		{
			dataPath = dataPath.parent_path();
			dirName = dataPath.filename();
		}

		return (dataPath.parent_path() / (dirName.string() + std::string(MergeDirSuffix))).string();
	}

	absl::Status DB::LoadMergeFiles()
	{
		auto mergePath = GetMergePath();
		std::error_code ec;
		if (!std::filesystem::exists(mergePath, ec))
		{
			if (ec)
			{
				return absl::InternalError("failed to check merge directory: " + ec.message());
			}
			return absl::OkStatus();
		}

		// 收集 mergePath 下的文件, 并确认 merge 是否已完成(存在 merge-finished 标记)
		bool mergeFinished = false;
		std::vector<std::filesystem::path> mergeFiles;
		for (const auto& entry : std::filesystem::directory_iterator(mergePath, ec))
		{
			if (ec)
			{
				return absl::InternalError("failed to read merge directory: " + ec.message());
			}
			if (!entry.is_regular_file())
			{
				continue;
			}
			if (entry.path().filename() == MergeFinishedFileName)
			{
				mergeFinished = true;
				continue;
			}
			// 跳过 merge 子库遗留的锁文件, 避免把它搬进数据目录(数据目录的锁文件由父库持有, 不能被 remove/rename)
			if (entry.path().filename() == FileLockName)
			{
				continue;
			}
			mergeFiles.push_back(entry.path());
		}
		// directory_iterator 构造失败时迭代器为空、循环体不执行, 必须在循环外再判一次 ec
		if (ec)
		{
			return absl::InternalError("failed to read merge directory: " + ec.message());
		}
		if (!mergeFinished)
		{
			// merge 未完成, 保留 mergePath 等待下次启动重试
			return absl::OkStatus();
		}

		auto nonMergedFidOr = GetNonMergedFid();
		if (!nonMergedFidOr.ok())
		{
			return nonMergedFidOr.status();
		}
		const auto nonMergedFid = *nonMergedFidOr;

		// mergeDb 是全新实例, 其 .data 文件 fid 从 0 开始; 统计产物文件数 K
		uint32_t mergedCount = 0;
		for (const auto& p : mergeFiles)
		{
			if (p.extension() == DataFileNameSuffix)
			{
				++mergedCount;
			}
		}

		const auto dataDir = std::filesystem::path(options.dataDir);

		// 1) 删除所有已被合并的旧数据文件 (fid < nonMergedFid)
		for (uint32_t fid = 0; fid < nonMergedFid; ++fid)
		{
			std::filesystem::remove(dataDir / DataFileName(fid), ec);
			if (ec)
			{
				return absl::InternalError("failed to remove old data file: " + ec.message());
			}
		}

		// 2) 把幸存的并发写入文件 (fid >= nonMergedFid) 整体上移 K 个 fid, 为合并产物腾出
		//    [0, K) 的低 fid 区间: 合并快照(fid 较小)在索引重建时会被并发写入正确覆盖,
		//    同时避免 rename 覆盖掉并发文件造成丢数据。从高到低 rename 规避级联冲突。
		if (mergedCount > 0)
		{
			std::vector<uint32_t> concurrentFids;
			for (const auto& entry : std::filesystem::directory_iterator(dataDir, ec))
			{
				if (ec)
				{
					return absl::InternalError("failed to read data directory: " + ec.message());
				}
				if (entry.path().extension() != DataFileNameSuffix)
				{
					continue;
				}
				uint32_t fid = 0;
				const auto stem = entry.path().stem().string();
				auto [ptr, err] = std::from_chars(stem.data(), stem.data() + stem.size(), fid);
				if (err != std::errc() || ptr != stem.data() + stem.size() || fid < nonMergedFid)
				{
					continue;
				}
				concurrentFids.push_back(fid);
			}
			if (ec)
			{
				return absl::InternalError("failed to iterate data directory: " + ec.message());
			}
			std::sort(concurrentFids.rbegin(), concurrentFids.rend());
			for (auto fid : concurrentFids)
			{
				std::filesystem::rename(dataDir / DataFileName(fid), dataDir / DataFileName(fid + mergedCount), ec);
				if (ec)
				{
					return absl::InternalError("failed to shift concurrent data file " + std::to_string(fid) + ": " + ec.message());
				}
			}
		}

		// 3) 将合并产物搬入 dataDir, 保持 mergeDb 分配的 fid(与 hint-index 中记录的 fid 一致)
		for (const auto& filePath : mergeFiles)
		{
			auto targetPath = dataDir / filePath.filename();
			if (filePath.extension() != DataFileNameSuffix && std::filesystem::exists(targetPath, ec))
			{
				if (ec)
				{
					return absl::InternalError("failed to check merged file target " + targetPath.filename().string() + ": " + ec.message());
				}
				std::filesystem::remove(targetPath, ec);
				if (ec)
				{
					return absl::InternalError("failed to remove old merged file target " + targetPath.filename().string() + ": " + ec.message());
				}
			}
			std::filesystem::rename(filePath, targetPath, ec);
			if (ec)
			{
				return absl::InternalError("failed to move merged file " + filePath.filename().string() + ": " + ec.message());
			}
		}

		// 4) 清理 merge 目录(含 merge-finished 标记)
		std::filesystem::remove_all(mergePath, ec);
		if (ec)
		{
			return absl::InternalError("failed to remove merge directory: " + ec.message());
		}
		return absl::OkStatus();
	}

	absl::StatusOr<uint32_t> DB::GetNonMergedFid()
	{
		const auto mergePath = GetMergePath();
		const auto mergeFinishedPath = std::filesystem::path(mergePath) / MergeFinishedFileName;
		std::error_code ec;
		if (!std::filesystem::exists(mergeFinishedPath, ec))
		{
			if (ec)
			{
				return absl::InternalError("failed to check merge-finished file: " + ec.message());
			}
			return absl::NotFoundError("merge-finished file not found");
		}

		auto mergeFinishedFileOr = DataFile::OpenMergeFinishedFile(mergePath);
		if (!mergeFinishedFileOr.ok())
		{
			return mergeFinishedFileOr.status();
		}
		auto& mergeFinishedFile = *mergeFinishedFileOr;

		auto recordOr = mergeFinishedFile->ReadLogRecord(0);
		if (!recordOr.ok())
		{
			return recordOr.status();
		}
		auto& record = (*recordOr).second;

		// merge-finished 记录的 value 是未合并起始 fid 的十进制字符串
		uint32_t fid = 0;
		auto [ptr, err] = std::from_chars(record.value.data(), record.value.data() + record.value.size(), fid);
		if (err != std::errc() || ptr != record.value.data() + record.value.size())
		{
			return absl::InternalError("invalid merge-finished record: failed to parse fid");
		}
		return fid;
	}

	absl::Status DB::LoadIndexFromHintFile()
	{
		auto hintFileName = std::filesystem::path(options.dataDir) / HintFileName;
		if (!std::filesystem::exists(hintFileName))
		{
			return absl::OkStatus();
		}

		auto hintFileOr = DataFile::OpenHint(options.dataDir, 0);
		if (!hintFileOr.ok())
		{
			return hintFileOr.status();
		}
		auto hintFile = std::move(*hintFileOr);

		int64_t offset = 0;
		while (true)
		{
			auto recordOr = hintFile->ReadHintRecord(offset);
			if (!recordOr.ok())
			{
				if (recordOr.status().code() == absl::StatusCode::kOutOfRange)
				{
					break;
				}
				return recordOr.status();
			}
			auto& [size, record] = *recordOr;
			// hint 文件只含 Merge 后的存活记录, 加载时每个 key 首次插入(无旧值), 不累加 reclaimSize
			(void)index->Put(record.key, record.pos);
			offset += size;
		}
		return absl::OkStatus();
	}
} // namespace bitcask
