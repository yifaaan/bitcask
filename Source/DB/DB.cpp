#include "DB.h"

#include "DB/WriteBatch.h"

#include <algorithm>
#include <charconv>
#include <filesystem>
#include <mutex>
#include <system_error>
#include <utility>

namespace
{
	// 解析 LogRecord 的 key，提取事务序列号和原始 key
	std::pair<uint64_t, std::string> ParseLogRecordKey(std::string_view key)
	{
		auto buf = std::span<const std::byte>(reinterpret_cast<const std::byte*>(key.data()), key.size());
		auto [seqNum, nextIdx] = *bitcask::GetVarint(buf);
		return {seqNum, std::string(key.substr(nextIdx))};
	}

	absl::Status LoadIndexFromOneFile(bitcask::Index* index, const bitcask::DataFile& file, uint32_t fid, std::unordered_map<uint64_t, std::vector<bitcask::TransactionLogRecord>>& txnRecords, uint64_t& maxSeqNum, int64_t* writeOffset = nullptr)
	{
		auto updateIndex = [&](std::string_view key, bitcask::LogRecordPos pos, bitcask::LogRecordType type) -> absl::Status {
			if (type == bitcask::LogRecordType::Deleted)
			{
				if (auto status = index->Delete(key); !status.ok() && status.code() != absl::StatusCode::kNotFound)
				{
					return status;
				}
			}
			else
			{
				if (auto status = index->Put(key, pos); !status.ok())
				{
					return status;
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
			auto fileOr = DataFile::Open(options.dataDir, fid, IOType::Standard);
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
		// 遍历所有数据文件，加载索引到内存
		for (const auto& [fid, file] : olderFiles)
		{
			if (hasMerged && fid < nonMergedFid)
			{
				// 已合并的文件的索引已经通过hint文件加载到内存了, 跳过
				continue;
			}
			if (auto status = LoadIndexFromOneFile(index.get(), *file, fid, txnRecords, maxSeqNum); !status.ok())
			{
				return status;
			}
		}

		// Load active file
		if (activeFile)
		{
			if (auto status = LoadIndexFromOneFile(index.get(), *activeFile, activeFile->fid, txnRecords, maxSeqNum, &activeFile->writeOffset); !status.ok())
			{
				return status;
			}
		}

		// 更新DB的当前事务序列号
		currentSeqNum = maxSeqNum;

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
		if (auto status = index->Put(key, *posOr); !status.ok())
		{
			return status;
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
		if (auto status = index->Delete(key); !status.ok() && status.code() != absl::StatusCode::kNotFound)
		{
			return status;
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
		return activeFile->Sync();
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

		closed = true;
		return result;
	}

	absl::Status DB::Merge()
	{
		std::vector<DataFile*> filesToMerge;
		uint32_t unMergedFid = 0;
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

			// 创建新的活跃文件用于写入
			if (auto status = activeFile->Sync(); !status.ok())
			{
				merging = false;
				return status;
			}
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
			if (auto status = index->Put(record.key, record.pos); !status.ok())
			{
				return status;
			}
			offset += size;
		}
		return absl::OkStatus();
	}
} // namespace bitcask
