#include "DB/WriteBatch.h"

#include "DB/DB.h"
#include "Data/Varint.h"

#include <atomic>

namespace bitcask
{
	WriteBatch::WriteBatch(DB* db, WriteBatchOptions options)
		: db(db), options(std::move(options))
	{
	}
	
	absl::Status WriteBatch::Put(std::string_view key, std::string_view value)
	{
		if (key.empty())
		{
			return absl::InvalidArgumentError("Key cannot be empty");
		}
		std::scoped_lock lock(mutex);
		pendingWrites[std::string(key)] = PendingWrite{LogRecordType::Normal, std::string(value)};
		return absl::OkStatus();
	}

	absl::Status WriteBatch::Delete(std::string_view key)
	{
		if (key.empty())
		{
			return absl::InvalidArgumentError("Key cannot be empty");
		}
		std::scoped_lock lock(mutex);
		// 暂存一条删除记录, 在 Commit 时统一写墓碑并更新索引。
		// 对同一 key, Delete 会覆盖之前 pending 的 Put; 删除不存在的 key 视作 no-op。
		pendingWrites[std::string(key)] = PendingWrite{LogRecordType::Deleted, ""};
		return absl::OkStatus();
	}

	absl::Status WriteBatch::Commit()
	{
		std::scoped_lock lock(mutex);
		if (pendingWrites.empty())
		{
			return absl::OkStatus();
		}
		if (pendingWrites.size() > options.maxBatchNum)
		{
			return absl::InvalidArgumentError("Too many pending writes");
		}

		// 获取当前最新事务序列号
		std::scoped_lock dbLock(db->mutex);
		auto seqNum = db->currentSeqNum.fetch_add(1);
		// 将所有待写入的 LogRecord 追加到当前活跃数据文件中
		std::unordered_map<std::string, LogRecordPos> positions;
		for (const auto& [key, record] : pendingWrites)
		{
			// 构造新的key，包含事务序列号和原始key
			auto posOr = db->AppendLogRecord(LogRecord{LogRecordKeyWithSeqNum(key, seqNum), record.value, record.type});
			if (!posOr.ok())
			{
				return posOr.status();
			}
			auto& pos = *posOr;
			positions[std::string(key)] = std::move(pos);
		}

		// 写一条表示事务完成的 LogRecord, 以便在启动时知道该事务已经完成
		auto finishRecord = LogRecord{LogRecordKeyWithSeqNum(TxnFinishKey, seqNum), "", LogRecordType::TxnFinished};
		if (auto finishPosOr = db->AppendLogRecord(finishRecord); !finishPosOr.ok())
		{
			return finishPosOr.status();
		}

		if (options.syncWrites && db->activeFile)
		{
			if (auto status = db->activeFile->Sync(); !status.ok())
			{
				return status;
			}
		}

		// 更新内存索引
		for (const auto& [key, record] : pendingWrites)
		{
			auto pos = positions[std::string(key)];
			if (record.type == LogRecordType::Deleted)
			{
				if (auto oldPos = db->index->Delete(key); oldPos.has_value())
				{
					db->reclaimSize.fetch_add(oldPos->size, std::memory_order_relaxed);
				}
			}
			else if (record.type == LogRecordType::Normal)
			{
				if (auto oldPos = db->index->Put(key, pos); oldPos.has_value())
				{
					db->reclaimSize.fetch_add(oldPos->size, std::memory_order_relaxed);
				}
			}
		}
		pendingWrites.clear();
		return absl::OkStatus();
	}

	// 将 seqNum 和 key 编码为 LogRecord 的字节序列
	std::string LogRecordKeyWithSeqNum(std::string_view key, uint64_t seqNum)
	{
		std::vector<std::byte> result;
		result.resize(MaxVarintLen64);
		auto n = PutVarint(result, seqNum);
		result.resize(n);
		auto keyBegin = reinterpret_cast<const std::byte*>(key.data());
		result.insert(result.end(), keyBegin, keyBegin + key.size());
		auto resultBegin = reinterpret_cast<const char*>(result.data());
		return std::string(resultBegin, resultBegin + result.size());
	}
} // namespace bitcask