#pragma once

#include "Data/LogRecord.h"

#include <absl/status/status.h>

#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

namespace bitcask
{
    // 事务完成的标记key
    constexpr std::string_view TxnFinishKey = "txn-finish";
    // 非事务写入的事务序列号
    constexpr auto NoTxnSeqNum = 0;

	struct WriteBatchOptions
	{
		uint64_t maxBatchNum = 10000;
		bool syncWrites = false;
	};

	class WriteBatch
	{
	public:
		absl::Status Put(std::string_view key, std::string_view value);
		absl::Status Delete(std::string_view key);
		// 将批量写入提交到数据库, 更新内存索引
		absl::Status Commit();

	private:
		friend class DB;

		explicit WriteBatch(DB* db, WriteBatchOptions options);

		struct PendingWrite
		{
			LogRecordType type = LogRecordType::Normal;
			std::string value;
		};

		DB* db = nullptr;
		WriteBatchOptions options;
		std::mutex mutex;
		std::unordered_map<std::string, PendingWrite> pendingWrites;
	};

    std::string LogRecordKeyWithSeqNum(std::string_view key, uint64_t seqNum);
} // namespace bitcask