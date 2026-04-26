#pragma once

#include <absl/container/btree_map.h>

#include <string>

#include "data/log_record.h"
#include "options.h"

namespace bitcask
{
    class DB;
    class WriteBatch
    {
    public:
        WriteBatch(DB* db, WriteBatchOptions opts);
        ~WriteBatch() = default;

        WriteBatch(const WriteBatch&) = delete;
        WriteBatch& operator=(const WriteBatch&) = delete;
        WriteBatch(WriteBatch&&) noexcept = delete;
        WriteBatch& operator=(WriteBatch&&) noexcept = delete;

        absl::Status Put(absl::string_view key, absl::string_view value);
        absl::Status Delete(absl::string_view key);
        absl::Status Commit();

    private:
        WriteBatchOptions opts_;
        absl::Mutex mutex_;
        DB* db_;
        absl::btree_map<std::string, LogRecord> pending_writes_;
    };

    // 将 key 和事务序列号编码成一个新的 key
    std::string LogRecordKeyWithSeq(absl::string_view key, uint64_t seq);
}