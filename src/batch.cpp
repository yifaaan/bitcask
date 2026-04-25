#include "batch.h"

#include <absl/status/status.h>
#include <absl/synchronization/mutex.h>
#include <google/protobuf/io/coded_stream.h>

#include <array>
#include <cstddef>
#include <cstdint>

#include "data/log_record.h"
#include "db.h"

namespace bitcask
{
    namespace
    {
        // 将 key 和事务序列号编码成一个新的 key
        std::vector<std::byte> LogRecordKeyWithSeq(absl::string_view key, uint64_t seq)
        {
            constexpr size_t kMaxVarint64Size = 10;
            std::array<std::byte, kMaxVarint64Size> seq_buf{};

            auto* seq_start = reinterpret_cast<uint8_t*>(seq_buf.data());
            auto* seq_end = google::protobuf::io::CodedOutputStream::WriteVarint64ToArray(seq, seq_start);
            const auto seq_size = static_cast<size_t>(seq_end - seq_start);

            std::vector<std::byte> encoded_key;
            encoded_key.reserve(seq_size + key.size());
            encoded_key.insert(encoded_key.end(), seq_buf.begin(), seq_buf.begin() + seq_size);
            if (!key.empty())
            {
                const auto* key_begin = reinterpret_cast<const std::byte*>(key.data());
                encoded_key.insert(encoded_key.end(), key_begin, key_begin + key.size());
            }
            return encoded_key;
        }
    }

    WriteBatch::WriteBatch(DB* db, WriteBatchOptions opts) : db_(db), opts_(std::move(opts))
    {
    }

    absl::Status WriteBatch::Put(absl::string_view key, absl::string_view value)
    {
        if (key.empty())
        {
            return absl::InvalidArgumentError("Key cannot be empty");
        }

        absl::MutexLock lock(mutex_);

        LogRecord record{ .key = std::string(key), .value = std::string(value), .type = LogRecordType::kNormal };
        pending_writes_[key] = record;
        return absl::OkStatus();
    }

    absl::Status WriteBatch::Delete(absl::string_view key)
    {
        if (key.empty())
        {
            return absl::InvalidArgumentError("Key cannot be empty");
        }

        absl::MutexLock lock(mutex_);
        if (!db_->index_->Get(key)) // 如果索引中不存在该 key，说明已经被删除了，或者根本不存在，无需再写入删除记录
        {
            if (pending_writes_.contains(key))
            {
                pending_writes_.erase(key);
            }
        }
        LogRecord record{ .key = std::string(key), .type = LogRecordType::kDeleted };
        pending_writes_[key] = record;
        return absl::OkStatus();
    }

    absl::Status WriteBatch::Commit()
    {
        absl::MutexLock lock(mutex_);

        if (pending_writes_.empty())
        {
            return absl::OkStatus();
        }

        if (pending_writes_.size() > opts_.max_batch_size)
        {
            return absl::InvalidArgumentError("Batch size exceeds the maximum limit");
        }

        // 加锁，保证事务提交的串行化
        absl::ReaderMutexLock txn_lock(db_->mutex_);
        // 获取最新的事物序列号
        auto txn_seq = db_->txn_seq_.fetch_add(1) + 1;
        absl::btree_map<std::string, LogRecordPos> positions; // 用于记录每个 key 的位置
        // 只有在成功将所有记录写入数据文件后，才会更新索引，这样可以保证原子性
        for (const auto& [key, record] : pending_writes_)
        {
            // 写入数据文件
            LogRecord to_write = record;
            auto encoded_key = LogRecordKeyWithSeq(key, txn_seq);
            to_write.key = std::string(reinterpret_cast<const char*>(encoded_key.data()), encoded_key.size());
            if (auto res = db_->AppendLogRecord(to_write, positions[key]); !res.ok())
            {
                return absl::InternalError("Failed to append log record for key: " + key);
            }
        }

        // 写入一个特殊的记录，表示一个事务的完成，这样在恢复时可以知道哪些记录是已经提交的，哪些是未提交的
        auto encoded_key = LogRecordKeyWithSeq("txn-finished", txn_seq);
        LogRecord commit_record{ .key = std::string(reinterpret_cast<const char*>(encoded_key.data()), encoded_key.size()), .type = LogRecordType::kTxnFinished };
        LogRecordPos txn_finished_pos;
        if (auto res = db_->AppendLogRecord(commit_record, txn_finished_pos); !res.ok())
        {
            return absl::InternalError("Failed to append txn finished record");
        }
        // 持久化？
        if (opts_.sync_on_commit && db_->active_file_)
        {
            if (!db_->active_file_->Sync())
            {
                return absl::InternalError("Failed to sync active data file on commit");
            }
        }
        // 更新内存索引
        for (const auto& [key, record] : pending_writes_)
        {
            if (record.type == LogRecordType::kDeleted)
            {
                if (!db_->index_->Delete(key))
                {
                    return absl::InternalError("Failed to update index for key: " + key);
                }
            }
            else if (record.type == LogRecordType::kNormal)
            {
                if (!db_->index_->Put(key, positions[key]))
                {
                    return absl::InternalError("Failed to update index for key: " + key);
                }
            }
        }

        pending_writes_.clear();

        return absl::OkStatus();
    }

}
