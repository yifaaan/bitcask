#include "db.h"
#include "batch.h"
#include "data/log_record.h"

#include <absl/strings/string_view.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>

#include <absl/synchronization/mutex.h>
#include <algorithm>
#include <filesystem>
#include <string>
#include <utility>

namespace fs = std::filesystem;

namespace bitcask
{
    namespace
    {
        // 解析seq+key格式的 key，返回原始 key 和事务序列号
        std::pair<std::string, uint64_t> ParseLogRecordKey(absl::Span<const std::byte> key)
        {
            google::protobuf::io::CodedInputStream stream(reinterpret_cast<const uint8_t*>(key.data()), key.size());
            uint64_t value;
            stream.ReadVarint64(&value);
            return std::make_pair(std::string(reinterpret_cast<const char*>(key.data() + stream.CurrentPosition()), key.size() - stream.CurrentPosition()), value);
        }

        // 暂存事务记录，等遇到 txn-finished 记录时再更新索引
        struct TransactionRecord
        {
            LogRecord record;
            LogRecordPos pos;
        };

        uint64_t CurrentTxnSeq = 0; // 全局事务序列号，每次提交一个事务时递增，非事务写入的记录 seq 为 0
    }

    DB::DB(Options options) : options_(std::move(options))
    {
        index_ = CreateIndexer(options_.index_type);
    }

    std::unique_ptr<DB> DB::Open(Options options)
    {
        if (options.data_dir.empty())
        {
            return nullptr;
        }
        if (options.max_data_file_size == 0)
        {
            return nullptr;
        }

        if (!fs::exists(options.data_dir))
        {
            if (!fs::create_directories(options.data_dir))
            {
                return nullptr;
            }
        }

        auto db = std::unique_ptr<DB>(new DB(std::move(options)));

        if (auto status = db->LoadDataFiles(); !status.ok())
        {
            return nullptr;
        }

        if (auto status = db->LoadIndexFromDataFiles(); !status.ok())
        {
            return nullptr;
        }

        return db;
    }

    absl::Status DB::Put(absl::string_view key, absl::string_view value)
    {
        if (key.empty())
        {
            return absl::InvalidArgumentError("Key cannot be empty");
        }

        LogRecord record;
        record.key = LogRecordKeyWithSeq(key, 0); // 非事务写入，seq为0
        record.value = std::string(value);
        record.type = LogRecordType::kNormal;

        LogRecordPos pos;
        if (auto status = APpendLogRecordWithLock(record, pos); !status.ok())
        {
            return status;
        }

        // 更新内存索引
        if (!index_->Put(key, pos))
        {
            return absl::InternalError("Failed to update index after writing log record");
        }

        return absl::OkStatus();
    }

    std::optional<std::string> DB::Get(absl::string_view key)
    {
        absl::ReaderMutexLock lock(mutex_);

        if (key.empty())
        {
            return std::nullopt;
        }

        // 从内存索引获取记录位置
        auto pos_opt = index_->Get(key);
        if (!pos_opt)
        {
            return std::nullopt;
        }

        // 根据位置获取数据文件
        if (auto res = GetValueByPosition(pos_opt.value()); res.ok())
        {
            return res.value();
        }
        return std::nullopt;
    }

    absl::Status DB::Delete(absl::string_view key)
    {
        if (key.empty())
        {
            return absl::InvalidArgumentError("Key cannot be empty");
        }

        if (!index_->Get(key))
        {
            return absl::OkStatus();
        }

        LogRecord record;
        record.key = LogRecordKeyWithSeq(key, 0); // 非事务写入，seq为0
        record.type = LogRecordType::kDeleted;

        LogRecordPos pos;
        if (auto status = APpendLogRecordWithLock(record, pos); !status.ok())
        {
            return status;
        }

        if (!index_->Delete(key))
        {
            return absl::InternalError("Failed to delete key from index");
        }

        return absl::OkStatus();
    }

    std::vector<std::string> DB::ListKeys()
    {
        auto iter = NewIterator();
        std::vector<std::string> keys;
        keys.reserve(index_->size());
        for (iter->Rewind(); iter->Valid(); iter->Next())
        {
            keys.emplace_back(iter->Key());
        }
        return keys;
    }

    absl::Status DB::Fold(std::function<bool(std::string_view, std::string)> f)
    {
        auto iter = NewIterator();
        for (iter->Rewind(); iter->Valid(); iter->Next())
        {
            const auto value_opt = iter->Value();
            if (!value_opt)
            {
                return absl::InternalError("Failed to read value during fold");
            }
            if (!f(iter->Key(), value_opt.value()))
            {
                break;
            }
        }
        return absl::OkStatus();
    }

    std::unique_ptr<Iterator> DB::NewIterator(IteratorOptions options)
    {
        absl::ReaderMutexLock lock(mutex_);
        return std::make_unique<Iterator>(this, index_->Iterator(options.reverse), options);
    }

    void DB::Close()
    {
        absl::WriterMutexLock lock(mutex_);
        if (active_file_)
        {
            active_file_->Sync();
            active_file_.reset();
        }
        older_files_.clear();
    }

    absl::Status DB::APpendLogRecordWithLock(const LogRecord& record, LogRecordPos& pos)
    {
        absl::WriterMutexLock lock(mutex_);
        return AppendLogRecord(record, pos);
    }
    absl::Status DB::AppendLogRecord(const LogRecord& record, LogRecordPos& pos)
    {
        // 判断当前活跃数据文件是否存在
        if (!active_file_)
        {
            if (auto status = SetActiveDataFile(); !status.ok())
            {
                return status;
            }
        }

        // 写入数据文件
        auto [encoded, size] = EncodeLogRecord(record);

        // 如果超过活跃数据文件的大小限制，则切换到新的数据文件
        if (active_file_->write_offset + size > static_cast<int64_t>(options_.max_data_file_size))
        {
            // 先持久化活跃数据文件
            if (!active_file_->Sync())
            {
                return absl::InternalError("Failed to sync active data file");
            }
            // 将当前活跃数据文件加入旧数据文件列表
            older_files_[active_file_->fid] = std::move(active_file_);
            active_file_.reset();
            // 切换到新的数据文件
            if (auto status = SetActiveDataFile(); !status.ok())
            {
                return status;
            }
        }

        auto write_offset = active_file_->write_offset;
        if (!active_file_->Write(encoded))
        {
            return absl::InternalError("Failed to write log record");
        }

        if (options_.sync_on_write)
        {
            if (!active_file_->Sync())
            {
                return absl::InternalError("Failed to sync active data file");
            }
        }

        // 返回记录的位置，方便更新索引
        pos.fid = active_file_->fid;
        pos.offset = write_offset;

        return absl::OkStatus();
    }

    absl::Status DB::SetActiveDataFile()
    {
        uint32_t initial_fid = 0;
        if (active_file_)
        {
            initial_fid = active_file_->fid + 1;
        }

        // 打开新的数据文件
        auto data_file = DataFile::Open(options_.data_dir, initial_fid);
        if (!data_file)
        {
            return absl::InternalError("Failed to open data file");
        }
        active_file_ = std::move(data_file);
        return absl::OkStatus();
    }

    absl::Status DB::LoadDataFiles()
    {
        std::error_code ec;
        if (!fs::exists(options_.data_dir, ec))
        {
            return absl::OkStatus();
        }

        std::vector<int> file_ids;
        for (const auto& entry : fs::directory_iterator(options_.data_dir, ec))
        {
            if (!entry.is_regular_file())
                continue;
            auto name = entry.path().filename().string();
            if (name.ends_with(kDataFileNameSuffix))
            {
                auto pos = name.find('.');
                if (pos != std::string::npos)
                {
                    try
                    {
                        file_ids.push_back(std::stoi(name.substr(0, pos)));
                    }
                    catch (...)
                    {
                        return absl::DataLossError("Invalid data file name");
                    }
                }
            }
        }

        std::sort(file_ids.begin(), file_ids.end());
        file_ids_ = file_ids;

        for (size_t i = 0; i < file_ids.size(); ++i)
        {
            auto data_file = DataFile::Open(options_.data_dir, static_cast<uint32_t>(file_ids[i]));
            if (!data_file)
            {
                return absl::InternalError("Failed to open data file during load");
            }

            if (i == file_ids.size() - 1)
            {
                active_file_ = std::move(data_file);
            }
            else
            {
                older_files_[static_cast<uint32_t>(file_ids[i])] = std::move(data_file);
            }
        }

        return absl::OkStatus();
    }

    absl::Status DB::LoadIndexFromDataFiles()
    {
        if (file_ids_.empty())
        {
            return absl::OkStatus();
        }

        auto update_index = [this](absl::string_view key, LogRecordType type, const LogRecordPos& pos) {
            bool ok = false;
            if (type == LogRecordType::kDeleted)
            {
                ok = index_->Delete(key);
            }
            else
            {
                ok = index_->Put(key, pos);
            }
            if (!ok)
            {
                return absl::InternalError("Failed to update index for key: " + std::string(key));
            }
            return absl::OkStatus();
        };

        // 暂存事务记录，等遇到 txn-finished 记录时再更新索引
        absl::btree_map<uint64_t, std::vector<TransactionRecord>> pending_txn_records;

        for (size_t i = 0; i < file_ids_.size(); ++i)
        {
            auto fid = static_cast<uint32_t>(file_ids_[i]);
            DataFile* data_file = nullptr;
            if (active_file_ && active_file_->fid == fid)
            {
                data_file = active_file_.get();
            }
            else
            {
                auto it = older_files_.find(fid);
                if (it != older_files_.end())
                {
                    data_file = it->second.get();
                }
            }
            if (!data_file)
            {
                return absl::NotFoundError("Data file not found during index load");
            }

            // 先将读到的数据暂存，要判断是否属于同一个事务
            int64_t offset = 0;
            while (true)
            {
                auto [record_opt, size, is_eof] = data_file->ReadLogRecord(offset);
                if (!record_opt)
                {
                    if (is_eof)
                        break; // EOF
                    return absl::InternalError("Failed to read log record during index load");
                }

                LogRecordPos pos{ fid, offset };
                //  解析 key，获取原始 key 和事务序列号
                auto [origin_key, txn_seq] = ParseLogRecordKey(absl::MakeConstSpan(reinterpret_cast<const std::byte*>(record_opt->key.data()), record_opt->key.size()));

                if (txn_seq == 0) // 普通记录，直接更新索引
                {
                    if (auto status = update_index(origin_key, record_opt->type, pos); !status.ok())
                    {
                        return status;
                    }
                }
                else // 事务记录，暂存起来，等遇到 txn-finished 记录时再更新索引
                {
                    if (record_opt->type == LogRecordType::kTxnFinished) // 事务完成，对应的txn-seq的记录都可以更新索引了
                    {
                        for (const auto& [record, pos] : pending_txn_records[txn_seq])
                        {
                            if (auto status = update_index(record.key, record.type, pos); !status.ok())
                            {
                                return status;
                            }
                        }
                        pending_txn_records.erase(txn_seq);
                    }
                    else // 事务中的记录，暂存起来
                    {
                        record_opt->key = origin_key; // 恢复原始 key，方便后续更新索引
                        pending_txn_records[txn_seq].push_back({ *record_opt, pos });
                    }
                }

                if (txn_seq > CurrentTxnSeq)
                {
                    CurrentTxnSeq = txn_seq; // 更新全局事务序列号，保证新写入的事务记录 seq 大于当前最大的 seq
                }

                offset += size;
            }

            if (i == file_ids_.size() - 1)
            {
                active_file_->write_offset = offset; // 更新 活跃数据文件的写入偏移量
            }
        }

        // 更新全局事务序列号，保证新写入的事务记录 seq 大于当前最大的 seq
        txn_seq_ = CurrentTxnSeq;
        return absl::OkStatus();
    }

    absl::StatusOr<std::string> DB::GetValueByPosition(const LogRecordPos& pos)
    {
        // 根据位置获取数据文件
        DataFile* data_file = nullptr;
        if (active_file_ && active_file_->fid == pos.fid)
        {
            data_file = active_file_.get();
        }
        else
        {
            auto it = older_files_.find(pos.fid);
            if (it != older_files_.end())
            {
                data_file = it->second.get();
            }
        }
        if (!data_file)
        {
            return absl::NotFoundError("data file not found");
        }

        // 读取数据文件中的记录
        auto [record_opt, size, is_eof] = data_file->ReadLogRecord(pos.offset);
        if (!record_opt || record_opt->type == LogRecordType::kDeleted)
        {
            return absl::NotFoundError("record not found");
        }

        return record_opt->value;
    }

} // namespace bitcask
