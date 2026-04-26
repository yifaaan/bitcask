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

        //  加载merge数据目录, 将merge后的文件移过来
        if (auto status = db->LoadMergeFiles(); !status.ok())
        {
            return nullptr;
        }

        if (auto status = db->LoadDataFiles(); !status.ok())
        {
            return nullptr;
        }

        if (auto status = db->LoadIndexFromHintFile(); !status.ok())
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

    bool DB::Sync()
    {
        absl::ReaderMutexLock lock(mutex_);
        if (active_file_)
        {
            active_file_->Sync();
        }
        return true;
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
        else if (!older_files_.empty())
        {
            initial_fid = older_files_.rbegin()->first + 1;
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
        // 发生过merge
        bool has_merge = false;
        uint32_t non_merge_file_id = 0;
        auto merge_finish_file = options_.data_dir / "merge-finished";
        if (fs::exists(merge_finish_file))
        {
            has_merge = true;
            non_merge_file_id = GetNonMergeFileID();
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
            if (has_merge && fid < non_merge_file_id)
            {
                continue; // 发生过merge的情况下，跳过 merge 前的数据文件
            }
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

    absl::Status DB::Merge()
    {
        if (!active_file_)
        {
            return absl::OkStatus();
        }

        DataFile* new_active_file = nullptr;
        std::vector<DataFile*> to_merge_files;
        {
            absl::WriterMutexLock lock(mutex_);
            if (is_merging_)
            {
                return absl::FailedPreconditionError("Merge is already in progress");
            }
            is_merging_ = true;

            if (!active_file_->Sync())
            {
                is_merging_ = false;
                return absl::InternalError("Failed to sync active data file");
            }

            // 打开新的活跃数据文件
            auto data_file = DataFile::Open(options_.data_dir, active_file_->fid + 1);
            if (!data_file)
            {
                is_merging_ = false;
                return absl::InternalError("Failed to open data file");
            }
            new_active_file = data_file.get();
            older_files_[active_file_->fid] = std::move(active_file_);
            active_file_ = std::move(data_file);

            for (const auto& [_, file] : older_files_)
            {
                to_merge_files.push_back(file.get());
            }
        }

        // 从小到大merge
        std::ranges::sort(to_merge_files, [](DataFile* a, DataFile* b) { return a->fid < b->fid; });
        auto merge_path = GetMergePath();
        if (fs::exists(merge_path))
        {
            if (!fs::remove_all(merge_path))
            {
                is_merging_ = false;
                return absl::InternalError("Failed to remove existing merge directory");
            }
        }
        if (!fs::create_directories(merge_path))
        {
            is_merging_ = false;
            return absl::InternalError("Failed to create merge directory");
        }

        // 打开新的DB实例，数据文件目录指向 merge_path
        Options merge_options = options_;
        merge_options.data_dir = merge_path;
        merge_options.sync_on_write = false; // 合并过程中不需要每次写入都 fsync，等合并完成后再一次性 fsync
        auto merge_db = Open(merge_options);
        if (!merge_db)
        {
            is_merging_ = false;
            return absl::InternalError("Failed to open merge DB instance");
        }

        // 打开 hint 索引文件
        auto hint_file = OpenHintFile(merge_path);
        for (auto f : to_merge_files)
        {
            int64_t offset = 0;
            while (true)
            {
                auto [record_opt, size, is_eof] = f->ReadLogRecord(offset);
                if (!record_opt)
                {
                    if (is_eof)
                        break; // EOF
                    return absl::InternalError("Failed to read log record during merge");
                }
                auto origin_key = ParseLogRecordKey(absl::MakeConstSpan(reinterpret_cast<const std::byte*>(record_opt->key.data()), record_opt->key.size())).first;
                auto pos = index_->Get(origin_key);
                // 内存索引的pos是最新的
                if (pos && pos->fid == f->fid && pos->offset == offset) // 只有当记录在内存索引中的位置和当前读取的位置一致时，才认为这条记录是最新的，需要被合并
                {
                    // 这里不需要使用事务记录的 seq，数据已经落盘了
                    record_opt->key = LogRecordKeyWithSeq(origin_key, 0);
                    if (auto res = merge_db->AppendLogRecord(*record_opt, pos.value()); !res.ok())
                    {
                        is_merging_ = false;
                        return absl::InternalError("Failed to append log record during merge");
                    }
                    // 写入 hint 索引文件:real key + position
                    if (auto res = hint_file->AppendHintRecord(origin_key, pos.value()); !res.ok())
                    {
                        is_merging_ = false;
                        return absl::InternalError("Failed to append hint record during merge");
                    }
                }

                offset += size;
            }
        }

        if (!hint_file->Sync())
        {
            is_merging_ = false;
            return absl::InternalError("Failed to sync hint file after merge");
        }
        if (!merge_db->Sync())
        {
            is_merging_ = false;
            return absl::InternalError("Failed to sync active data file after merge");
        }

        // 标记合并完成的文件，等下次打开数据库时，如果发现 merge-finished 文件存在，就删除旧数据文件，保留合并后的数据文件和 hint 索引文件
        auto merge_finished_file = OpenMergeFinishedFile(merge_path);
        if (!merge_finished_file)
        {
            is_merging_ = false;
            return absl::InternalError("Failed to create merge finished file");
        }
        // 存储merge到了哪个文件
        // key="merge-finished"，value=新活跃数据文件的fid，新的active_file并未merge
        auto record = LogRecord{ .key = "merge-finished", .value = std::to_string(new_active_file->fid), .type = LogRecordType::kNormal };
        auto [encoded_, _] = EncodeLogRecord(record);
        if (!merge_finished_file->Write(encoded_))
        {
            is_merging_ = false;
            return absl::InternalError("Failed to write merge finished record");
        }
        if (!merge_finished_file->Sync())
        {
            is_merging_ = false;
            return absl::InternalError("Failed to sync merge finished file");
        }
        is_merging_ = false;
        return absl::OkStatus();
    }

    std::filesystem::path DB::GetMergePath() const
    {
        auto merge_dir = options_.data_dir.parent_path() / options_.data_dir.lexically_normal().filename() / "-merge";
        return merge_dir;
    }

    absl::Status DB::LoadMergeFiles()
    {
        auto merge_path = GetMergePath();
        if (!fs::exists(merge_path))
        {
            return absl::OkStatus();
        }

        auto dir_entries = fs::directory_iterator(merge_path);
        bool has_merge_finished_file = false;

        std::vector<std::string> merge_file_ids;
        for (const auto& entry : dir_entries)
        {
            if (!entry.is_regular_file())
                continue;
            auto name = entry.path().filename().string();
            if (name == "merge-finished")
            {
                has_merge_finished_file = true;
            }
            merge_file_ids.push_back(name);
        }
        if (!has_merge_finished_file)
        {
            if (!fs::remove_all(merge_path))
            {
                return absl::InternalError("Failed to remove merge directory");
            }
            return absl::OkStatus();
        }

        auto non_merge_fid = GetNonMergeFileID();
        // 删除旧数据文件，保留合并后的数据文件和 hint 索引文件
        for (uint32_t fid = 0; fid < non_merge_fid; fid++)
        {
            auto filename = options_.data_dir / std::format("{:09d}{}", fid, kDataFileNameSuffix);
            if (fs::exists(filename))
            {
                if (!fs::remove(filename))
                {
                    return absl::InternalError("Failed to remove old data file after merge");
                }
            }
        }

        // 将合并后的数据文件移动到数据目录下
        for (const auto& name : merge_file_ids)
        {
            auto src = merge_path / name;
            auto dst = options_.data_dir / name;
            fs::rename(src, dst);
        }
        if (!fs::remove_all(merge_path))
        {
            return absl::InternalError("Failed to remove merge directory");
        }
        return absl::OkStatus();
    }

    uint32_t DB::GetNonMergeFileID() const
    {
        auto merge_path = GetMergePath();
        if (!fs::exists(merge_path / "merge-finished"))
        {
            merge_path = options_.data_dir;
        }
        if (!fs::exists(merge_path / "merge-finished"))
        {
            return 0;
        }

        auto merge_finished_file = OpenMergeFinishedFile(merge_path);
        if (!merge_finished_file)
        {
            return 0;
        }
        auto [record_opt, _, is_eof] = merge_finished_file->ReadLogRecord(0);
        if (!record_opt || is_eof)
        {
            return 0;
        }
        return static_cast<uint32_t>(std::stoul(record_opt->value));
    }

    absl::Status DB::LoadIndexFromHintFile()
    {
        auto hint_file_name = options_.data_dir / "hint-index";
        if (!fs::exists(hint_file_name))
        {
            return absl::OkStatus();
        }
        auto hint_file = OpenHintFile(options_.data_dir);
        if (!hint_file)
        {
            return absl::InternalError("Failed to open hint file");
        }

        int64_t offset = 0;
        while (true)
        {
            auto [record_opt, size, is_eof] = hint_file->ReadLogRecord(offset);
            if (!record_opt)
            {
                if (is_eof)
                    break; // EOF
                return absl::InternalError("Failed to read log record from hint file");
            }

            auto [pos_opt, _] = DecodeLogRecordPos(absl::MakeConstSpan(reinterpret_cast<const std::byte*>(record_opt->value.data()), record_opt->value.size()));
            if (!pos_opt)
            {
                return absl::InternalError("Failed to decode log record position from hint file");
            }
            if (!index_->Put(record_opt->key, pos_opt.value()))
            {
                return absl::InternalError("Failed to update index from hint file");
            }
            offset += size;
        }
        return absl::OkStatus();
    }

} // namespace bitcask
