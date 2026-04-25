#include "db.h"

#include <algorithm>
#include <filesystem>
#include <format>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>

#include "absl/strings/string_view.h"
#include "absl/status/status.h"

namespace fs = std::filesystem;

namespace bitcask
{

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
        record.key = std::string(key);
        record.value = std::string(value);
        record.type = LogRecordType::kNormal;

        LogRecordPos pos;
        if (auto status = AppendLogRecord(record, pos); !status.ok())
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
        std::shared_lock lock(mutex_);

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
        DataFile* data_file = nullptr;
        if (active_file_ && active_file_->fid == pos_opt->fid)
        {
            data_file = active_file_.get();
        }
        else
        {
            auto it = older_files_.find(pos_opt->fid);
            if (it != older_files_.end())
            {
                data_file = it->second.get();
            }
        }
        if (!data_file)
        {
            return std::nullopt;
        }

        // 读取数据文件中的记录
        auto [record_opt, size, is_eof] = data_file->ReadLogRecord(pos_opt->offset);
        if (!record_opt || record_opt->type == LogRecordType::kDeleted)
        {
            return std::nullopt;
        }

        return record_opt->value;
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
        record.key = std::string(key);
        record.type = LogRecordType::kDeleted;

        LogRecordPos pos;
        if (auto status = AppendLogRecord(record, pos); !status.ok())
        {
            return status;
        }

        if (!index_->Delete(key))
        {
            return absl::InternalError("Failed to delete key from index");
        }

        return absl::OkStatus();
    }

    void DB::Close()
    {
        std::unique_lock lock(mutex_);
        if (active_file_)
        {
            active_file_->Sync();
            active_file_.reset();
        }
        older_files_.clear();
    }

    absl::Status DB::AppendLogRecord(const LogRecord& record, LogRecordPos& pos)
    {
        std::unique_lock lock(mutex_);

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

                LogRecordPos pos{fid, offset};
                if (record_opt->type == LogRecordType::kDeleted)
                {
                    index_->Delete(record_opt->key);
                }
                else
                {
                    index_->Put(record_opt->key, pos);
                }

                offset += size;
            }

            if (i == file_ids_.size() - 1)
            {
                active_file_->write_offset = offset; // 更新 活跃数据文件的写入偏移量
            }
        }

        return absl::OkStatus();
    }

} // namespace bitcask
