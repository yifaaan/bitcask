#include "db.h"
#include "data/data_file.h"
#include "data/log_record.h"
#include "status.h"

#include <algorithm>
#include <cstring>
#include <mutex>
#include <shared_mutex>
#include <utility>

#ifdef _WIN32
#include <intrin.h>
#endif

namespace bitcask
{

    Status DB::Put(std::string_view key, std::string_view value)
    {
        if (key.empty())
        {
            return Status::KeyIsEmpty("Key cannot be empty");
        }

        auto record = LogRecord{ std::string(key), std::string(value), LogRecordType::kNormal };

        // 将 LogRecord 追加写入数据文件，并获取记录写入的位置
        LogRecordPos pos;
        if (auto status = AppendLogRecord(record, pos); !status)
        {
            return status;
        }

        // 更新内存索引
        if (auto status = index_->Put(key, pos); !status)
        {
            return Status::UpdateIndexFailed("Failed to update index after writing log record");
        }

        return Status::Ok();
    }

    Status DB::AppendLogRecord(const LogRecord& record, LogRecordPos& pos)
    {
        std::unique_lock lock(mutex_);

        // 判断当前活跃数据文件是否存在
        if (!active_data_file_)
        {
            if (auto status = SetActiveDataFile(); !status.ok())
            {
                return status;
            }
        }

        // 写入数据文件
        auto encoded = LogRecord::Encode(record);
        // 如果超过活跃数据文件的大小限制，则切换到新的数据文件
        if (active_data_file_->write_offset + encoded.size() > options_.max_data_file_size) // 10MB
        {
            // 先持久化活跃数据文件
            if (!active_data_file_->io->Sync())
            {
                return Status::IOError("Failed to sync active data file");
            }
            // 将当前活跃数据文件加入旧数据文件列表
            old_data_files_[active_data_file_->fid] = std::move(active_data_file_);
            active_data_file_.reset();
            // 切换到新的数据文件
            if (auto status = SetActiveDataFile(); !status.ok())
            {
                return status;
            }
        }

        auto write_offset = active_data_file_->write_offset;
        if (auto status = active_data_file_->Write(encoded); !status)
        {
            return status;
        }
        if (options_.sync_on_write)
        {
            if (!active_data_file_->Sync())
            {
                return Status::IOError("Failed to sync active data file");
            }
        }

        // 返回记录的位置，方便更新索引
        pos.fid = active_data_file_->fid;
        pos.offset = write_offset;

        return Status::Ok();
    }

    Status DB::SetActiveDataFile()
    {
        uint32_t initial_fid = 0;
        if (active_data_file_)
        {
            initial_fid = active_data_file_->fid + 1;
        }
        // 打开新的数据文件
        auto data_file = DataFile::Open(options_.data_dir, initial_fid);
        if (!data_file)
        {
            return Status::IOError("Failed to open data file");
        }
        active_data_file_ = std::move(data_file);
        return Status::Ok();
    }

    std::optional<std::string> DB::Get(std::string_view key) const
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
        if (active_data_file_ && active_data_file_->fid == pos_opt->fid)
        {
            data_file = active_data_file_.get();
        }
        else
        {
            data_file = old_data_files_.at(pos_opt->fid).get();
        }
        if (!data_file)
        {
            return std::nullopt;
        }

        // 读取数据文件中的记录
        LogRecord record;
        if (auto status = data_file->ReadLogRecord(pos_opt->offset, record); !status || record.type == LogRecordType::kDeleted)
        {
            return std::nullopt;
        }

        return record.value;
    }

} // namespace bitcask
