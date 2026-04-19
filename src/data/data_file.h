#pragma once

#include <cstdint>
#include <span>

#include "../fio/io_manager.h"
#include "../status.h"
#include "log_record.h"

namespace bitcask
{
    // 数据文件
    struct DataFile
    {
        static std::unique_ptr<DataFile> Open(const std::filesystem::path& path, uint32_t fid)
        {
            // TODO:
            return nullptr;
        }

        bool Sync()
        {
            return io->Sync();
        }

        Status Write(std::span<const std::byte> data)
        {
            auto written = io->Write(data);
            // TODO: 处理写入不完整的情况，目前简单地当作错误返回
            if (written != data.size())
            {
                return Status::IOError("Failed to write complete record");
            }
            write_offset += written;
            return Status::Ok();
        }

        Status ReadLogRecord(uint64_t offset, LogRecord& record)
        {
            // TODO:
            return Status::Ok();
        }

        uint32_t fid; // 文件ID
        uint64_t write_offset; // 当前写入位置
        std::unique_ptr<IOManager> io; // 文件IO接口
    };
} // namespace bitcask