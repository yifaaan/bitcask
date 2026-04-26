#pragma once

#include <absl/status/status.h>

#include <cstdint>
#include <filesystem>
#include <format>
#include <memory>
#include <string>
#include <tuple>

#include "../fio/io_manager.h"
#include "log_record.h"

namespace bitcask
{

    constexpr auto kDataFileNameSuffix = ".data";

    struct DataFile
    {
        uint32_t fid = 0;
        int64_t write_offset = 0;
        std::unique_ptr<IOManager> io;

        static std::unique_ptr<DataFile> Open(const std::filesystem::path& dir_path, uint32_t fid)
        {
            auto filename = dir_path / std::format("{:09d}{}", fid, kDataFileNameSuffix);
            auto io = CreateIOManager(filename);
            if (!io)
            {
                return nullptr;
            }
            auto df = std::make_unique<DataFile>();
            df->fid = fid;
            df->write_offset = 0;
            df->io = std::move(io);
            return df;
        }

        bool Sync()
        {
            return io->Sync();
        }

        bool Write(absl::Span<const std::byte> data)
        {
            auto written = io->Write(data);
            if (written != static_cast<int>(data.size()))
            {
                return false;
            }
            write_offset += written;
            return true;
        }

        std::tuple<std::optional<LogRecord>, int64_t, bool> ReadLogRecord(int64_t offset)
        {
            auto file_size = io->Size();
            if (file_size < 0)
            {
                return { std::nullopt, 0, false };
            }

            // header 在文件末尾时，需要调整 header 的最大读取大小，避免越界
            int64_t header_max_size = kMaxLogRecordHeaderSize;
            if (offset + header_max_size > file_size)
            {
                header_max_size = file_size - offset;
            }

            // Read header bytes
            std::vector<std::byte> header_buf(header_max_size);
            auto read = io->Read(absl::MakeSpan(header_buf), offset);
            if (read < 0)
            {
                return { std::nullopt, 0, false };
            }

            auto [header_opt, hsize] = DecodeLogRecordHeader(header_buf);
            if (!header_opt || (header_opt->crc == 0 && header_opt->key_size == 0 && header_opt->value_size == 0))
            {
                return { std::nullopt, 0, true }; // EOF
            }

            auto key_size = header_opt->key_size;
            auto value_size = header_opt->value_size;
            int64_t length = hsize + key_size + value_size;

            LogRecord record;
            record.type = header_opt->type;

            if (key_size > 0 || value_size > 0)
            {
                std::vector<std::byte> kv_buf(key_size + value_size);
                auto kv_read = io->Read(absl::MakeSpan(kv_buf), offset + hsize);
                if (kv_read != static_cast<int>(key_size + value_size))
                {
                    return { std::nullopt, 0, false };
                }

                if (key_size > 0)
                {
                    record.key.assign(reinterpret_cast<const char*>(kv_buf.data()), key_size);
                }
                if (value_size > 0)
                {
                    record.value.assign(reinterpret_cast<const char*>(kv_buf.data() + key_size), value_size);
                }
            }

            // Validate CRC
            if (hsize < 4)
            {
                return { std::nullopt, 0, true }; // EOF
            }
            auto header_data = absl::Span<const std::byte>(header_buf).subspan(4, static_cast<size_t>(hsize - 4));
            uint32_t computed_crc = CalcLogRecordCRC(record, header_data);
            if (computed_crc != header_opt->crc)
            {
                return { std::nullopt, 0, false }; // CRC mismatch
            }

            return { record, length, false };
        }

        absl::Status AppendHintRecord(absl::string_view key, const LogRecordPos& pos)
        {
            auto encoded_pos = EncodeLogRecordPos(pos).first;
            auto record = LogRecord{ .key = std::string(key), .value = std::string(reinterpret_cast<const char*>(encoded_pos.data()), encoded_pos.size()), .type = LogRecordType::kNormal };
            auto [encoded_record, _] = EncodeLogRecord(record);
            if (!Write(encoded_record))
            {
                return absl::InternalError("Failed to write hint record");
            }
            return absl::OkStatus();
        }
    };

    inline std::unique_ptr<DataFile> OpenHintFile(const std::filesystem::path& dir_path)
    {
        auto filename = dir_path / "hint-index";
        auto io = CreateIOManager(filename);
        if (!io)
        {
            return nullptr;
        }
        auto df = std::make_unique<DataFile>();
        df->fid = 0; // Hint file typically uses fid 0
        df->write_offset = 0;
        df->io = std::move(io);
        return df;
    }

    inline std::unique_ptr<DataFile> OpenMergeFinishedFile(const std::filesystem::path& dir_path)
    {
        auto filename = dir_path / "merge-finished";
        auto io = CreateIOManager(filename);
        if (!io)
        {
            return nullptr;
        }
        auto df = std::make_unique<DataFile>();
        df->fid = 0; // Merge finished file typically uses fid 0
        df->write_offset = 0;
        df->io = std::move(io);
        return df;
    }
} // namespace bitcask
