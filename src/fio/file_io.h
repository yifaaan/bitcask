#pragma once

#include <cstdio>
#include <filesystem>
#include <memory>

#include "io_manager.h"

namespace bitcask
{

    class FileIO final : public IOManager
    {
    public:
        static std::unique_ptr<FileIO> Open(const std::filesystem::path& path);

        ~FileIO() override;

        FileIO(const FileIO&) = delete;
        FileIO& operator=(const FileIO&) = delete;
        FileIO(FileIO&&) = delete;
        FileIO& operator=(FileIO&&) = delete;

        int Read(std::span<std::byte> buf, int64_t offset) override;
        int Write(std::span<const std::byte> data) override;
        bool Sync() override;
        bool Close() override;
        int64_t Size() const override;

    private:
        explicit FileIO(std::FILE* fd);
        std::FILE* fd_;
    };

} // namespace bitcask
