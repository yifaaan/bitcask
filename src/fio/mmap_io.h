#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

#include "io_manager.h"

namespace bitcask
{

    class MMapIO final : public IOManager
    {
    public:
        static std::unique_ptr<MMapIO> Open(const std::filesystem::path& path);

        ~MMapIO() override;

        MMapIO(const MMapIO&) = delete;
        MMapIO& operator=(const MMapIO&) = delete;
        MMapIO(MMapIO&&) = delete;
        MMapIO& operator=(MMapIO&&) = delete;

        int Read(absl::Span<std::byte> buf, int64_t offset) override;
        int Write(absl::Span<const std::byte> data) override;
        bool Sync() override;
        bool Close() override;
        int64_t Size() const override;

    private:
#ifdef _WIN32
        MMapIO(HANDLE file_handle, HANDLE mapping_handle, std::byte* data, int64_t size);

        HANDLE file_handle_ = INVALID_HANDLE_VALUE;
        HANDLE mapping_handle_ = nullptr;
#else
        MMapIO(int fd, std::byte* data, int64_t size);

        int fd_ = -1;
#endif
        std::byte* data_ = nullptr;
        int64_t size_ = 0;
    };

} // namespace bitcask
