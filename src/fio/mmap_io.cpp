#include "mmap_io.h"

#include <algorithm>
#include <cstring>
#include <limits>

#ifdef _WIN32
#include <io.h>
#else
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace bitcask
{

    std::unique_ptr<MMapIO> MMapIO::Open(const std::filesystem::path& path)
    {
#ifdef _WIN32
        auto file_handle = ::CreateFileW(path.wstring().c_str(), GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (file_handle == INVALID_HANDLE_VALUE)
        {
            return nullptr;
        }

        LARGE_INTEGER file_size{};
        if (::GetFileSizeEx(file_handle, &file_size) == 0)
        {
            ::CloseHandle(file_handle);
            return nullptr;
        }

        if (file_size.QuadPart == 0)
        {
            return std::unique_ptr<MMapIO>(new MMapIO(file_handle, nullptr, nullptr, 0));
        }

        auto mapping_handle = ::CreateFileMappingW(file_handle, nullptr, PAGE_READONLY, 0, 0, nullptr);
        if (!mapping_handle)
        {
            ::CloseHandle(file_handle);
            return nullptr;
        }

        auto* data = static_cast<std::byte*>(::MapViewOfFile(mapping_handle, FILE_MAP_READ, 0, 0, 0));
        if (!data)
        {
            ::CloseHandle(mapping_handle);
            ::CloseHandle(file_handle);
            return nullptr;
        }

        return std::unique_ptr<MMapIO>(new MMapIO(file_handle, mapping_handle, data, file_size.QuadPart));
#else
        auto fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0)
        {
            return nullptr;
        }

        struct stat st;
        if (::fstat(fd, &st) != 0)
        {
            ::close(fd);
            return nullptr;
        }

        if (st.st_size == 0)
        {
            return std::unique_ptr<MMapIO>(new MMapIO(fd, nullptr, 0));
        }

        auto* mapped = ::mmap(nullptr, static_cast<size_t>(st.st_size), PROT_READ, MAP_SHARED, fd, 0);
        if (mapped == MAP_FAILED)
        {
            ::close(fd);
            return nullptr;
        }
        auto* data = static_cast<std::byte*>(mapped);

        return std::unique_ptr<MMapIO>(new MMapIO(fd, data, st.st_size));
#endif
    }

#ifdef _WIN32
    MMapIO::MMapIO(HANDLE file_handle, HANDLE mapping_handle, std::byte* data, int64_t size)
        : file_handle_(file_handle), mapping_handle_(mapping_handle), data_(data), size_(size)
    {
    }
#else
    MMapIO::MMapIO(int fd, std::byte* data, int64_t size) : fd_(fd), data_(data), size_(size)
    {
    }
#endif

    MMapIO::~MMapIO()
    {
        Close();
    }

    int MMapIO::Read(absl::Span<std::byte> buf, int64_t offset)
    {
        if (offset < 0)
        {
            return -1;
        }
        if (buf.empty() || offset >= size_)
        {
            return 0;
        }

        const auto available = size_ - offset;
        const auto bytes_to_read = std::min<int64_t>({ static_cast<int64_t>(buf.size()), available, std::numeric_limits<int>::max() });
        std::memcpy(buf.data(), data_ + offset, static_cast<size_t>(bytes_to_read));
        return static_cast<int>(bytes_to_read);
    }

    int MMapIO::Write(absl::Span<const std::byte>)
    {
        return -1;
    }

    bool MMapIO::Sync()
    {
        return true;
    }

    bool MMapIO::Close()
    {
        bool ok = true;
#ifdef _WIN32
        if (data_)
        {
            ok = ::UnmapViewOfFile(data_) != 0 && ok;
            data_ = nullptr;
        }
        if (mapping_handle_)
        {
            ok = ::CloseHandle(mapping_handle_) != 0 && ok;
            mapping_handle_ = nullptr;
        }
        if (file_handle_ != INVALID_HANDLE_VALUE)
        {
            ok = ::CloseHandle(file_handle_) != 0 && ok;
            file_handle_ = INVALID_HANDLE_VALUE;
        }
#else
        if (data_)
        {
            ok = ::munmap(data_, static_cast<size_t>(size_)) == 0 && ok;
            data_ = nullptr;
        }
        if (fd_ >= 0)
        {
            ok = ::close(fd_) == 0 && ok;
            fd_ = -1;
        }
#endif
        size_ = 0;
        return ok;
    }

    int64_t MMapIO::Size() const
    {
        return size_;
    }

} // namespace bitcask
