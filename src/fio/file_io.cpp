#include "file_io.h"

#ifdef _WIN32
#include <io.h>
#include <sys/stat.h>
#else
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace bitcask
{

    std::unique_ptr<FileIO> FileIO::Open(const std::filesystem::path& path)
    {
        auto* fd = std::fopen(path.string().c_str(), "a+b");
        if (!fd)
        {
            return nullptr;
        }
        return std::unique_ptr<FileIO>(new FileIO(fd));
    }

    FileIO::FileIO(std::FILE* fd) : fd_(fd)
    {
    }

    FileIO::~FileIO()
    {
        Close();
    }

    int FileIO::Read(absl::Span<std::byte> buf, int64_t offset)
    {
#ifdef _WIN32
        if (_fseeki64(fd_, static_cast<__int64>(offset), SEEK_SET) != 0)
            return -1;
#else
        if (fseeko(fd_, static_cast<off_t>(offset), SEEK_SET) != 0)
            return -1;
#endif
        return static_cast<int>(std::fread(buf.data(), 1, buf.size(), fd_));
    }

    int FileIO::Write(absl::Span<const std::byte> data)
    {
        return static_cast<int>(std::fwrite(data.data(), 1, data.size(), fd_));
    }

    bool FileIO::Sync()
    {
#ifdef _WIN32
        return std::fflush(fd_) == 0 && _commit(_fileno(fd_)) == 0;
#else
        return std::fflush(fd_) == 0 && fsync(fileno(fd_)) == 0;
#endif
    }

    bool FileIO::Close()
    {
        if (fd_)
        {
            auto ret = std::fclose(fd_);
            fd_ = nullptr;
            return ret == 0;
        }
        return true;
    }

    int64_t FileIO::Size() const
    {
#ifdef _WIN32
        struct _stat64 st;
        if (_fstat64(_fileno(fd_), &st) != 0)
            return -1;
#else
        struct stat st;
        if (fstat(fileno(fd_), &st) != 0)
            return -1;
#endif
        return st.st_size;
    }

    std::unique_ptr<IOManager> CreateIOManager(const std::filesystem::path& path)
    {
        return FileIO::Open(path);
    }

} // namespace bitcask
