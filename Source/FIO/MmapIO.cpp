#include "MmapIO.h"

#include <algorithm>
#include <cstring>
#include <format>
#include <tuple>

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

	MmapIO::~MmapIO()
	{
		if (addr)
		{
			std::ignore = Close();
		}
	}

	absl::StatusOr<std::unique_ptr<MmapIO>> MmapIO::Open(const std::string& path)
	{
#ifdef _WIN32
		// Windows implementation
		HANDLE hFile = CreateFileA(
			path.c_str(),
			GENERIC_READ,
			FILE_SHARE_READ,
			nullptr,
			OPEN_EXISTING,
			FILE_ATTRIBUTE_NORMAL,
			nullptr);

		if (hFile == INVALID_HANDLE_VALUE)
		{
			return absl::InternalError(
				std::format("CreateFile failed for '{}'", path));
		}

		LARGE_INTEGER fileSize;
		if (!GetFileSizeEx(hFile, &fileSize))
		{
			CloseHandle(hFile);
			return absl::InternalError("GetFileSize failed");
		}

		HANDLE hMap = CreateFileMappingA(
			hFile,
			nullptr,
			PAGE_READONLY,
			0, 0,
			nullptr);

		if (!hMap)
		{
			CloseHandle(hFile);
			return absl::InternalError("CreateFileMapping failed");
		}

		void* addr = MapViewOfFile(
			hMap,
			FILE_MAP_READ,
			0, 0, 0);

		CloseHandle(hMap); // Can close after mapping
		if (!addr)
		{
			CloseHandle(hFile);
			return absl::InternalError("MapViewOfFile failed");
		}

		return std::unique_ptr<MmapIO>(new MmapIO(addr, fileSize.QuadPart, hFile));

#else
		// POSIX implementation
		int fd = open(path.c_str(), O_RDONLY);
		if (fd < 0)
		{
			return absl::InternalError(
				std::format("open failed for '{}': {}", path, strerror(errno)));
		}

		struct stat st;
		if (fstat(fd, &st) < 0)
		{
			close(fd);
			return absl::InternalError("fstat failed");
		}

		void* addr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
		if (addr == MAP_FAILED)
		{
			close(fd);
			return absl::InternalError("mmap failed");
		}

		auto io = std::unique_ptr<MmapIO>(new MmapIO(addr, st.st_size));
		io->fd_ = fd;
		return io;
#endif
	}

	absl::StatusOr<int64_t> MmapIO::Read(std::span<std::byte> buf, int64_t offset)
	{
		if (!addr)
		{
			return absl::FailedPreconditionError("file not mapped");
		}
		if (offset < 0 || offset > size)
		{
			return absl::InvalidArgumentError("offset out of range");
		}

		int64_t avail = size - offset;
		int64_t toRead = std::min(avail, static_cast<int64_t>(buf.size()));

		if (toRead > 0)
		{
			std::memcpy(buf.data(), static_cast<char*>(addr) + offset, toRead);
		}

		return toRead;
	}

	absl::StatusOr<int64_t> MmapIO::Write(std::span<const std::byte> /*data*/)
	{
		// MmapIO is read-only
		return absl::FailedPreconditionError("MmapIO is read-only");
	}

	absl::Status MmapIO::Sync()
	{
		// Read-only mapping, no sync needed
		return absl::OkStatus();
	}

	absl::Status MmapIO::Close()
	{
		if (!addr)
		{
			return absl::OkStatus();
		}

#ifdef _WIN32
		UnmapViewOfFile(addr);
		if (handle != INVALID_HANDLE_VALUE)
		{
			CloseHandle(handle);
			handle = INVALID_HANDLE_VALUE;
		}
#else
		munmap(addr, size);
		if (fd_ >= 0)
		{
			close(fd_);
			fd_ = -1;
		}
#endif
		addr = nullptr;
		size = 0;
		return absl::OkStatus();
	}

	absl::StatusOr<int64_t> MmapIO::Size()
	{
		if (!addr)
		{
			return absl::FailedPreconditionError("file not mapped");
		}
		return size;
	}

} // namespace bitcask
