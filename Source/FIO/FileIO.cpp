#include "FileIO.h"

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#endif

#include <filesystem>
#include <format>
#include <system_error>
#include <tuple>

namespace bitcask
{
	namespace
	{
		absl::Status SystemErrorStatus(std::string_view op, std::string_view path)
		{
#ifdef _WIN32
			const DWORD err = GetLastError();
#else
			const int err = errno;
#endif
			return absl::InternalError(std::format("{} failed for '{}': {}", op, path,
				std::system_category().message(err)));
		}
	}

	FileIO::~FileIO()
	{
		if (IsValid())
		{
			std::ignore = Close();
		}
	}

	absl::StatusOr<std::unique_ptr<FileIO>> FileIO::Open(std::string_view path)
	{
		const std::string pathString(path);

#ifdef _WIN32
		const std::wstring widePath = std::filesystem::path(pathString).wstring();
		const HANDLE h = CreateFileW(
			widePath.c_str(),
			GENERIC_READ | GENERIC_WRITE,
			FILE_SHARE_READ | FILE_SHARE_WRITE,
			nullptr,
			OPEN_ALWAYS,
			FILE_ATTRIBUTE_NORMAL,
			nullptr);

		if (h == INVALID_HANDLE_VALUE)
		{
			return SystemErrorStatus("open", pathString);
		}

		return std::unique_ptr<FileIO>(new FileIO(h, pathString));
#else
		const int fd = ::open(
			pathString.c_str(),
			O_RDWR | O_CREAT | O_APPEND,
			0644);

		if (fd < 0)
		{
			return SystemErrorStatus("open", pathString);
		}

		return std::unique_ptr<FileIO>(new FileIO(static_cast<native_handle>(fd), pathString));
#endif
	}

	absl::StatusOr<int64_t> FileIO::Read(std::span<std::byte> buf, int64_t offset)
	{
		if (!IsValid())
		{
			return absl::FailedPreconditionError("file not open");
		}

#ifdef _WIN32
		OVERLAPPED overlapped{};
		overlapped.Offset = static_cast<DWORD>(offset);
		overlapped.OffsetHigh = static_cast<DWORD>(offset >> 32);

		DWORD bytesRead = 0;
		if (!ReadFile(static_cast<HANDLE>(fd), buf.data(), static_cast<DWORD>(buf.size()), &bytesRead, &overlapped))
		{
			const DWORD err = GetLastError();
			if (err == ERROR_HANDLE_EOF)
			{
				return static_cast<int64_t>(bytesRead);
			}
			return SystemErrorStatus("read", path);
		}

		return static_cast<int64_t>(bytesRead);
#else
		const auto bytes = buf.size();
		ssize_t rc = pread(static_cast<int>(fd), buf.data(), bytes, offset);

		if (rc < 0)
		{
			return SystemErrorStatus("read", path);
		}

		return static_cast<int64_t>(rc);
#endif
	}

	absl::StatusOr<int64_t> FileIO::Write(std::span<const std::byte> data)
	{
		if (!IsValid())
		{
			return absl::FailedPreconditionError("file not open");
		}

#ifdef _WIN32
		LARGE_INTEGER li{};
		li.QuadPart = 0;
		if (!SetFilePointerEx(static_cast<HANDLE>(fd), li, nullptr, FILE_END))
		{
			return SystemErrorStatus("write", path);
		}

		DWORD bytesWritten = 0;
		if (!WriteFile(static_cast<HANDLE>(fd), data.data(), static_cast<DWORD>(data.size()), &bytesWritten, nullptr))
		{
			return SystemErrorStatus("write", path);
		}

		return static_cast<int64_t>(bytesWritten);
#else
		ssize_t rc = write(static_cast<int>(fd), data.data(), data.size());

		if (rc < 0)
		{
			return SystemErrorStatus("write", path);
		}

		return static_cast<int64_t>(rc);
#endif
	}

	absl::Status FileIO::Sync()
	{
		if (!IsValid())
		{
			return absl::FailedPreconditionError("file not open");
		}

#ifdef _WIN32
		if (!FlushFileBuffers(static_cast<HANDLE>(fd)))
		{
			return SystemErrorStatus("fsync", path);
		}
#else
		if (fsync(static_cast<int>(fd)) < 0)
		{
			return SystemErrorStatus("fsync", path);
		}
#endif

		return absl::OkStatus();
	}

	absl::Status FileIO::Close()
	{
		if (!IsValid())
		{
			return absl::OkStatus();
		}

#ifdef _WIN32
		if (!CloseHandle(static_cast<HANDLE>(fd)))
		{
			fd = invalid_handle_v;
			return SystemErrorStatus("close", path);
		}
#else
		if (::close(static_cast<int>(fd)) < 0)
		{
			fd = invalid_handle_v;
			return SystemErrorStatus("close", path);
		}
#endif

		fd = invalid_handle_v;
		return absl::OkStatus();
	}

	absl::StatusOr<int64_t> FileIO::Size()
	{
		if (!IsValid())
		{
			return absl::FailedPreconditionError("file not open");
		}

#ifdef _WIN32
		LARGE_INTEGER li{};
		if (!GetFileSizeEx(static_cast<HANDLE>(fd), &li))
		{
			return SystemErrorStatus("fstat", path);
		}

		return static_cast<int64_t>(li.QuadPart);
#else
		struct stat st;
		if (fstat(static_cast<int>(fd), &st) < 0)
		{
			return SystemErrorStatus("fstat", path);
		}

		return static_cast<int64_t>(st.st_size);
#endif
	}

} // namespace bitcask
