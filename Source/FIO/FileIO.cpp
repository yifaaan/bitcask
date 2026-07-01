#include "FileIO.h"

#include <format>
#include <tuple>

namespace bitcask
{
	namespace
	{
		absl::Status UvErrorStatus(int rc, std::string_view op, std::string_view path)
		{
			return absl::InternalError(
				std::format("{} failed for '{}': {}", op, path, uv_strerror(rc)));
		}
	}

	FileIO::~FileIO()
	{
		if (fd != -1)
		{
			std::ignore = Close();
		}
	}

	absl::StatusOr<std::unique_ptr<FileIO>> FileIO::Open(std::string_view path)
	{
		uv_fs_t req;
		const std::string pathString(path);
		int rc = uv_fs_open(
			uv_default_loop(),
			&req,
			pathString.c_str(),
			UV_FS_O_RDWR | UV_FS_O_CREAT | UV_FS_O_APPEND,
			0644,
			nullptr);

		if (rc < 0)
		{
			uv_fs_req_cleanup(&req);
			return UvErrorStatus(rc, "open", pathString);
		}

		auto file = std::unique_ptr<FileIO>(new FileIO(static_cast<uv_file>(rc), pathString));
		uv_fs_req_cleanup(&req);
		return file;
	}

	absl::StatusOr<int64_t> FileIO::Read(std::span<std::byte> buf, int64_t offset)
	{
		if (fd == -1)
		{
			return absl::FailedPreconditionError("file not open");
		}

		uv_fs_t req;
		uv_buf_t iov = uv_buf_init(
			reinterpret_cast<char*>(buf.data()),
			static_cast<unsigned int>(buf.size()));

		int rc = uv_fs_read(
			uv_default_loop(),
			&req,
			fd,
			&iov,
			1,
			offset,
			nullptr);

		if (rc < 0)
		{
			uv_fs_req_cleanup(&req);
			return UvErrorStatus(rc, "read", path);
		}

		uv_fs_req_cleanup(&req);
		return static_cast<int64_t>(rc);
	}

	absl::StatusOr<int64_t> FileIO::Write(std::span<const std::byte> data)
	{
		if (fd == -1)
		{
			return absl::FailedPreconditionError("file not open");
		}

		uv_fs_t req;
		uv_buf_t iov = uv_buf_init(
			const_cast<char*>(reinterpret_cast<const char*>(data.data())),
			static_cast<unsigned int>(data.size()));

		int rc = uv_fs_write(
			uv_default_loop(),
			&req,
			fd,
			&iov,
			1,
			-1,
			nullptr);

		if (rc < 0)
		{
			uv_fs_req_cleanup(&req);
			return UvErrorStatus(rc, "write", path);
		}

		uv_fs_req_cleanup(&req);
		return static_cast<int64_t>(rc);
	}

	absl::Status FileIO::Sync()
	{
		if (fd == -1)
		{
			return absl::FailedPreconditionError("file not open");
		}

		uv_fs_t req;
		int rc = uv_fs_fsync(uv_default_loop(), &req, fd, nullptr);
		if (rc < 0)
		{
			uv_fs_req_cleanup(&req);
			return UvErrorStatus(rc, "fsync", path);
		}

		uv_fs_req_cleanup(&req);
		return absl::OkStatus();
	}

	absl::Status FileIO::Close()
	{
		if (fd == -1)
		{
			return absl::OkStatus();
		}

		uv_fs_t req;
		int rc = uv_fs_close(uv_default_loop(), &req, fd, nullptr);
		uv_fs_req_cleanup(&req);
		fd = -1;

		if (rc < 0)
		{
			return UvErrorStatus(rc, "close", path);
		}
		return absl::OkStatus();
	}

	absl::StatusOr<int64_t> FileIO::Size()
	{
		if (fd == -1)
		{
			return absl::FailedPreconditionError("file not open");
		}

		uv_fs_t req;
		int rc = uv_fs_fstat(uv_default_loop(), &req, fd, nullptr);
		if (rc < 0)
		{
			uv_fs_req_cleanup(&req);
			return UvErrorStatus(rc, "fstat", path);
		}

		const auto size = static_cast<int64_t>(req.statbuf.st_size);
		uv_fs_req_cleanup(&req);
		return size;
	}

} // namespace bitcask
