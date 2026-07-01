#pragma once

#include "IOManager.h"

#include <uv.h>

#include <span>
#include <string>
#include <utility>

namespace bitcask
{

	class FileIO : public IOManager
	{
	public:
		~FileIO() override;

		// Open file for read/write + append + create
		static absl::StatusOr<std::unique_ptr<FileIO>> Open(std::string_view path);

		absl::StatusOr<int64_t> Read(std::span<std::byte> buf, int64_t offset) override;
		absl::StatusOr<int64_t> Write(std::span<const std::byte> data) override;
		absl::Status Sync() override;
		absl::Status Close() override;
		absl::StatusOr<int64_t> Size() override;

	private:
		explicit FileIO(uv_file fd, std::string path)
			: fd(fd), path(std::move(path)) {}

		uv_file fd = -1;
		std::string path;
	};

} // namespace bitcask
