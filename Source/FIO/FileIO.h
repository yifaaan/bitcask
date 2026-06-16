#pragma once

#include "IOManager.h"
#include <cstdio>
#include <string>
#include <span>

namespace bitcask
{

	class FileIO : public IOManager
	{
	public:
		~FileIO() override;

		// Open file for read/write + append + create
		static absl::StatusOr<std::unique_ptr<FileIO>> Open(const std::string& path);

		absl::StatusOr<int64_t> Read(std::span<std::byte> buf, int64_t offset) override;
		absl::StatusOr<int64_t> Write(std::span<const std::byte> data) override;
		absl::Status Sync() override;
		absl::Status Close() override;
		absl::StatusOr<int64_t> Size() override;

	private:
		explicit FileIO(FILE* file, std::string path)
			: file(file), path(std::move(path)) {}

		FILE* file;
		std::string path;
	};

} // namespace bitcask
