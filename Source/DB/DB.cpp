#include "DB.h"

#include <filesystem>
#include <mutex>
#include <system_error>
#include <utility>

namespace bitcask
{

	DB::DB(Options options)
		: options(std::move(options)),
		  index(CreateIndex(this->options.indexType))
	{
	}

	absl::StatusOr<std::unique_ptr<DB>> DB::Open(const Options& options)
	{
		auto db = std::unique_ptr<DB>(new DB(options));
		auto status = db->Initialize();
		if (!status.ok())
		{
			return status;
		}
		return db;
	}

	absl::Status DB::Initialize()
	{
		if (options.dataDir.empty())
		{
			return absl::InvalidArgumentError("dataDir is empty");
		}
		if (!index)
		{
			return absl::InternalError("failed to create index");
		}

		std::error_code ec;
		std::filesystem::create_directories(options.dataDir, ec);
		if (ec)
		{
			return absl::InternalError("failed to create data directory: " + ec.message());
		}

		auto file = DataFile::Open(options.dataDir, activeFid, IOType::Standard);
		if (!file.ok())
		{
			return file.status();
		}
		activeFile = std::move(*file);
		return absl::OkStatus();
	}

	absl::Status DB::Put(std::string_view key, std::string_view value)
	{
		if (key.empty())
		{
			return absl::InvalidArgumentError("key is empty");
		}

		
		return absl::OkStatus();
	}

	

} // namespace bitcask
