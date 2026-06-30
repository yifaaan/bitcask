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

		// Create a log record
		LogRecord record{
			.key = std::string(key),
			.value = std::string(value),
			.type = LogRecordType::Normal,
		};

		auto posOr = AppendLogRecord(record);
		if (!posOr.ok())
		{
			return posOr.status();
		}

		// Update the index
		if (auto status = index->Put(key, *posOr); !status.ok())
		{
			return status;
		}
		
		return absl::OkStatus();
	}

	absl::StatusOr<LogRecordPos> DB::AppendLogRecord(const LogRecord& record)
	{
		std::unique_lock lock(mutex);

		// Active data file exists?
		if (!activeFile)
		{
			if (auto status = SetActiveFile(); !status.ok())
			{
				return status;
			}
		}

		// 写入编码后的数据
		
		auto encodedRecord = EncodeLogRecord(record);
		auto len = encodedRecord.size();
		// 如果到达文件大小限制，则切换到新的数据文件
		if (activeFile->writeOffset + len > options.maxDataFileSize)
		{
			if (auto status = activeFile->Sync(); !status.ok())
			{
				return status;
			}
			olderFiles[activeFile->fid] = std::move(activeFile);
			if (auto status = SetActiveFile(); !status.ok())
			{
				return status;
			}
		}
		writeOffset = activeFile->writeOffset;
		auto writeResult = activeFile->Write(encodedRecord);
		if (!writeResult.ok())
		{
			return writeResult.status();
		}
		if (options.syncOnWrite)
		{
			auto status = activeFile->Sync();
			if (!status.ok())
			{
				return status;
			}
		}
		// 构造内存索引
		auto pos = LogRecordPos{
			.fid = activeFile->fid,
			.offset = writeOffset,
			.size = len,
		};
		return pos;
	}

	absl::Status DB::SetActiveFile()
	{
		uint32_t initialFileId = 0;
		if (activeFile)
		{
			initialFileId = activeFile->fid + 1;
		}

		auto newFile = DataFile::Open(options.dataDir, initialFileId, IOType::Standard);
		if (!newFile.ok())
		{
			return newFile.status();
		}
		activeFile = std::move(*newFile);
		return absl::OkStatus();
	}


} // namespace bitcask
