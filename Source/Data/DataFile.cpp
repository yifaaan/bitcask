#include "DataFile.h"

#include <absl/status/status.h>

#include <filesystem>
#include <limits>
#include <sstream>
#include <iomanip>
#include <vector>

namespace bitcask
{

	namespace
	{
		constexpr const char* kHintFileName = "hint-index";
		constexpr const char* kMergeFinishedFileName = "merge-finished";

		absl::StatusOr<std::unique_ptr<DataFile>> OpenNamedFile(const std::string& dirPath, const std::string& fileName, uint32_t fid, IOType ioType)
		{
			const auto path = (std::filesystem::path(dirPath) / fileName).string();
			auto io = IOManager::Open(path, ioType);
			if (!io.ok())
			{
				return io.status();
			}

			auto file = std::make_unique<DataFile>();
			file->fid = fid;
			file->io = std::move(*io);

			auto size = file->io->Size();
			if (!size.ok())
			{
				return size.status();
			}
			file->writeOffset = *size;
			return file;
		}
	} // namespace

	std::string DataFileName(uint32_t fid)
	{
		std::ostringstream stream;
		stream << std::setw(9) << std::setfill('0') << fid << kDataFileNameSuffix;
		return stream.str();
	}

	absl::StatusOr<std::unique_ptr<DataFile>> DataFile::Open(const std::string& dirPath, uint32_t fid, IOType ioType)
	{
		return OpenNamedFile(dirPath, DataFileName(fid), fid, ioType);
	}

	absl::Status DataFile::Sync()
	{
		if (!io)
		{
			return absl::FailedPreconditionError("data file is not open");
		}
		return io->Sync();
	}

	absl::StatusOr<int64_t> DataFile::Write(std::span<const std::byte> data)
	{
		if (!io)
		{
			return absl::FailedPreconditionError("data file is not open");
		}

		auto n = io->Write(data);
		if (!n.ok())
		{
			return n.status();
		}
		writeOffset += *n;
		return *n;
	}

	absl::StatusOr<ReadLogRecordResult> DataFile::ReadLogRecord(int64_t offset)
	{
		if (!io)
		{
			return absl::FailedPreconditionError("data file is not open");
		}
		if (offset < 0)
		{
			return absl::InvalidArgumentError("offset out of range");
		}

		std::vector<std::byte> headerBuf(4 + MaxLogRecordHeaderSize);
		auto headerRead = io->Read(headerBuf, offset);
		if (!headerRead.ok())
		{
			return headerRead.status();
		}
		if (*headerRead == 0)
		{
			return ReadLogRecordResult{.size = 0, .isEof = true};
		}
		headerBuf.resize(static_cast<size_t>(*headerRead));

		auto [headerOpt, headerSize] = DecodeLogRecordHeader(headerBuf);
		if (!headerOpt.has_value())
		{
			return absl::DataLossError("invalid log record header");
		}

		const auto& header = *headerOpt;
		if (header.keySize > (std::numeric_limits<int64_t>::max)() - header.valueSize)
		{
			return absl::DataLossError("invalid log record size");
		}

		const int64_t kvSize = header.keySize + header.valueSize;
		if (static_cast<uint64_t>(kvSize) > (std::numeric_limits<size_t>::max)())
		{
			return absl::DataLossError("log record too large");
		}

		std::vector<std::byte> kvBuf(static_cast<size_t>(kvSize));
		if (kvSize > 0)
		{
			auto kvRead = io->Read(kvBuf, offset + headerSize);
			if (!kvRead.ok())
			{
				return kvRead.status();
			}
			if (*kvRead != kvSize)
			{
				return absl::DataLossError("incomplete log record key/value");
			}
		}

		LogRecord record;
		record.type = header.type;
		if (header.keySize > 0)
		{
			record.key.assign(reinterpret_cast<const char*>(kvBuf.data()), static_cast<size_t>(header.keySize));
		}
		if (header.valueSize > 0)
		{
			record.value.assign(reinterpret_cast<const char*>(kvBuf.data() + header.keySize), static_cast<size_t>(header.valueSize));
		}

		const auto headerWithoutCRC = std::span<const std::byte>(headerBuf).subspan(4, static_cast<size_t>(headerSize - 4));
		if (CalcLogRecordCRC(record, headerWithoutCRC) != header.crc)
		{
			return absl::DataLossError("log record CRC mismatch");
		}

		return ReadLogRecordResult{.record = std::move(record), .size = headerSize + kvSize, .isEof = false};
	}

	absl::StatusOr<int64_t> DataFile::AppendHintRecord(const std::string& key, const LogRecordPos& pos)
	{
		auto [posBytes, posSize] = EncodeLogRecordPos(pos);

		LogRecord record;
		record.key = key;
		record.value.assign(reinterpret_cast<const char*>(posBytes.data()), static_cast<size_t>(posSize));
		record.type = LogRecordType::Normal;

		auto [encoded, encodedSize] = EncodeLogRecord(record);
		auto write = Write(encoded);
		if (!write.ok())
		{
			return write.status();
		}
		return encodedSize;
	}

	absl::StatusOr<std::unique_ptr<DataFile>> OpenHintFile(const std::string& dirPath, IOType ioType)
	{
		return OpenNamedFile(dirPath, kHintFileName, 0, ioType);
	}

	absl::StatusOr<std::unique_ptr<DataFile>> OpenMergeFinishedFile(const std::string& dirPath, IOType ioType)
	{
		return OpenNamedFile(dirPath, kMergeFinishedFileName, 0, ioType);
	}

} // namespace bitcask
