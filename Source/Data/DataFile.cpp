#include "DataFile.h"

#include <absl/status/status.h>

#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <sstream>

namespace bitcask
{

	namespace
	{

		absl::StatusOr<std::unique_ptr<DataFile>> OpenNamedFile(std::string_view dirPath, std::string_view fileName, uint32_t fid, IOType ioType)
		{
			const auto path = (std::filesystem::path(dirPath) / fileName).string();
			auto io = IOManager::NewIOManager(path, ioType);
			if (!io.ok())
			{
				return io.status();
			}

			auto file = std::make_unique<DataFile>();
			file->fid = fid;
			file->io = std::move(*io);

			auto sizeOr = file->io->Size();
			if (!sizeOr.ok())
			{
				return sizeOr.status();
			}
			file->writeOffset = *sizeOr;
			return file;
		}

		uint32_t ReadLittleEndian32(std::span<const std::byte> buf)
		{
			return static_cast<uint32_t>(std::to_integer<uint8_t>(buf[0])) |
				(static_cast<uint32_t>(std::to_integer<uint8_t>(buf[1])) << 8) |
				(static_cast<uint32_t>(std::to_integer<uint8_t>(buf[2])) << 16) |
				(static_cast<uint32_t>(std::to_integer<uint8_t>(buf[3])) << 24);
		}

	} // namespace

	std::string DataFileName(uint32_t fid)
	{
		std::ostringstream stream;
		stream << std::setw(9) << std::setfill('0') << fid << DataFileNameSuffix;
		return stream.str();
	}

	absl::StatusOr<std::unique_ptr<DataFile>> bitcask::DataFile::Open(std::string_view dirPath, uint32_t fid, IOType ioType)
	{
		return OpenNamedFile(dirPath, DataFileName(fid), fid, ioType);
	}

	absl::Status DataFile::Sync()
	{
		return io->Sync();
	}

	absl::StatusOr<int64_t> DataFile::Write(std::span<const std::byte> data)
	{
		auto writeOr = io->Write(data);
		if (!writeOr.ok())
		{
			return writeOr.status();
		}
		if (*writeOr != data.size())
		{
			return absl::InternalError("short write");
		}

		writeOffset += *writeOr;
		return *writeOr;
	}

	absl::StatusOr<std::pair<int64_t, LogRecord>> DataFile::ReadLogRecord(int64_t offset) const
	{
		auto sizeOr = io->Size();
		if (!sizeOr.ok())
		{
			return sizeOr.status();
		}
		const int64_t fileSize = *sizeOr;
		if (offset >= fileSize)
		{
			return absl::OutOfRangeError("offset beyond file end");
		}
		const int64_t remaining = fileSize - offset;

		const auto maxHeaderRead = static_cast<size_t>(std::min<int64_t>(remaining, MaxLogRecordHeaderSize));
		std::vector<std::byte> headerBuf(maxHeaderRead);
		auto headerReadOr = io->Read(headerBuf, offset);
		if (!headerReadOr.ok())
		{
			return headerReadOr.status();
		}
		headerBuf.resize(static_cast<size_t>(*headerReadOr));

		auto headerOr = DecodeLogRecordHeader(headerBuf);
		if (!headerOr.first)
		{
			if (*headerReadOr < 5)
			{
				return absl::OutOfRangeError("unexpected eof");
			}
			return absl::InternalError("invalid log record header");
		}

		const auto& header = *headerOr.first;
		const uint64_t totalSize = 4ull + static_cast<uint64_t>(headerOr.second) +
			static_cast<uint64_t>(header.keySize) + static_cast<uint64_t>(header.valueSize);
		if (totalSize > static_cast<uint64_t>(remaining))
		{
			return absl::OutOfRangeError("truncated log record");
		}

		std::vector<std::byte> recordBuf(static_cast<size_t>(totalSize));
		auto recordReadOr = io->Read(recordBuf, offset);
		if (!recordReadOr.ok())
		{
			return recordReadOr.status();
		}
		if (static_cast<uint64_t>(*recordReadOr) != totalSize)
		{
			return absl::OutOfRangeError("truncated log record");
		}

		LogRecord record;
		record.type = header.type;
		record.key.assign(
			reinterpret_cast<const char*>(recordBuf.data() + 4 + headerOr.second),
			static_cast<size_t>(header.keySize));
		record.value.assign(
			reinterpret_cast<const char*>(recordBuf.data() + 4 + headerOr.second + static_cast<int64_t>(header.keySize)),
			static_cast<size_t>(header.valueSize));

		if (CalcLogRecordCRC(record, header) != header.crc)
		{
			return absl::InternalError("crc mismatch");
		}

		return std::pair{static_cast<int64_t>(totalSize), std::move(record)};
	}


	absl::Status DataFile::Close()
	{
		return io->Close();
	}
} // namespace bitcask
