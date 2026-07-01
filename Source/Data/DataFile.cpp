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

		// Read only the prefix needed to decode the variable-length header.
		// A short read that cannot even cover the minimal header is treated as EOF/truncation.
		const auto maxHeaderRead = static_cast<size_t>(std::min<int64_t>(remaining, MaxLogRecordHeaderSize));
		std::vector<std::byte> headerBuf(maxHeaderRead);
		auto headerSizeOr = io->Read(headerBuf, offset);
		if (!headerSizeOr.ok())
		{
			return headerSizeOr.status();
		}
		headerBuf.resize(*headerSizeOr);
		if (*headerSizeOr < 5)
		{
			return absl::OutOfRangeError("unexpected eof");
		}

		auto headerOr = DecodeLogRecordHeader(headerBuf);
		if (!headerOr.first)
		{
			// If we could not read the full maximum header prefix, the record is most
			// likely cut off at the file tail. Only a fully read header prefix that
			// still cannot be decoded is considered corruption.
			if (static_cast<size_t>(*headerSizeOr) < MaxLogRecordHeaderSize)
			{
				return absl::OutOfRangeError("truncated log record header");
			}
			return absl::InternalError("invalid log record header");
		}

		const auto& header = *headerOr.first;
		if (header.crc == 0 && header.keySize == 0 && header.valueSize == 0)
		{
			return absl::OutOfRangeError("empty log record");
		}

		const auto headerSize = headerOr.second;
		const auto totalSize = headerSize + header.keySize + header.valueSize;
		if (totalSize > static_cast<uint64_t>(remaining))
		{
			return absl::OutOfRangeError("truncated log record");
		}

		// Re-read the whole record only after the header and sizes are validated.
		std::vector<std::byte> kvBuf(header.keySize + header.valueSize);
		auto kvBufSizeOr = io->Read(kvBuf, offset + headerSize);
		if (!kvBufSizeOr.ok())
		{
			return kvBufSizeOr.status();
		}
		if (*kvBufSizeOr != header.keySize + header.valueSize)
		{
			return absl::OutOfRangeError("truncated log record");
		}

		LogRecord record;
		record.type = header.type;
		// Layout: [crc(4)][type][key varint][value varint][key bytes][value bytes].
		record.key.assign(reinterpret_cast<const char*>(kvBuf.data()), header.keySize);
		record.value.assign(reinterpret_cast<const char*>(kvBuf.data() + header.keySize), header.valueSize);

		// CRC covers the header without CRC plus key/value payload.
		if (CalcLogRecordCRC(record, header) != header.crc)
		{
			return absl::InternalError("crc mismatch");
		}

		return std::make_pair(totalSize, std::move(record));
	}


	absl::Status DataFile::Close()
	{
		return io->Close();
	}
} // namespace bitcask
