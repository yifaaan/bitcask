#include "LogRecord.h"

#include <absl/crc/crc32c.h>

#include <cstring>
#include <limits>
#include <string_view>

namespace bitcask
{

	namespace
	{
		uint32_t ReadLittleEndian32(std::span<const std::byte> buf)
		{
			return static_cast<uint32_t>(std::to_integer<uint8_t>(buf[0])) |
				(static_cast<uint32_t>(std::to_integer<uint8_t>(buf[1])) << 8) |
				(static_cast<uint32_t>(std::to_integer<uint8_t>(buf[2])) << 16) |
				(static_cast<uint32_t>(std::to_integer<uint8_t>(buf[3])) << 24);
		}

		void WriteLittleEndian32(std::span<std::byte> buf, uint32_t value)
		{
			buf[0] = static_cast<std::byte>(value & 0xFF);
			buf[1] = static_cast<std::byte>((value >> 8) & 0xFF);
			buf[2] = static_cast<std::byte>((value >> 16) & 0xFF);
			buf[3] = static_cast<std::byte>((value >> 24) & 0xFF);
		}
	} // namespace

	std::vector<std::byte> EncodeLogRecord(const LogRecord& record)
	{
		if (record.key.size() > std::numeric_limits<uint32_t>::max() || record.value.size() > std::numeric_limits<uint32_t>::max())
		{
			return {};
		}

		std::vector<std::byte> header(MaxLogRecordHeaderSize);
		size_t headerSize = 0;

		header[headerSize++] = static_cast<std::byte>(record.type);
		headerSize += PutVarint(std::span<std::byte>(header).subspan(headerSize), record.key.size());
		headerSize += PutVarint(std::span<std::byte>(header).subspan(headerSize), record.value.size());
		header.resize(headerSize);

		LogRecordHeader headerMeta;
		headerMeta.type = record.type;
		headerMeta.keySize = record.key.size();
		headerMeta.valueSize = record.value.size();
		const auto crc = CalcLogRecordCRC(record, headerMeta);
		std::vector<std::byte> result(4 + headerSize + record.key.size() + record.value.size());

		WriteLittleEndian32(std::span<std::byte>(result).first<4>(), crc);
		std::memcpy(result.data() + 4, header.data(), headerSize);
		std::memcpy(result.data() + 4 + headerSize, record.key.data(), record.key.size());
		std::memcpy(result.data() + 4 + headerSize + record.key.size(), record.value.data(), record.value.size());
		return result;
	}

	std::pair<std::optional<LogRecordHeader>, int64_t> DecodeLogRecordHeader(std::span<const std::byte> buf)
	{
		if (buf.size() < 5)
		{
			return {std::nullopt, 0};
		}

		LogRecordHeader header;
		int64_t offset = 0;

		header.crc = ReadLittleEndian32(buf.subspan(offset, 4));
		offset += 4;
		header.type = static_cast<LogRecordType>(std::to_integer<uint8_t>(buf[offset++]));

		auto keySize = GetVarint(buf.subspan(offset));
		if (!keySize || keySize->first > std::numeric_limits<uint32_t>::max())
		{
			return {std::nullopt, 0};
		}
		header.keySize = keySize->first;
		offset += static_cast<int64_t>(keySize->second);

		auto valueSize = GetVarint(buf.subspan(offset));
		if (!valueSize || valueSize->first > std::numeric_limits<uint32_t>::max())
		{
			return {std::nullopt, 0};
		}
		header.valueSize = valueSize->first;
		offset += static_cast<int64_t>(valueSize->second);

		return {header, offset};
	}

	uint32_t CalcLogRecordCRC(const LogRecord& record, const LogRecordHeader& header)
	{
		std::vector<std::byte> headerWithoutCrc(1 + VarintSize(header.keySize) + VarintSize(header.valueSize));
		size_t offset = 0;
		headerWithoutCrc[offset++] = static_cast<std::byte>(static_cast<uint8_t>(header.type));
		offset += PutVarint(std::span<std::byte>(headerWithoutCrc).subspan(offset), header.keySize);
		offset += PutVarint(std::span<std::byte>(headerWithoutCrc).subspan(offset), header.valueSize);
		headerWithoutCrc.resize(offset);

		auto crc = absl::ComputeCrc32c(absl::string_view(
			reinterpret_cast<const char*>(headerWithoutCrc.data()), headerWithoutCrc.size()));
		crc = absl::ExtendCrc32c(crc, absl::string_view(record.key));
		crc = absl::ExtendCrc32c(crc, absl::string_view(record.value));
		return static_cast<uint32_t>(crc);
	}

	std::pair<std::vector<std::byte>, int64_t> EncodeLogRecordPos(const LogRecordPos& pos)
	{
		if (pos.offset < 0 || pos.size < 0)
		{
			return {{}, 0};
		}

		std::vector<std::byte> buf(3 * MaxVarintLen64);
		size_t offset = 0;
		offset += PutVarint(std::span<std::byte>(buf).subspan(offset), pos.fid);
		offset += PutVarint(std::span<std::byte>(buf).subspan(offset), static_cast<uint64_t>(pos.offset));
		offset += PutVarint(std::span<std::byte>(buf).subspan(offset), static_cast<uint64_t>(pos.size));
		buf.resize(offset);
		return {std::move(buf), static_cast<int64_t>(offset)};
	}

	std::pair<std::optional<LogRecordPos>, int64_t> DecodeLogRecordPos(std::span<const std::byte> buf)
	{
		LogRecordPos pos;
		int64_t offset = 0;

		auto fid = GetVarint(buf.subspan(offset));
		if (!fid || fid->first > std::numeric_limits<uint32_t>::max())
		{
			return {std::nullopt, 0};
		}
		pos.fid = static_cast<uint32_t>(fid->first);
		offset += static_cast<int64_t>(fid->second);

		auto recordOffset = GetVarint(buf.subspan(offset));
		if (!recordOffset || recordOffset->first > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))
		{
			return {std::nullopt, 0};
		}
		pos.offset = static_cast<int64_t>(recordOffset->first);
		offset += static_cast<int64_t>(recordOffset->second);

		auto size = GetVarint(buf.subspan(offset));
		if (!size || size->first > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))
		{
			return {std::nullopt, 0};
		}
		pos.size = static_cast<int64_t>(size->first);
		offset += static_cast<int64_t>(size->second);

		return {pos, offset};
	}

} // namespace bitcask
