#include "LogRecord.h"

#include "CRC32C.h"

#include <cstring>
#include <limits>

namespace bitcask
{

	namespace
	{
		constexpr size_t kCRCSize = 4;

		void WriteFixed32LE(std::span<std::byte> buf, uint32_t value)
		{
			buf[0] = static_cast<std::byte>(value & 0xFFu);
			buf[1] = static_cast<std::byte>((value >> 8) & 0xFFu);
			buf[2] = static_cast<std::byte>((value >> 16) & 0xFFu);
			buf[3] = static_cast<std::byte>((value >> 24) & 0xFFu);
		}

		uint32_t ReadFixed32LE(std::span<const std::byte> buf)
		{
			return static_cast<uint32_t>(std::to_integer<uint8_t>(buf[0])) |
				   (static_cast<uint32_t>(std::to_integer<uint8_t>(buf[1])) << 8) |
				   (static_cast<uint32_t>(std::to_integer<uint8_t>(buf[2])) << 16) |
				   (static_cast<uint32_t>(std::to_integer<uint8_t>(buf[3])) << 24);
		}

		std::span<const std::byte> StringBytes(const std::string& value)
		{
			if (value.empty())
			{
				return {};
			}
			return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
		}

		bool IsValidType(uint8_t value)
		{
			return value <= static_cast<uint8_t>(LogRecordType::TxnFinished);
		}
	} // namespace

	std::pair<std::vector<std::byte>, int64_t> EncodeLogRecord(const LogRecord& record)
	{
		std::vector<std::byte> header(MaxLogRecordHeaderSize);
		size_t headerLen = 0;

		header[headerLen++] = static_cast<std::byte>(record.type);
		const int keyLen = PutVarint(std::span(header).subspan(headerLen), record.key.size());
		headerLen += static_cast<size_t>(keyLen);
		const int valueLen = PutVarint(std::span(header).subspan(headerLen), record.value.size());
		headerLen += static_cast<size_t>(valueLen);
		header.resize(headerLen);

		const auto crc = CalcLogRecordCRC(record, header);
		const auto totalSize = static_cast<int64_t>(kCRCSize + headerLen + record.key.size() + record.value.size());
		std::vector<std::byte> result(static_cast<size_t>(totalSize));

		WriteFixed32LE(std::span(result).first(kCRCSize), crc);
		size_t pos = kCRCSize;
		std::memcpy(result.data() + pos, header.data(), header.size());
		pos += header.size();
		if (!record.key.empty())
		{
			std::memcpy(result.data() + pos, record.key.data(), record.key.size());
			pos += record.key.size();
		}
		if (!record.value.empty())
		{
			std::memcpy(result.data() + pos, record.value.data(), record.value.size());
		}

		return {std::move(result), totalSize};
	}

	std::pair<std::optional<LogRecordHeader>, int64_t> DecodeLogRecordHeader(std::span<const std::byte> buf)
	{
		if (buf.size() < kCRCSize + 1)
		{
			return {std::nullopt, 0};
		}

		LogRecordHeader header;
		header.crc = ReadFixed32LE(buf.first(kCRCSize));
		size_t pos = kCRCSize;

		const auto typeValue = std::to_integer<uint8_t>(buf[pos++]);
		if (!IsValidType(typeValue))
		{
			return {std::nullopt, 0};
		}
		header.type = static_cast<LogRecordType>(typeValue);

		auto [keySize, keySizeLen] = GetVarint(buf.subspan(pos));
		if (keySizeLen == 0 || keySize > static_cast<uint64_t>((std::numeric_limits<int64_t>::max)()))
		{
			return {std::nullopt, 0};
		}
		header.keySize = static_cast<int64_t>(keySize);
		pos += static_cast<size_t>(keySizeLen);

		auto [valueSize, valueSizeLen] = GetVarint(buf.subspan(pos));
		if (valueSizeLen == 0 || valueSize > static_cast<uint64_t>((std::numeric_limits<int64_t>::max)()))
		{
			return {std::nullopt, 0};
		}
		header.valueSize = static_cast<int64_t>(valueSize);
		pos += static_cast<size_t>(valueSizeLen);

		return {header, static_cast<int64_t>(pos)};
	}

	uint32_t CalcLogRecordCRC(const LogRecord& record, std::span<const std::byte> headerWithoutCRC)
	{
		auto crc = ComputeCRC32C(headerWithoutCRC);
		crc = ExtendCRC32C(crc, StringBytes(record.key));
		crc = ExtendCRC32C(crc, StringBytes(record.value));
		return crc;
	}

	std::pair<std::vector<std::byte>, int64_t> EncodeLogRecordPos(const LogRecordPos& pos)
	{
		std::vector<std::byte> buf(3 * MaxVarintLength);
		size_t n = 0;
		n += static_cast<size_t>(PutVarint(std::span(buf).subspan(n), pos.fid));
		n += static_cast<size_t>(PutVarint(std::span(buf).subspan(n), static_cast<uint64_t>(pos.offset)));
		n += static_cast<size_t>(PutVarint(std::span(buf).subspan(n), static_cast<uint64_t>(pos.size)));
		buf.resize(n);
		return {std::move(buf), static_cast<int64_t>(n)};
	}

	std::pair<std::optional<LogRecordPos>, int64_t> DecodeLogRecordPos(std::span<const std::byte> buf)
	{
		LogRecordPos pos;
		size_t n = 0;

		auto [fid, fidLen] = GetVarint(buf.subspan(n));
		if (fidLen == 0 || fid > (std::numeric_limits<uint32_t>::max)())
		{
			return {std::nullopt, 0};
		}
		pos.fid = static_cast<uint32_t>(fid);
		n += static_cast<size_t>(fidLen);

		auto [offset, offsetLen] = GetVarint(buf.subspan(n));
		if (offsetLen == 0 || offset > static_cast<uint64_t>((std::numeric_limits<int64_t>::max)()))
		{
			return {std::nullopt, 0};
		}
		pos.offset = static_cast<int64_t>(offset);
		n += static_cast<size_t>(offsetLen);

		auto [size, sizeLen] = GetVarint(buf.subspan(n));
		if (sizeLen == 0 || size > static_cast<uint64_t>((std::numeric_limits<int64_t>::max)()))
		{
			return {std::nullopt, 0};
		}
		pos.size = static_cast<int64_t>(size);
		n += static_cast<size_t>(sizeLen);

		return {pos, static_cast<int64_t>(n)};
	}

} // namespace bitcask
