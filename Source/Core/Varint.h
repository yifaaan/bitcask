#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <tuple>
#include <utility>
#include <vector>

namespace bitcask
{

	constexpr int MaxVarintLength = 10;

	inline int VarintLength(uint64_t value) noexcept
	{
		int len = 1;
		while (value >= 0x80)
		{
			value >>= 7;
			++len;
		}
		return len;
	}

	inline int PutVarint(std::span<std::byte> buf, uint64_t value) noexcept
	{
		const int len = VarintLength(value);
		if (buf.size() < static_cast<size_t>(len))
		{
			return 0;
		}

		int n = 0;
		while (value >= 0x80)
		{
			buf[n++] = static_cast<std::byte>((value & 0x7F) | 0x80);
			value >>= 7;
		}
		buf[n++] = static_cast<std::byte>(value);
		return n;
	}

	inline std::pair<uint64_t, int> GetVarint(std::span<const std::byte> buf) noexcept
	{
		uint64_t result = 0;
		int shift = 0;

		for (int n = 0; n < static_cast<int>(buf.size()) && n < MaxVarintLength; ++n)
		{
			const auto byte = std::to_integer<uint8_t>(buf[n]);
			if (shift == 63 && (byte & 0x7Eu) != 0)
			{
				return {0, 0};
			}

			result |= static_cast<uint64_t>(byte & 0x7Fu) << shift;
			if ((byte & 0x80u) == 0)
			{
				return {result, n + 1};
			}
			shift += 7;
		}

		return {0, 0};
	}

	inline std::vector<std::byte> EncodeVarint(uint64_t value)
	{
		std::vector<std::byte> buf(static_cast<size_t>(VarintLength(value)));
		std::ignore = PutVarint(buf, value);
		return buf;
	}

} // namespace bitcask
