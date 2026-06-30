#include "Varint.h"

namespace bitcask
{

size_t VarintSize(uint64_t value)
{
	size_t size = 1;
	while (value >= 0x80)
	{
		value >>= 7;
		++size;
	}
	return size;
}

size_t PutVarint(std::span<std::byte> out, uint64_t value)
{
	size_t index = 0;
	while (value >= 0x80)
	{
		if (index >= out.size())
		{
			return 0;
		}
		out[index++] = static_cast<std::byte>((value & 0x7F) | 0x80);
		value >>= 7;
	}
	if (index >= out.size())
	{
		return 0;
	}
	out[index++] = static_cast<std::byte>(value);
	return index;
}

std::optional<std::pair<uint64_t, size_t>> GetVarint(std::span<const std::byte> in)
{
	uint64_t value = 0;
	size_t shift = 0;

	for (size_t index = 0; index < in.size() && index < MaxVarintLen64; ++index)
	{
		const auto byte = std::to_integer<uint8_t>(in[index]);

		if (index == MaxVarintLen64 - 1)
		{
			if ((byte & 0x80) != 0 || byte > 1)
			{
				return std::nullopt;
			}
			value |= static_cast<uint64_t>(byte) << shift;
			return std::pair{value, index + 1};
		}

		value |= static_cast<uint64_t>(byte & 0x7F) << shift;
		if ((byte & 0x80) == 0)
		{
			return std::pair{value, index + 1};
		}
		shift += 7;
	}

	return std::nullopt;
}

} // namespace bitcask
