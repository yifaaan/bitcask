#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace bitcask
{

	uint32_t ComputeCRC32C(std::span<const std::byte> data);
	uint32_t ExtendCRC32C(uint32_t crc, std::span<const std::byte> data);

} // namespace bitcask
