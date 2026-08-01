#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <utility>

namespace bitcask
{

	constexpr size_t MaxVarintLen32 = 5;
	constexpr size_t MaxVarintLen64 = 10;

	size_t VarintSize(uint64_t value);
	size_t PutVarint(std::span<std::byte> out, uint64_t value);
	std::optional<std::pair<uint64_t, size_t>> GetVarint(std::span<const std::byte> in);

} // namespace bitcask
