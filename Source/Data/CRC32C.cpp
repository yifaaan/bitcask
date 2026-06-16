#include "CRC32C.h"

#include <absl/crc/crc32c.h>
#include <absl/strings/string_view.h>

namespace bitcask
{

	namespace
	{
		absl::string_view AsStringView(std::span<const std::byte> data)
		{
			if (data.empty())
			{
				return {};
			}
			return {reinterpret_cast<const char*>(data.data()), data.size()};
		}
	} // namespace

	uint32_t ComputeCRC32C(std::span<const std::byte> data)
	{
		return static_cast<uint32_t>(absl::ComputeCrc32c(AsStringView(data)));
	}

	uint32_t ExtendCRC32C(uint32_t crc, std::span<const std::byte> data)
	{
		return static_cast<uint32_t>(absl::ExtendCrc32c(absl::crc32c_t{crc}, AsStringView(data)));
	}

} // namespace bitcask
