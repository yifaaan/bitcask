#include "IOManager.h"

#include "FileIO.h"

#include <absl/status/status.h>
#include <absl/status/statusor.h>

#include <memory>
#include <string_view>

namespace bitcask
{

	absl::StatusOr<std::unique_ptr<IOManager>> bitcask::IOManager::NewIOManager(std::string_view filePath, IOType type)
	{
		switch (type)
		{
		case IOType::Standard:
			return FileIO::Open(filePath);
		}
		return absl::InvalidArgumentError("unknown IOType");
	}

} // namespace bitcask
