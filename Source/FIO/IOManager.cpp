#include "IOManager.h"
#include "FileIO.h"
#include "MmapIO.h"

namespace bitcask
{

	absl::StatusOr<std::unique_ptr<IOManager>> IOManager::Open(const std::string& path, IOType type)
	{
		switch (type)
		{
		case IOType::Standard:
			return FileIO::Open(path);
		case IOType::MMap:
			return MmapIO::Open(path);
		}
		return absl::InvalidArgumentError("unknown IOType");
	}

} // namespace bitcask
