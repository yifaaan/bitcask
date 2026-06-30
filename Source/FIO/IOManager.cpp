#include "IOManager.h"
#include "FileIO.h"
#include "MmapIO.h"

#include <absl/status/status.h>

#include <limits>
#include <vector>

namespace bitcask
{

	absl::StatusOr<std::unique_ptr<IOManager>> IOManager::Open(const std::string& path, IOType type)
	{
		switch (type)
		{
		case IOType::Standard:
			return FileIO::Open(path);
		}
		return absl::InvalidArgumentError("unknown IOType");
	}

	absl::StatusOr<ReadLogRecordResult> IOManager::ReadLogRecord(int64_t offset)
	{
		
	}

} // namespace bitcask
