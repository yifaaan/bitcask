#include "DataFile.h"

#include <absl/status/status.h>

#include <filesystem>
#include <sstream>
#include <iomanip>

namespace bitcask
{

	namespace
	{

		absl::StatusOr<std::unique_ptr<DataFile>> OpenNamedFile(const std::string& dirPath, const std::string& fileName, uint32_t fid, IOType ioType)
		{
			const auto path = (std::filesystem::path(dirPath) / fileName).string();
			auto io = IOManager::Open(path, ioType);
			if (!io.ok())
			{
				return io.status();
			}

			auto file = std::make_unique<DataFile>();
			file->fid = fid;
			file->io = std::move(*io);

			auto size = file->io->Size();
			if (!size.ok())
			{
				return size.status();
			}
			file->writeOffset = *size;
			return file;
		}
	} // namespace

	std::string DataFileName(uint32_t fid)
	{
		std::ostringstream stream;
		stream << std::setw(9) << std::setfill('0') << fid << kDataFileNameSuffix;
		return stream.str();
	}

	absl::StatusOr<std::unique_ptr<DataFile>> DataFile::Open(const std::string& dirPath, uint32_t fid, IOType ioType)
	{
		return OpenNamedFile(dirPath, DataFileName(fid), fid, ioType);
	}

	absl::Status DataFile::Sync()
	{
		if (!io)
		{
			return absl::FailedPreconditionError("data file is not open");
		}
		return io->Sync();
	}
} // namespace bitcask
