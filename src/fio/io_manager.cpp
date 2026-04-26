#include "io_manager.h"

#include "file_io.h"
#include "mmap_io.h"

namespace bitcask
{

    std::unique_ptr<IOManager> CreateIOManager(const std::filesystem::path& path)
    {
        return CreateIOManager(path, IOType::Standard);
    }

    std::unique_ptr<IOManager> CreateIOManager(const std::filesystem::path& path, IOType io_type)
    {
        switch (io_type)
        {
        case IOType::Standard:
            return FileIO::Open(path);
        case IOType::MMap:
            return MMapIO::Open(path);
        }
        return nullptr;
    }

} // namespace bitcask
