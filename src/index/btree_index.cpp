#include "btree_index.h"

namespace bitcask
{

    bool BTreeIndex::Put(std::string_view key, LogRecordPos pos)
    {
        std::unique_lock lock(mutex_);
        index_[std::string(key)] = pos;
        return true;
    }

    std::optional<LogRecordPos> BTreeIndex::Get(std::string_view key) const
    {
        std::shared_lock lock(mutex_);
        if (auto it = index_.find(std::string(key)); it != index_.end())
        {
            return it->second;
        }
        return std::nullopt;
    }

    bool BTreeIndex::Delete(std::string_view key)
    {
        std::unique_lock lock(mutex_);
        return index_.erase(std::string(key)) > 0;
    }

    std::unique_ptr<BTreeIndex> CreateBTreeIndex()
    {
        return std::make_unique<BTreeIndex>();
    }

} // namespace bitcask
