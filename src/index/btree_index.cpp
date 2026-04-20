#include "btree_index.h"

namespace bitcask
{
    Status BTree::Put(std::string_view key, LogRecordPos pos)
    {
        std::unique_lock lock(mutex_);
        index_[std::string(key)] = pos;
        return Status::Ok();
    }

    std::optional<LogRecordPos> BTree::Get(std::string_view key) const
    {
        std::shared_lock lock(mutex_);
        if (auto it = index_.find(std::string(key)); it != index_.end())
        {
            return it->second;
        }
        return std::nullopt;
    }

    Status BTree::Delete(std::string_view key)
    {
        std::unique_lock lock(mutex_);
        return index_.erase(std::string(key)) > 0 ? Status::Ok() : Status::NotFound("Key not found in index");
    }

    std::unique_ptr<BTree> CreateBTreeIndex()
    {
        return std::make_unique<BTree>();
    }
} // namespace bitcask
