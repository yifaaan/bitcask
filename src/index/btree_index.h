#pragma once

#include <parallel_hashmap/btree.h>

#include "index.h"

namespace bitcask
{
    class BTree final : public Indexer
    {
    public:
        BTree() = default;
        ~BTree() override = default;

        Status Put(std::string_view key, LogRecordPos pos) override;
        std::optional<LogRecordPos> Get(std::string_view key) const override;
        Status Delete(std::string_view key) override;

    private:
        phmap::btree_map<std::string, LogRecordPos> index_;
        mutable std::shared_mutex mutex_;
    };

    std::unique_ptr<BTree> CreateBTreeIndex();

} // namespace bitcask