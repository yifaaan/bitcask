#pragma once

#include <parallel_hashmap/btree.h>
#include <shared_mutex>
#include <string>

#include "index.h"

namespace bitcask
{

    class BTreeIndex final : public Indexer
    {
    public:
        BTreeIndex() = default;
        ~BTreeIndex() override = default;

        bool Put(std::string_view key, LogRecordPos pos) override;
        std::optional<LogRecordPos> Get(std::string_view key) const override;
        bool Delete(std::string_view key) override;

    private:
        phmap::btree_map<std::string, LogRecordPos> index_;
        mutable std::shared_mutex mutex_;
    };

    std::unique_ptr<BTreeIndex> CreateBTreeIndex();

} // namespace bitcask
