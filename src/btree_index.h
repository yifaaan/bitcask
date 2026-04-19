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

        void Put(std::string_view key, RecordPos pos) override;
        std::optional<RecordPos> Get(std::string_view key) const override;
        bool Delete(std::string_view key) override;

    private:
        phmap::btree_map<std::string, RecordPos> index_;
        mutable std::shared_mutex mutex_;
    };

} // namespace bitcask