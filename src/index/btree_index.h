#pragma once

#include <absl/container/btree_map.h>
#include <absl/strings/string_view.h>

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

        bool Put(absl::string_view key, LogRecordPos pos) override;
        std::optional<LogRecordPos> Get(absl::string_view key) const override;
        bool Delete(absl::string_view key) override;

        std::unique_ptr<IndexIterator> Iterator(bool reverse = false) const override;
        size_t size() const override;
    private:
        absl::btree_map<std::string, LogRecordPos> index_;
        mutable std::shared_mutex mutex_;
    };

    std::unique_ptr<BTreeIndex> CreateBTreeIndex();

} // namespace bitcask
