#pragma once

#include <absl/strings/string_view.h>

#include <atomic>
#include <memory>
#include <shared_mutex>
#include <string>

#include <art.h>

#include "index.h"

namespace bitcask
{

    class AdaptiveRadixTreeIndex final : public Indexer
    {
    public:
        AdaptiveRadixTreeIndex();
        ~AdaptiveRadixTreeIndex() override;

        bool Put(absl::string_view key, LogRecordPos pos) override;
        std::optional<LogRecordPos> Get(absl::string_view key) const override;
        bool Delete(absl::string_view key) override;

        std::unique_ptr<IndexIterator> Iterator(bool reverse = false) const override;
        size_t size() const override;

    private:
        art_tree tree_;
        mutable std::shared_mutex mutex_;
    };

    std::unique_ptr<AdaptiveRadixTreeIndex> CreateAdaptiveRadixTreeIndex();

} // namespace bitcask
