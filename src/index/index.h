#pragma once

#include <absl/strings/string_view.h>

#include <optional>
#include <memory>

#include "../data/log_record.h"

namespace bitcask
{

    enum class IndexType : uint8_t
    {
        BTree = 1,
        ART = 2,
    };

    class IndexIterator
    {
    public:
        virtual ~IndexIterator() = default;
        virtual void Rewind() = 0;

        // Forward: move to first >= key.
        // Reverse: move to last <= key.
        virtual void Seek(absl::string_view key) = 0;

        virtual void Next() = 0;

        virtual bool Valid() const = 0;

        virtual absl::string_view Key() const = 0;
        virtual LogRecordPos Value() const = 0;
    };

    class Indexer
    {
    public:
        virtual ~Indexer() = default;

        virtual bool Put(absl::string_view key, LogRecordPos pos) = 0;
        virtual std::optional<LogRecordPos> Get(absl::string_view key) const = 0;
        virtual bool Delete(absl::string_view key) = 0;

        virtual std::unique_ptr<IndexIterator> Iterator(bool reverse = false) const = 0;

        virtual size_t size() const = 0;
    };

    std::unique_ptr<Indexer> CreateIndexer(IndexType type);

} // namespace bitcask
