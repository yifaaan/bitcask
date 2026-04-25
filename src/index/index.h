#pragma once

#include <optional>
#include <memory>
#include <string_view>

#include "../data/log_record.h"

namespace bitcask
{

    enum class IndexType : uint8_t
    {
        BTree = 1,
        ART = 2,
    };

    class Indexer
    {
    public:
        virtual ~Indexer() = default;

        virtual bool Put(std::string_view key, LogRecordPos pos) = 0;
        virtual std::optional<LogRecordPos> Get(std::string_view key) const = 0;
        virtual bool Delete(std::string_view key) = 0;
    };

    std::unique_ptr<Indexer> CreateIndexer(IndexType type);

} // namespace bitcask
