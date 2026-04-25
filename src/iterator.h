#pragma once

#include "index/index.h"
#include "options.h"

class DB;

namespace bitcask
{

    class Iterator final
    {
    public:
        Iterator(DB& db, std::unique_ptr<IndexIterator> index_iter, IteratorOptions options);

        Iterator(const Iterator&) = delete;
        Iterator& operator=(const Iterator&) = delete;

        Iterator(Iterator&&) noexcept = default;
        Iterator& operator=(Iterator&&) noexcept = default;

        void Rewind();
        void Seek(absl::string_view key);
        void Next();

        [[nodiscard]] bool Valid() const;
        [[nodiscard]] absl::string_view Key() const;
        [[nodiscard]] std::optional<std::string> Value() const;

    private:
        void SkipToNext();
        [[nodiscard]] bool MatchPrefix() const;

        DB* db_;
        std::unique_ptr<IndexIterator> index_iter_;
        IteratorOptions options_;
    };
}