#pragma once

#include "index/index.h"


#include <memory>
#include <optional>
#include <string>

#include "options.h"

namespace bitcask
{

    class DB;

    // 用户迭代器接口
    class Iterator final
    {
    public:
        Iterator(DB* db, std::unique_ptr<IndexIterator> index_iter, IteratorOptions options);

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
        // 跳过不匹配前缀的项
        void SkipToNext();

        DB* db_;
        std::unique_ptr<IndexIterator> index_iter_;
        IteratorOptions options_;
    };
}
