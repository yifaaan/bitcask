#include "iterator.h"

#include <absl/status/statusor.h>

#include <shared_mutex>

#include "db.h"

namespace bitcask
{
    Iterator::Iterator(DB* db, std::unique_ptr<IndexIterator> index_iter, IteratorOptions options) : db_(db), index_iter_(std::move(index_iter)), options_(std::move(options))
    {
    }

    void Iterator::Rewind()
    {
        index_iter_->Rewind();
        SkipToNext();
    }

    void Iterator::Seek(absl::string_view key)
    {
        index_iter_->Seek(key);
        SkipToNext();
    }

    void Iterator::Next()
    {
        index_iter_->Next();
        SkipToNext();
    }

    [[nodiscard]] bool Iterator::Valid() const
    {
        return index_iter_->Valid();
    }

    [[nodiscard]] absl::string_view Iterator::Key() const
    {
        return index_iter_->Key();
    }

    [[nodiscard]] std::optional<std::string> Iterator::Value() const
    {
        auto pos = index_iter_->Value();
        absl::ReaderMutexLock lock(db_->mutex_);
        if (auto res = db_->GetValueByPosition(pos); res.ok())
        {
            return res.value();
        }
        return std::nullopt;
    }

    
    void Iterator::SkipToNext()
    {
        if (options_.prefix.empty())
        {
            return;
        }
       for (; index_iter_->Valid(); index_iter_->Next())
       {
            const auto key = index_iter_->Key();
            if (key.starts_with(options_.prefix))
            {
                break;
            }
       }

    }
}
