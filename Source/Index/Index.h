#pragma once

#include "Data/LogRecord.h"

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <utility>

namespace bitcask
{
	enum class IndexType
	{
		FlatHash,
		BTree,
	};

	class IndexIterator;

	class Index
	{
	public:
		virtual ~Index() = default;

		virtual std::optional<LogRecordPos> Put(std::string_view key, const LogRecordPos& pos) = 0;
		virtual absl::StatusOr<LogRecordPos> Get(std::string_view key) const = 0;
		virtual std::optional<LogRecordPos> Delete(std::string_view key) = 0;

		virtual std::unique_ptr<IndexIterator> NewIterator() const = 0;

		virtual size_t Size() const = 0;
		virtual void Clear() = 0;
	};

	std::unique_ptr<Index> NewIndex(IndexType type);

	struct IndexEntry
	{
		std::string key;
		LogRecordPos pos;
	};

	class IndexIterator
	{
	public:
		virtual ~IndexIterator() = default;

		// 定位到第一个 key >= target 的位置；若不存在则 Valid() 变为 false。
		virtual void Seek(std::string_view target) = 0;

		virtual void Next() = 0;
		virtual void Prev() = 0;
		virtual void Rewind() = 0;

		virtual bool Valid() const = 0;
		virtual std::string_view Key() const = 0;
		virtual const LogRecordPos& Value() const = 0;
	};

	struct IteratorOptions
	{
		std::string prefix;
		bool reverse = false;
	};

	class Iterator
	{
	public:
		using ValueReader = std::function<absl::StatusOr<std::string>(const LogRecordPos&)>;

		Iterator(std::unique_ptr<IndexIterator> iter, IteratorOptions options,
				 ValueReader reader = ValueReader())
			: iter(std::move(iter)), options(std::move(options)), reader(std::move(reader))
		{
			if (!this->options.prefix.empty() || this->options.reverse)
			{
				Rewind();
			}
		}

		void Seek(std::string_view target)
		{
			if (options.reverse)
			{
				iter->Seek(target);
				if (!iter->Valid())
				{
					SeekToLast();
				}
				else if (iter->Key() > target)
				{
					iter->Prev();
				}
				SkipPrefixInReverse();
			}
			else
			{
				iter->Seek(target);
				SkipPrefixInForward();
			}
		}

		void Next()
		{
			if (options.reverse)
			{
				iter->Prev();
				SkipPrefixInReverse();
			}
			else
			{
				iter->Next();
				SkipPrefixInForward();
			}
		}

		void Prev()
		{
			if (options.reverse)
			{
				iter->Next();
				SkipPrefixInForward();
			}
			else
			{
				iter->Prev();
				SkipPrefixInReverse();
			}
		}

		void Rewind()
		{
			if (options.reverse)
			{
				SeekToLast();
				SkipPrefixInReverse();
			}
			else
			{
				iter->Rewind();
				SkipPrefixInForward();
			}
		}

		bool Valid() const { return iter->Valid(); }
		std::string_view Key() const { return iter->Key(); }

		absl::StatusOr<std::string> Value() const
		{
			if (!Valid())
			{
				return absl::FailedPreconditionError("iterator is not valid");
			}
			if (!reader)
			{
				return absl::InternalError("no value reader provided");
			}
			return reader(iter->Value());
		}

	private:
		void SkipPrefixInForward()
		{
			while (iter->Valid() && !HasPrefix(iter->Key()))
			{
				iter->Next();
			}
		}

		void SkipPrefixInReverse()
		{
			while (iter->Valid() && !HasPrefix(iter->Key()))
			{
				iter->Prev();
			}
		}

		void SeekToLast()
		{
			iter->Rewind();
			if (!iter->Valid())
			{
				return;
			}
			while (true)
			{
				std::string key(iter->Key());
				iter->Next();
				if (!iter->Valid())
				{
					iter->Seek(key);
					return;
				}
			}
		}

		bool HasPrefix(std::string_view key) const
		{
			auto p = options.prefix;
			return key.size() >= p.size() && key.substr(0, p.size()) == p;
		}

		std::unique_ptr<IndexIterator> iter;
		IteratorOptions options;
		ValueReader reader;
	};

} // namespace bitcask
