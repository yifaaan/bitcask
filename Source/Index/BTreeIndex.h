#pragma once

#include "Data/LogRecord.h"
#include "Index.h"

#include <absl/container/btree_map.h>
#include <absl/status/statusor.h>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>


namespace bitcask
{

	class BTreeIndex final : public Index
	{
	public:
		BTreeIndex() = default;
		~BTreeIndex() override = default;

		std::optional<LogRecordPos> Put(std::string_view key, const LogRecordPos& pos) override;
		absl::StatusOr<LogRecordPos> Get(std::string_view key) const override;
		std::optional<LogRecordPos> Delete(std::string_view key) override;

		size_t Size() const override;
		void Clear() override;
		std::unique_ptr<IndexIterator> NewIterator() const override;

	private:
		mutable std::shared_mutex mutex;
		absl::btree_map<std::string, LogRecordPos> btree;
	};

	class BTreeIndexIterator final : public IndexIterator
	{
	public:
		explicit BTreeIndexIterator(std::vector<IndexEntry> entries)
			: entries(std::move(entries))
		{
			Rewind();
		}

		void Rewind() override
		{
			index = 0;
		}

		void Next() override
		{
			++index;
		}

		void Prev() override
		{
			--index;
		}

		void Seek(std::string_view target) override
		{
			index = std::ranges::lower_bound(entries, target, {}, &IndexEntry::key) - entries.begin();
		}

		bool Valid() const override
		{
			return index < entries.size();
		}

		std::string_view Key() const override
		{
			return entries[index].key;
		}

		const LogRecordPos& Value() const override
		{
			return entries[index].pos;
		}

	private:
		std::vector<IndexEntry> entries;
		size_t index = 0;
	};

} // namespace bitcask
