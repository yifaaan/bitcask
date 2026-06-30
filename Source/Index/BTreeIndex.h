#pragma once

#include "Index.h"

#include <shared_mutex>

#include <absl/container/btree_map.h>

namespace bitcask
{

class BTreeIndex final : public Index
{
public:
	BTreeIndex() = default;
	~BTreeIndex() override = default;

	absl::Status Put(std::string_view key, const LogRecordPos& pos) override;
	absl::StatusOr<LogRecordPos> Get(std::string_view key) const override;
	absl::Status Delete(std::string_view key) override;

	size_t Size() const override;
	void Clear() override;

private:
	mutable std::shared_mutex mutex;
	absl::btree_map<std::string, LogRecordPos> btree;
};

}
