#pragma once

#include "Data/LogRecord.h"

#include <cstddef>
#include <memory>
#include <string_view>

#include <absl/status/status.h>
#include <absl/status/statusor.h>

namespace bitcask
{
	enum class IndexType
	{
		FlatHash,
		BTree,
	};

	class Index
	{
	public:
		virtual ~Index() = default;

		virtual absl::Status Put(std::string_view key, const LogRecordPos& pos) = 0;
		virtual absl::StatusOr<LogRecordPos> Get(std::string_view key) const = 0;
		virtual absl::Status Delete(std::string_view key) = 0;

		virtual size_t Size() const = 0;
		virtual void Clear() = 0;
	};

	std::unique_ptr<Index> CreateIndex(IndexType type);
} // namespace bitcask
