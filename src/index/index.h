#pragma once

#include <parallel_hashmap/btree.h>

#include "../data/log_record.h"

namespace bitcask
{

    // 数据的内存索引，key -> RecordPos
    class Indexer
    {
    public:
        virtual ~Indexer() = default;

        // 向索引中添加或更新 key 对应的 位置信息
        virtual void Put(std::string_view key, LogRecordPos pos) = 0;

        // 从索引中获取 key 对应的 位置信息，如果 key 不存在则返回 std::nullopt
        virtual std::optional<LogRecordPos> Get(std::string_view key) const = 0;

        // 从索引中删除 key 对应的 位置信息，返回是否成功删除
        virtual bool Delete(std::string_view key) = 0;
    };

    std::unique_ptr<Indexer> CreateBTreeIndex();
}
