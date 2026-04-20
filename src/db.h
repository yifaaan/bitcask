#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>

#include "data/data_file.h"
#include "index/index.h"
#include "status.h"
#include "options.h"

namespace bitcask
{

    class DB
    {
    public:
        ~DB() = default;

        DB(const DB&) = delete;
        auto operator=(const DB&) -> DB& = delete;
        DB(DB&&) = delete;
        auto operator=(DB&&) -> DB& = delete;

        // 打开数据库，加载索引
        static std::unique_ptr<DB> Open(Options options);

        // 写入 key-value 对
        Status Put(std::string_view key, std::string_view value);

        // 读取 key 对应的 value
        std::optional<std::string> Get(std::string_view key) const;

        // 删除 key
        Status Delete(std::string_view key);

        void Close();

    private:
        DB(Options options);

        // 将 LogRecord 写入数据文件，并返回记录的位置，方便更新索引
        Status AppendLogRecord(const LogRecord& record, LogRecordPos& pos);
        // 设置当前活跃数据文件
        // 调用该方法时需要持有 mutex_
        Status SetActiveDataFile();

        mutable std::shared_mutex mutex_;
        Options options_;
        std::unique_ptr<Indexer> index_;
        std::unique_ptr<DataFile> active_data_file_;
        phmap::btree_map<uint32_t, std::unique_ptr<DataFile>> old_data_files_;
    };

} // namespace bitcask
