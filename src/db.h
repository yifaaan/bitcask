#pragma once

#include <absl/container/btree_map.h>
#include <absl/strings/string_view.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

#include "data/data_file.h"
#include "index/index.h"
#include "iterator.h"
#include "options.h"

namespace bitcask
{

    class DB
    {
    public:
        ~DB() = default;

        DB(const DB&) = delete;
        DB& operator=(const DB&) = delete;
        DB(DB&&) = delete;
        DB& operator=(DB&&) = delete;

        // 打开数据库，加载索引
        static std::unique_ptr<DB> Open(Options options);

        // 写入 key-value 对
        absl::Status Put(absl::string_view key, absl::string_view value);
        // 读取 key 对应的 value
        std::optional<std::string> Get(absl::string_view key);
        // 删除 key
        absl::Status Delete(absl::string_view key);
        // 列出所有 key
        std::vector<std::string> ListKeys();

        // 遍历所有 key-value 对，调用 f 进行处理
        absl::Status Fold(std::function<bool(std::string_view, std::string)> f);

        std::unique_ptr<Iterator> NewIterator(IteratorOptions options = {});
        void Close();
    private:
        explicit DB(Options options);

        friend class Iterator;

        // 将 LogRecord 写入数据文件，并返回记录的位置，方便更新索引
        absl::Status AppendLogRecord(const LogRecord& record, LogRecordPos& pos);

        // 设置当前活跃数据文件
        // 调用该方法时需要持有 mutex_
        absl::Status SetActiveDataFile();

        absl::Status LoadDataFiles();
        absl::Status LoadIndexFromDataFiles();

        absl::StatusOr<std::string> GetValueByPosition(const LogRecordPos& pos);

        Options options_;
        std::unique_ptr<Indexer> index_;
        std::unique_ptr<DataFile> active_file_;
        absl::btree_map<uint32_t, std::unique_ptr<DataFile>> older_files_;
        std::vector<int> file_ids_;
        mutable std::shared_mutex mutex_;
    };

} // namespace bitcask
