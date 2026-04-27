#include <benchmark/benchmark.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "batch.h"
#include "db.h"
#include "options.h"

namespace
{
    constexpr auto kDefaultValueSize = 128;
    constexpr auto kGetKeyCount = 10000;

    std::atomic<uint64_t> g_dir_counter = 0;

    std::filesystem::path MakeBenchDir(std::string_view name)
    {
        const auto id = g_dir_counter.fetch_add(1, std::memory_order_relaxed);
        return std::filesystem::temp_directory_path() / ("bitcask_bench_" + std::string(name) + "_" + std::to_string(id));
    }

    class BenchDB
    {
    public:
        explicit BenchDB(std::string_view name)
            : dir_(MakeBenchDir(name))
        {
            std::filesystem::remove_all(dir_);
            std::filesystem::create_directories(dir_);

            bitcask::Options options;
            options.data_dir = dir_;
            options.max_data_file_size = 64 * 1024 * 1024;
            options.bytes_per_sync = 0;
            options.sync_on_write = false;

            db_ = bitcask::DB::Open(options);
        }

        ~BenchDB()
        {
            if (db_)
            {
                db_->Close();
            }
            std::filesystem::remove_all(dir_);
        }

        BenchDB(const BenchDB&) = delete;
        BenchDB& operator=(const BenchDB&) = delete;
        BenchDB(BenchDB&&) = delete;
        BenchDB& operator=(BenchDB&&) = delete;

        bitcask::DB* get() const { return db_.get(); }

    private:
        std::filesystem::path dir_;
        std::unique_ptr<bitcask::DB> db_;
    };

    std::string MakeKey(uint64_t index)
    {
        return "key:" + std::to_string(index);
    }

    std::vector<std::string> MakeKeys(int64_t count)
    {
        std::vector<std::string> keys;
        keys.reserve(static_cast<size_t>(count));
        for (int64_t i = 0; i < count; ++i)
        {
            keys.push_back(MakeKey(static_cast<uint64_t>(i)));
        }
        return keys;
    }

    bool EnsureOpen(benchmark::State& state, const BenchDB& bench_db)
    {
        if (bench_db.get() != nullptr)
        {
            return true;
        }
        state.SkipWithError("failed to open benchmark database");
        return false;
    }

    void ReportThroughput(benchmark::State& state, int64_t item_count, size_t value_size)
    {
        state.SetItemsProcessed(item_count);
        state.SetBytesProcessed(item_count * static_cast<int64_t>(value_size));
    }
} // namespace

static void BM_DBPutOverwrite(benchmark::State& state)
{
    BenchDB bench_db("put_overwrite");
    if (!EnsureOpen(state, bench_db))
    {
        return;
    }

    const std::string value(static_cast<size_t>(state.range(0)), 'v');
    for (auto _ : state)
    {
        const auto status = bench_db.get()->Put("key", value);
        if (!status.ok())
        {
            state.SkipWithError(status.ToString().c_str());
            break;
        }
    }

    ReportThroughput(state, state.iterations(), value.size());
}

static void BM_DBPutUniqueKeys(benchmark::State& state)
{
    BenchDB bench_db("put_unique");
    if (!EnsureOpen(state, bench_db))
    {
        return;
    }

    const std::string value(static_cast<size_t>(state.range(0)), 'v');
    uint64_t index = 0;
    for (auto _ : state)
    {
        const auto key = MakeKey(index++);
        const auto status = bench_db.get()->Put(key, value);
        if (!status.ok())
        {
            state.SkipWithError(status.ToString().c_str());
            break;
        }
    }

    ReportThroughput(state, state.iterations(), value.size());
}

static void BM_DBGet(benchmark::State& state)
{
    BenchDB bench_db("get");
    if (!EnsureOpen(state, bench_db))
    {
        return;
    }

    const auto keys = MakeKeys(state.range(0));
    const std::string value(kDefaultValueSize, 'v');
    for (const auto& key : keys)
    {
        const auto status = bench_db.get()->Put(key, value);
        if (!status.ok())
        {
            state.SkipWithError(status.ToString().c_str());
            return;
        }
    }

    uint64_t index = 0;
    for (auto _ : state)
    {
        const auto result = bench_db.get()->Get(keys[index++ % keys.size()]);
        if (!result)
        {
            state.SkipWithError("failed to read preloaded key");
            break;
        }
        benchmark::DoNotOptimize(*result);
    }

    ReportThroughput(state, state.iterations(), value.size());
}

static void BM_DBDeleteExistingKey(benchmark::State& state)
{
    BenchDB bench_db("delete");
    if (!EnsureOpen(state, bench_db))
    {
        return;
    }

    const std::string value(kDefaultValueSize, 'v');
    uint64_t index = 0;
    for (auto _ : state)
    {
        state.PauseTiming();
        const auto key = MakeKey(index++);
        auto status = bench_db.get()->Put(key, value);
        if (!status.ok())
        {
            state.SkipWithError(status.ToString().c_str());
            break;
        }
        state.ResumeTiming();

        status = bench_db.get()->Delete(key);
        if (!status.ok())
        {
            state.SkipWithError(status.ToString().c_str());
            break;
        }
    }

    state.SetItemsProcessed(state.iterations());
}

static void BM_WriteBatchCommit(benchmark::State& state)
{
    BenchDB bench_db("write_batch");
    if (!EnsureOpen(state, bench_db))
    {
        return;
    }

    const auto batch_size = static_cast<int64_t>(state.range(0));
    const std::string value(kDefaultValueSize, 'v');
    uint64_t index = 0;
    for (auto _ : state)
    {
        bitcask::WriteBatch batch(bench_db.get(), {});
        for (int64_t i = 0; i < batch_size; ++i)
        {
            const auto status = batch.Put(MakeKey(index++), value);
            if (!status.ok())
            {
                state.SkipWithError(status.ToString().c_str());
                return;
            }
        }

        const auto status = batch.Commit();
        if (!status.ok())
        {
            state.SkipWithError(status.ToString().c_str());
            break;
        }
    }

    ReportThroughput(state, state.iterations() * batch_size, value.size());
}

BENCHMARK(BM_DBPutOverwrite)->Arg(kDefaultValueSize);
BENCHMARK(BM_DBPutUniqueKeys)->Arg(kDefaultValueSize);
BENCHMARK(BM_DBGet)->Arg(kGetKeyCount);
BENCHMARK(BM_DBDeleteExistingKey);
BENCHMARK(BM_WriteBatchCommit)->Arg(100);
