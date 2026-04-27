#include <cstdint>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>

#include "http_server.h"
#include "index/index.h"
#include "options.h"

namespace
{
    void PrintUsage(const char* program)
    {
        std::cout << "Usage: " << program << " [options]\n"
                  << "\n"
                  << "Options:\n"
                  << "  --data-dir <path>                 DB data directory (default: ./bitcask_data)\n"
                  << "  --host <host>                     HTTP listen host (default: 127.0.0.1)\n"
                  << "  --port <port>                     HTTP listen port (default: 8080)\n"
                  << "  --max-data-file-size <bytes>      Max data file size (default: 10485760)\n"
                  << "  --bytes-per-sync <bytes>          Periodic sync threshold; 0 disables it\n"
                  << "  --auto-merge-reclaim-ratio <0..1> Auto merge reclaim ratio; 0 disables it\n"
                  << "  --sync-on-write                   Sync every write\n"
                  << "  --index <btree|art>               Index implementation (default: btree)\n"
                  << "  --help                            Show this help\n";
    }

    std::optional<std::string> NextValue(int& index, int argc, char* argv[])
    {
        if (index + 1 >= argc)
        {
            return std::nullopt;
        }
        ++index;
        return std::string(argv[index]);
    }

    bool ParsePort(std::string_view text, int& port)
    {
        try
        {
            size_t consumed = 0;
            const auto parsed = std::stoi(std::string(text), &consumed);
            if (consumed != text.size() || parsed <= 0 || parsed > 65535)
            {
                return false;
            }
            port = parsed;
            return true;
        }
        catch (...)
        {
            return false;
        }
    }

    bool ParseUint64(std::string_view text, uint64_t& value)
    {
        try
        {
            size_t consumed = 0;
            const auto parsed = std::stoull(std::string(text), &consumed);
            if (consumed != text.size())
            {
                return false;
            }
            value = parsed;
            return true;
        }
        catch (...)
        {
            return false;
        }
    }

    bool ParseRatio(std::string_view text, double& value)
    {
        try
        {
            size_t consumed = 0;
            const auto parsed = std::stod(std::string(text), &consumed);
            if (consumed != text.size() || parsed < 0 || parsed > 1)
            {
                return false;
            }
            value = parsed;
            return true;
        }
        catch (...)
        {
            return false;
        }
    }
} // namespace

int main(int argc, char* argv[])
{
    bitcask::Options db_options;
    db_options.data_dir = "./bitcask_data";

    bitcask::http::ServerOptions server_options;

    for (int i = 1; i < argc; ++i)
    {
        const std::string arg = argv[i];
        if (arg == "--help" || arg == "-h")
        {
            PrintUsage(argv[0]);
            return 0;
        }
        if (arg == "--sync-on-write")
        {
            db_options.sync_on_write = true;
            continue;
        }

        auto value = NextValue(i, argc, argv);
        if (!value)
        {
            std::cerr << "Missing value for " << arg << '\n';
            PrintUsage(argv[0]);
            return 2;
        }

        if (arg == "--data-dir")
        {
            db_options.data_dir = *value;
        }
        else if (arg == "--host")
        {
            server_options.host = *value;
        }
        else if (arg == "--port")
        {
            if (!ParsePort(*value, server_options.port))
            {
                std::cerr << "Invalid --port value: " << *value << '\n';
                return 2;
            }
        }
        else if (arg == "--max-data-file-size")
        {
            if (!ParseUint64(*value, db_options.max_data_file_size) || db_options.max_data_file_size == 0)
            {
                std::cerr << "Invalid --max-data-file-size value: " << *value << '\n';
                return 2;
            }
        }
        else if (arg == "--bytes-per-sync")
        {
            if (!ParseUint64(*value, db_options.bytes_per_sync))
            {
                std::cerr << "Invalid --bytes-per-sync value: " << *value << '\n';
                return 2;
            }
        }
        else if (arg == "--auto-merge-reclaim-ratio")
        {
            if (!ParseRatio(*value, db_options.auto_merge_reclaim_ratio))
            {
                std::cerr << "Invalid --auto-merge-reclaim-ratio value: " << *value << '\n';
                return 2;
            }
        }
        else if (arg == "--index")
        {
            if (*value == "btree")
            {
                db_options.index_type = bitcask::IndexType::BTree;
            }
            else if (*value == "art")
            {
                db_options.index_type = bitcask::IndexType::ART;
            }
            else
            {
                std::cerr << "Invalid --index value: " << *value << '\n';
                return 2;
            }
        }
        else
        {
            std::cerr << "Unknown option: " << arg << '\n';
            PrintUsage(argv[0]);
            return 2;
        }
    }

    auto db = bitcask::DB::Open(db_options);
    if (!db)
    {
        std::cerr << "Failed to open database at " << db_options.data_dir << '\n';
        return 1;
    }

    std::cout << "bitcask_http listening on http://" << server_options.host << ':' << server_options.port << '\n';
    if (!bitcask::http::Listen(*db, server_options))
    {
        std::cerr << "Failed to listen on " << server_options.host << ':' << server_options.port << '\n';
        return 1;
    }

    return 0;
}
