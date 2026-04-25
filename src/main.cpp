#include <iostream>
#include <string>

#include "db.h"

int main()
{
    bitcask::Options opts;
    opts.data_dir = "./bitcask_data";
    opts.max_data_file_size = 1024 * 1024 * 10;

    auto db = bitcask::DB::Open(std::move(opts));
    if (!db)
    {
        std::cerr << "Failed to open database\n";
        return 1;
    }

    auto status = db->Put("hello", "world");
    if (!status)
    {
        std::cerr << "Put failed: " << status.message() << '\n';
    }

    auto value = db->Get("hello");
    if (value)
    {
        std::cout << "hello = " << *value << '\n';
    }

    status = db->Put("bitcask", "storage engine");
    if (!status)
    {
        std::cerr << "Put failed: " << status.message() << '\n';
    }

    value = db->Get("bitcask");
    if (value)
    {
        std::cout << "bitcask = " << *value << '\n';
    }

    status = db->Delete("hello");
    if (!status)
    {
        std::cerr << "Delete failed: " << status.message() << '\n';
    }

    value = db->Get("hello");
    if (!value)
    {
        std::cout << "hello is deleted\n";
    }

    db->Close();
    return 0;
}
