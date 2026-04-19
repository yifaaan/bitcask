#include <parallel_hashmap/btree.h>
#include <cstdint>
#include <iostream>
#include <string>

int main() {
    phmap::btree_map<std::string, std::string> kv;

    kv["hello"] = "world";
    kv["bitcask"] = "storage engine";

    for (const auto& [key, value] : kv) {
        std::cout << key << " -> " << value << '\n';
    }

    return 0;
}
