find_package(Catch2 3 CONFIG REQUIRED)

add_executable(bitcask_tests
    test_file_io.cpp
    test_index.cpp
    test_data_file.cpp
    test_db.cpp
    ${CMAKE_SOURCE_DIR}/src/data/log_record.cpp
    ${CMAKE_SOURCE_DIR}/src/index/index.cpp
    ${CMAKE_SOURCE_DIR}/src/index/btree_index.cpp
    ${CMAKE_SOURCE_DIR}/src/fio/file_io.cpp
    ${CMAKE_SOURCE_DIR}/src/db.cpp
)

target_include_directories(bitcask_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${PARALLEL_HASHMAP_INCLUDE_DIRS}
)

target_link_libraries(bitcask_tests PRIVATE
    Catch2::Catch2WithMain
    absl::status absl::crc32c absl::btree absl::strings protobuf::libprotobuf
)

include(CTest)
include(Catch)
catch_discover_tests(bitcask_tests)
