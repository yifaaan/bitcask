find_package(Catch2 3 CONFIG REQUIRED)

add_executable(bitcask_tests
    test_file_io.cpp
    test_index.cpp
    test_art_index.cpp
    test_data_file.cpp
    test_db.cpp
    test_http_server.cpp
    test_merge.cpp
    test_write_batch.cpp
)

target_include_directories(bitcask_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${PARALLEL_HASHMAP_INCLUDE_DIRS}
)

target_link_libraries(bitcask_tests PRIVATE
    bitcask_lib
    bitcask_http_lib
    Catch2::Catch2WithMain
)

include(Catch)
catch_discover_tests(bitcask_tests)
