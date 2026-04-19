find_package(Catch2 3 CONFIG REQUIRED)

add_executable(bitcask_tests
    test_index.cpp
    test_file_io.cpp
    ${CMAKE_SOURCE_DIR}/src/index/btree_index.cpp
    ${CMAKE_SOURCE_DIR}/src/fio/file_io.cpp
)

target_include_directories(bitcask_tests PRIVATE
    # ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
    ${PARALLEL_HASHMAP_INCLUDE_DIRS}
)

target_link_libraries(bitcask_tests PRIVATE
    Catch2::Catch2WithMain
    spdlog::spdlog
)

include(CTest)
include(Catch)
catch_discover_tests(bitcask_tests)
