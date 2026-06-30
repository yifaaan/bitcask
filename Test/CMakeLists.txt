find_package(GTest CONFIG REQUIRED)

add_executable(bitcask_tests
    TestIndex/TestBTreeIndex.cpp
)

target_link_libraries(bitcask_tests PRIVATE
    bitcask_lib
    GTest::gtest
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(bitcask_tests)
