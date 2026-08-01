find_package(GTest CONFIG REQUIRED)

add_executable(bitcask_tests
    TestDB/TestDB.cpp
    TestDB/TestWriteBatch.cpp
    TestIndex/TestBTreeIndex.cpp
    TestData/TestLogRecord.cpp
    TestFIO/TestFileIO.cpp
    TestFIO/TestMmapIO.cpp
)

target_link_libraries(bitcask_tests PRIVATE
    bitcask_lib
    GTest::gtest
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(bitcask_tests)
