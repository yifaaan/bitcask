find_package(GTest CONFIG REQUIRED)

add_executable(bitcask_tests
    TestCore/TestVarint.cpp
    TestData/TestLogRecord.cpp
    TestData/TestDataFile.cpp
)

target_link_libraries(bitcask_tests PRIVATE
    bitcask_lib
    GTest::gtest
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(bitcask_tests)
