cmake_minimum_required(VERSION 3.24)

project(bitcask VERSION 0.2.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Default to Release build
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# === Dependencies ===
find_package(absl CONFIG REQUIRED)

# === Bitcask Core Library ===
add_library(bitcask_lib STATIC
    Source/Data/Varint.cpp
    Source/Data/LogRecord.cpp
    Source/Data/DataFile.cpp
    Source/DB/DB.cpp
    Source/FIO/IOManager.cpp
    Source/FIO/FileIO.cpp
    Source/Index/BTreeIndex.cpp
 )

target_include_directories(bitcask_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/Source
)

target_compile_options(bitcask_lib PUBLIC
    $<$<CXX_COMPILER_ID:MSVC>:/utf-8>
    $<$<CXX_COMPILER_ID:MSVC>:/Zc:__cplusplus>
)

target_link_libraries(bitcask_lib PUBLIC
    absl::crc32c
    absl::status
    absl::statusor
    absl::strings
)


# === Tests (optional) ===
option(BITCASK_BUILD_TESTS "Build tests" OFF)
if(BITCASK_BUILD_TESTS)
    enable_testing()
    add_subdirectory(Test)
endif()
