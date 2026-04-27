cmake_minimum_required(VERSION 3.24)

if(NOT DEFINED CMAKE_TOOLCHAIN_FILE AND DEFINED ENV{VCPKG_ROOT})
    file(TO_CMAKE_PATH "$ENV{VCPKG_ROOT}" VCPKG_ROOT)
    set(CMAKE_TOOLCHAIN_FILE "${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" CACHE STRING "Vcpkg toolchain file")
endif()

project(bitcask VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Default to Release build
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

find_package(absl CONFIG REQUIRED)
find_package(benchmark CONFIG REQUIRED)
find_package(httplib CONFIG REQUIRED)
find_package(protobuf CONFIG REQUIRED)
find_package(tlx CONFIG REQUIRED)

include(FetchContent)
FetchContent_Declare(
    libart
    GIT_REPOSITORY https://github.com/armon/libart.git
    GIT_TAG master
)

# libart has no CMakeLists.txt — write a thin shim and add_subdirectory it
FetchContent_MakeAvailable(libart)
if(NOT TARGET libart)
    # strings.h is POSIX-only; libart only needs string.h functions on this code path
    file(WRITE "${libart_BINARY_DIR}/strings.h" "#include <string.h>\n")
    file(WRITE "${libart_BINARY_DIR}/compat.h"
        "#ifdef _MSC_VER\n"
        "#include <intrin.h>\n"
        "#define __builtin_ctz(x) (int)_tzcnt_u32((unsigned int)(x))\n"
        "#endif\n"
    )
    set_source_files_properties(${libart_SOURCE_DIR}/src/art.c PROPERTIES LANGUAGE CXX)
    add_library(libart STATIC ${libart_SOURCE_DIR}/src/art.c)
    target_include_directories(libart PUBLIC ${libart_SOURCE_DIR}/src ${libart_BINARY_DIR})
    target_compile_definitions(libart PRIVATE _CRT_SECURE_NO_WARNINGS)
    if(MSVC)
        target_compile_options(libart PRIVATE "/FI${libart_BINARY_DIR}/compat.h")
    endif()
endif()

include(CTest)

add_library(bitcask_lib STATIC
    src/fio/file_io.cpp
    src/fio/io_manager.cpp
    src/fio/mmap_io.cpp
    src/data/log_record.cpp
    src/index/index.cpp
    src/index/btree_index.cpp
    src/index/art_index.cpp
    src/db.cpp
    src/iterator.cpp
    src/batch.cpp
    src/types/redis_data_struct.cpp
)

target_include_directories(bitcask_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/src
)
target_compile_options(bitcask_lib PUBLIC $<$<CXX_COMPILER_ID:MSVC>:/utf-8>)
target_link_libraries(bitcask_lib PUBLIC absl::status absl::crc32c absl::btree absl::strings protobuf::libprotobuf libart tlx)

add_library(bitcask_http_lib STATIC
    src/http_server.cpp
)

target_include_directories(bitcask_http_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/src
)
target_link_libraries(bitcask_http_lib PUBLIC bitcask_lib httplib::httplib)

add_executable(bitcask_http src/main.cpp)
target_link_libraries(bitcask_http PRIVATE bitcask_http_lib)

add_subdirectory(benchmarks)
add_subdirectory(tests)
