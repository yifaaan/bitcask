cmake_minimum_required(VERSION 3.21)

if(NOT DEFINED CMAKE_TOOLCHAIN_FILE AND DEFINED ENV{VCPKG_ROOT})
    file(TO_CMAKE_PATH "$ENV{VCPKG_ROOT}" VCPKG_ROOT)
    set(CMAKE_TOOLCHAIN_FILE "${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" CACHE STRING "Vcpkg toolchain file")
endif()

project(bitcask VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_path(PARALLEL_HASHMAP_INCLUDE_DIRS "parallel_hashmap/btree.h")
find_package(spdlog CONFIG REQUIRED)

add_executable(bitcask src/main.cpp
    src/btree_index.cpp)
target_include_directories(bitcask PRIVATE
    # ${CMAKE_SOURCE_DIR}/include
    ${PARALLEL_HASHMAP_INCLUDE_DIRS}
)
target_link_libraries(bitcask PRIVATE spdlog::spdlog)

add_subdirectory(tests)