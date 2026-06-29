# Repository Guidelines

## Project Structure & Module Organization

This repository is a C++23 Bitcask storage engine project built with CMake. Core library code lives under `Source/`; the current implemented module is `Source/FIO`, containing `IOManager`, `FileIO`, and `MmapIO`. Tests are organized under `Test/` by subsystem, such as `TestCore`, `TestDB`, `TestIndex`, `TestRedis`, and `TestRESP`. Design notes, diagrams, and data format references live in `Docs/`. CMake output should stay in `build/` or the preset binary directories under `build/cmake/`.

## Build, Test, and Development Commands

- `cmake --preset windows-msvc`: configure a Windows MSVC build with vcpkg dependencies.
- `cmake --build --preset windows-msvc-debug`: build the Debug configuration on Windows.
- `cmake --preset linux-debug && cmake --build --preset linux-debug`: configure and build on Linux.
- `ctest --preset windows-msvc-debug`: run tests for the Windows Debug preset.
- `ctest --preset linux-debug`: run tests for the Linux Debug preset.

For the existing Visual Studio build tree, `cmake --build build --config Debug --target ALL_BUILD -j 8 --` is also valid.

## Coding Style & Naming Conventions

Use the checked-in `.clang-format` file. The project follows Microsoft C++ formatting with tabs, 4-space tab width, all namespaces indented, and left-aligned pointers/references. Keep code in namespace `bitcask`. Use PascalCase for classes and public methods (`FileIO::Open`, `IOManager::Read`), and prefer clear lower-case member names consistent with the surrounding file. New code should use C++23 facilities where appropriate, RAII for ownership, and `absl::Status` / `absl::StatusOr` for recoverable errors.

## Testing Guidelines

Enable tests with the provided presets; they set `BITCASK_BUILD_TESTS=ON`. Place new tests in the matching subsystem directory under `Test/`, for example FIO tests should go near the file I/O module once that test target exists. Name tests after observable behavior, such as `FileIO_AppendsAndReadsAtOffset`. Run the relevant `ctest --preset ...` command before submitting changes.

## Commit & Pull Request Guidelines

Recent commits use short imperative summaries such as `Implement File IO`, `Add Redis Set`, and `Update documentation and configuration for Redis server integration`. Keep the first line concise and action-oriented. Pull requests should describe the change, list affected modules, mention tests run, and link related issues. Include screenshots only for documentation diagram changes.