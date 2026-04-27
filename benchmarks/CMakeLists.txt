add_executable(bitcask_bench
    db_bench.cpp
)

target_link_libraries(bitcask_bench PRIVATE
    bitcask_lib
    benchmark::benchmark
    benchmark::benchmark_main
)
