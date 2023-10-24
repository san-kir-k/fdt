#pragma once

#include <chrono>
#include <iostream>

#define BENCHMARK(msg, function, args...)                                                                       \
    do                                                                                                          \
    {                                                                                                           \
        auto start_time = std::chrono::high_resolution_clock::now();                                            \
        function(args);                                                                                         \
        auto end_time = std::chrono::high_resolution_clock::now();                                              \
        auto time = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);               \
        std::cerr << msg << time.count() << " [microsec]" << std::endl;                                         \
    } while (0)
