#pragma once

#include <chrono>
#include <iostream>

#define BENCHMARK(msg, bytes, function, args...)                                                                \
    do                                                                                                          \
    {                                                                                                           \
        using namespace std::chrono_literals;                                                                   \
        using duration = std::chrono::duration<double>;                                                         \
        constexpr auto GB = 1'000'000'000l;                                                                     \
        auto start_time = std::chrono::high_resolution_clock::now();                                            \
        function(args);                                                                                         \
        auto end_time = std::chrono::high_resolution_clock::now();                                              \
        duration time = end_time - start_time;                                                                  \
        std::cerr << msg                                                                                        \
                  << static_cast<double>(bytes) / (GB * time.count()) << " [GB/sec]"                            \
                  << std::endl;                                                                                 \
    } while (0)
