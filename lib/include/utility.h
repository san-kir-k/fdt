#pragma once

#include <chrono>
#include <iostream>
#include <algorithm>
#include <vector>
#include <numeric>

#define BENCHMARK(msg, bytes, function, args...)                                                                    \
    do                                                                                                              \
    {                                                                                                               \
        using namespace std::chrono_literals;                                                                       \
        using duration = std::chrono::duration<double>;                                                             \
        constexpr auto GB = 1'000'000'000l;                                                                         \
                                                                                                                    \
        int tries = 35;                                                                                             \
        std::vector<double> measures;                                                                               \
        while (tries--)                                                                                             \
        {                                                                                                           \
            auto start_time = std::chrono::high_resolution_clock::now();                                            \
            function(args);                                                                                         \
            auto end_time = std::chrono::high_resolution_clock::now();                                              \
            duration time = end_time - start_time;                                                                  \
            measures.push_back(time.count());                                                                       \
        }                                                                                                           \
        std::sort(measures.begin(), measures.end());                                                                \
        auto avgtime = std::accumulate(measures.begin() + 5, measures.end() - 5, 0.0) / static_cast<double>(25);    \
        std::cerr << msg                                                                                            \
                  << static_cast<double>(bytes) / (GB * avgtime) << " [GB/sec]"                                     \
                  << std::endl;                                                                                     \
    } while (0)
