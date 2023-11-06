#include <iostream>
#include <cstdint>
#include <cstring>
#include <cassert>

#include "utility.h"

// -------------------------------------------------------------------------

void BestPossible(unsigned char* dst, uint64_t size)
{
    BENCHMARK("Memset speed: ", size, std::memset, dst, '1', size);
}

// -------------------------------------------------------------------------

int main()
{
    constexpr uint64_t size = 10'000'000;

    alignas(8) unsigned char* dst = new unsigned char[size]{};

    BestPossible(dst, size);

    delete[] dst;

    return 0;
}