#include <iostream>
#include <cstdint>
#include <cstring>
#include <cassert>

#include "utility.h"
#include "raw_transpose.h"

// -------------------------------------------------------------------------

struct SoA
{
    alignas(8) int*    a{};
    alignas(8) int*    b{};
    alignas(8) char*   x{};
    alignas(8) char*   y{};
    alignas(8) char*   z{};
    alignas(8) double* n{};
};

// -------------------------------------------------------------------------

void Naive(SoA& src, unsigned char* dst, uint64_t size)
{
    std::vector<AoSField> table = {
        AoSField{.ptr = reinterpret_cast<unsigned char*>(src.a), .size = sizeof(int)},
        AoSField{.ptr = reinterpret_cast<unsigned char*>(src.b), .size = sizeof(int)},
        AoSField{.ptr = reinterpret_cast<unsigned char*>(src.x), .size = sizeof(char)},
        AoSField{.ptr = reinterpret_cast<unsigned char*>(src.y), .size = sizeof(char)},
        AoSField{.ptr = reinterpret_cast<unsigned char*>(src.z), .size = sizeof(char)},
        AoSField{.ptr = reinterpret_cast<unsigned char*>(src.n), .size = sizeof(double)},
    };

    BENCHMARK("Naive speed: ", size * (sizeof(int) * 2 + sizeof(char) * 3 + sizeof(double)), SoA2AoS, table, dst, size);
}

// -------------------------------------------------------------------------

int main()
{
    constexpr uint64_t size = 10'000'000;

    SoA soa;
    soa.a = new int[size]{};
    soa.b = new int[size]{};
    soa.x = new char[size]{};
    soa.y = new char[size]{};
    soa.z = new char[size]{};
    soa.n = new double[size]{};

    alignas(8) unsigned char* dst = new unsigned char[size * (sizeof(int) * 2 + sizeof(char) * 3 + sizeof(double))]{};

    Naive(soa, dst, size);

    delete[] dst;

    delete[] soa.a;
    delete[] soa.b;
    delete[] soa.x;
    delete[] soa.y;
    delete[] soa.z;
    delete[] soa.n;

    return 0;
}