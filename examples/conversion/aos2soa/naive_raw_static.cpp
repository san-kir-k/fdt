#include <iostream>
#include <cstdint>
#include <cstring>
#include <cassert>

#include "utility.h"
#include "raw_static/aos2soa.h"

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

void Naive(uint8_t* src, SoA& dst, uint64_t size)
{
    std::vector<SoAField> table = {
        SoAField{.ptr = reinterpret_cast<uint8_t*>(dst.a), .size = sizeof(int)},
        SoAField{.ptr = reinterpret_cast<uint8_t*>(dst.b), .size = sizeof(int)},
        SoAField{.ptr = reinterpret_cast<uint8_t*>(dst.x), .size = sizeof(char)},
        SoAField{.ptr = reinterpret_cast<uint8_t*>(dst.y), .size = sizeof(char)},
        SoAField{.ptr = reinterpret_cast<uint8_t*>(dst.z), .size = sizeof(char)},
        SoAField{.ptr = reinterpret_cast<uint8_t*>(dst.n), .size = sizeof(double)},
    };

    BENCHMARK("AoS -> SoA: Naive static speed: ", size * (sizeof(char) * 3 + sizeof(int) * 2 + sizeof(double)), AoS2SoA, src, table, size);
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

    alignas(8) uint8_t* aos = new unsigned char[size * (sizeof(char) * 3 + sizeof(int) * 2 + sizeof(double))]{};

    Naive(aos, soa, size);

    delete[] aos;

    delete[] soa.a;
    delete[] soa.b;
    delete[] soa.x;
    delete[] soa.y;
    delete[] soa.z;
    delete[] soa.n;

    return 0;
}