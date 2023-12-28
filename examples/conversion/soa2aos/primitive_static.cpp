#include <iostream>
#include <cstdint>
#include <cstring>
#include <cassert>

#include "utility.h"
#include "raw_static/soa2aos.h"

// -------------------------------------------------------------------------

struct SoA
{
    int*    a{};
    int*    b{};
    char*   x{};
    char*   y{};
    char*   z{};
    double* n{};
};

// -------------------------------------------------------------------------

void Naive(SoA& src, uint8_t* dst, uint64_t size)
{
    std::vector<SoAField> table = {
        SoAField{.ptr = reinterpret_cast<uint8_t*>(src.a), .size = sizeof(int)},
        SoAField{.ptr = reinterpret_cast<uint8_t*>(src.b), .size = sizeof(int)},
        SoAField{.ptr = reinterpret_cast<uint8_t*>(src.x), .size = sizeof(char)},
        SoAField{.ptr = reinterpret_cast<uint8_t*>(src.y), .size = sizeof(char)},
        SoAField{.ptr = reinterpret_cast<uint8_t*>(src.z), .size = sizeof(char)},
        SoAField{.ptr = reinterpret_cast<uint8_t*>(src.n), .size = sizeof(double)},
    };

    BENCHMARK_VOID("SoA -> AoS: Primitive static speed: ", size * (sizeof(int) * 2 + sizeof(char) * 3 + sizeof(double)), SoA2AoS, table, dst, size);
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

    uint8_t* dst = new unsigned char[size * (sizeof(int) * 2 + sizeof(char) * 3 + sizeof(double))]{};

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