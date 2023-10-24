#include <iostream>
#include <cstdint>
#include <cstring>
#include <cassert>

#include "utility.h"

// -------------------------------------------------------------------------

struct AoS
{
    uint64_t x{};
    uint64_t y{};
    uint64_t z{};
};

struct SoA
{
    uint64_t* x{};
    uint64_t* y{};
    uint64_t* z{};    
};

// -------------------------------------------------------------------------

inline void SetSoA(SoA& dst, uint64_t size, unsigned char c)
{
    memset(dst.x, c, size * sizeof(uint64_t));
    memset(dst.y, c, size * sizeof(uint64_t));
    memset(dst.z, c, size * sizeof(uint64_t));
}

inline void SetAoS(AoS* dst, uint64_t size, unsigned char c)
{
    memset(dst, c, size * sizeof(AoS));
}

// -------------------------------------------------------------------------

// функция для проверки того, что при конвертации значения не испортились
bool ValidateEq(const AoS* aos, const SoA& soa, uint64_t size)
{
    for (uint64_t i = 0; i < size; ++i)
    {
        if (!(aos[i].x == soa.x[i] && aos[i].y == soa.y[i] && aos[i].z == soa.z[i]))
        {
            return false;
        }
    }
    return true;
}

// -------------------------------------------------------------------------

// имитация конвертации через memset
void BestPossibleAoSToSoA(const AoS*, SoA& dst, uint64_t size, unsigned char c)
{
    memset(dst.x, c, size * sizeof(uint64_t));
    memset(dst.y, c, size * sizeof(uint64_t));
    memset(dst.z, c, size * sizeof(uint64_t));
}

void BestPossibleSoAToAoS(const SoA&, AoS* dst, uint64_t size, unsigned char c)
{
    memset(dst, c, size * sizeof(AoS));
}

// -------------------------------------------------------------------------

void BestPossible(SoA& soa, AoS* aos, uint64_t size)
{
    SetAoS(aos, size, 2);
    BENCHMARK("BestPossible AoS to SoA transformation: ", BestPossibleAoSToSoA, aos, soa, size, 2);
    assert(ValidateEq(aos, soa, size));

    SetSoA(soa, size, 3);
    BENCHMARK("BestPossible SoA to AoS transformation: ", BestPossibleSoAToAoS, soa, aos, size, 3);
    assert(ValidateEq(aos, soa, size));
}

// -------------------------------------------------------------------------

int main()
{
    constexpr uint64_t size = 10'000'000;

    auto* aos = new AoS[size]{};

    SoA soa;
    soa.x = new uint64_t[size]{};
    soa.y = new uint64_t[size]{};
    soa.z = new uint64_t[size]{};

    BestPossible(soa, aos, size);

    delete[] aos;

    delete[] soa.x;
    delete[] soa.y;
    delete[] soa.z;

    return 0;
}