#include <iostream>
#include <cstdint>
#include <cstring>
#include <chrono>
#include <cassert>

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

void BestPossible(SoA& soa, AoS* aos, uint64_t size)
{
    SetAoS(aos, size, 2);
    auto start_time = std::chrono::high_resolution_clock::now();
    BestPossibleAoSToSoA(aos, soa, size, 2);
    auto end_time = std::chrono::high_resolution_clock::now();
    auto time = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "BestPossible AoS to SoA transformation: " << time.count() << " [microsec]" << std::endl;
    assert(ValidateEq(aos, soa, size));

    SetSoA(soa, size, 3);
    start_time = std::chrono::high_resolution_clock::now();
    BestPossibleSoAToAoS(soa, aos, size, 3);
    end_time = std::chrono::high_resolution_clock::now();
    time = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "BestPossible SoA to AoS transformation: " << time.count() << " [microsec]" << std::endl;
    assert(ValidateEq(aos, soa, size));
}

// -------------------------------------------------------------------------

void NaiveAoSToSoA(const AoS* src, SoA& dst, uint64_t size)
{
    for (uint64_t i = 0; i < size; ++i)
    {
        memcpy(&dst.x[i], &src[i].x, sizeof(uint64_t));
        memcpy(&dst.y[i], &src[i].y, sizeof(uint64_t));
        memcpy(&dst.z[i], &src[i].z, sizeof(uint64_t));
    }
}

void NaiveSoAToAoS(const SoA& src, AoS* dst, uint64_t size)
{
    for (uint64_t i = 0; i < size; ++i)
    {
        memcpy(&dst[i].x, &src.x[i], sizeof(uint64_t));
        memcpy(&dst[i].y, &src.y[i], sizeof(uint64_t));
        memcpy(&dst[i].z, &src.z[i], sizeof(uint64_t));
    }
}

void Naive(SoA& soa, AoS* aos, uint64_t size)
{
    SetAoS(aos, size, 2);
    auto start_time = std::chrono::high_resolution_clock::now();
    NaiveAoSToSoA(aos, soa, size);
    auto end_time = std::chrono::high_resolution_clock::now();
    auto time = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "Naive AoS to SoA transformation: " << time.count() << " [microsec]" << std::endl;
    assert(ValidateEq(aos, soa, size));

    SetSoA(soa, size, 3);
    start_time = std::chrono::high_resolution_clock::now();
    NaiveSoAToAoS(soa, aos, size);
    end_time = std::chrono::high_resolution_clock::now();
    time = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "Naive SoA to AoS transformation: " << time.count() << " [microsec]" << std::endl;
    assert(ValidateEq(aos, soa, size));
}

int main()
{
    constexpr uint64_t size = 10'000'000;

    auto* aos = new AoS[size]{};

    SoA soa;
    soa.x = new uint64_t[size]{};
    soa.y = new uint64_t[size]{};
    soa.z = new uint64_t[size]{};

    BestPossible(soa, aos, size);
    Naive(soa, aos, size);

    delete[] aos;

    delete[] soa.x;
    delete[] soa.y;
    delete[] soa.z;

    return 0;
}