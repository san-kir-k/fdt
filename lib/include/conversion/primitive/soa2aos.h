#pragma once

#include <arrow/type_traits.h>
#include <cstring>


template <typename T, arrow::enable_if_number<T, bool> = true>
void SoA2AoSx4(
    const uint8_t* p1, const uint8_t* p2,
    const uint8_t* p3, const uint8_t* p4,
    uint64_t insz,
    uint8_t* out, uint64_t outsz, uint64_t datalen)
{
    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(insz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(out + outsz * i,          p1 + insz * i, insz);
        std::memcpy(out + outsz * i + insz,   p2 + insz * i, insz);
        std::memcpy(out + outsz * i + 2*insz, p3 + insz * i, insz);
        std::memcpy(out + outsz * i + 3*insz, p4 + insz * i, insz);
    }
}

template <typename T, arrow::enable_if_number<T, bool> = true>
void SoA2AoSx3(
    const uint8_t* p1, const uint8_t* p2, const uint8_t* p3,
    uint64_t insz,
    uint8_t* out, uint64_t outsz, uint64_t datalen)
{
    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(insz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(out + outsz * i,          p1 + insz * i, insz);
        std::memcpy(out + outsz * i + insz,   p2 + insz * i, insz);
        std::memcpy(out + outsz * i + 2*insz, p3 + insz * i, insz);
    }
}

template <typename T, arrow::enable_if_number<T, bool> = true>
void SoA2AoSx2(
    const uint8_t* p1, const uint8_t* p2,
    uint64_t insz,
    uint8_t* out, uint64_t outsz, uint64_t datalen)
{
    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(insz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(out + outsz * i,        p1 + insz * i, insz);
        std::memcpy(out + outsz * i + insz, p2 + insz * i, insz);
    }
}

template <typename T, arrow::enable_if_number<T, bool> = true>
void SoA2AoSx1(
    const uint8_t* p1,
    uint64_t insz,
    uint8_t* out, uint64_t outsz, uint64_t datalen)
{
    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(insz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(out + outsz * i, p1 + insz * i, insz);
    }
}
