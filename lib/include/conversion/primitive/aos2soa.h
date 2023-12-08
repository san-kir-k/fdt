#pragma once

#include <arrow/type_traits.h>
#include <cstring>


template <typename T, arrow::enable_if_number<T, bool> = true>
void AoS2SoAx4(
    const uint8_t* input, uint64_t insz,
    uint8_t* p1, uint8_t* p2, uint8_t* p3, uint8_t* p4,
    uint64_t outsz, uint64_t datalen)
{
    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(insz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(p1 + outsz * i, input + insz * i,           outsz);
        std::memcpy(p2 + outsz * i, input + insz * i + outsz,   outsz);
        std::memcpy(p3 + outsz * i, input + insz * i + 2*outsz, outsz);
        std::memcpy(p4 + outsz * i, input + insz * i + 3*outsz, outsz);
    }
}

template <typename T, arrow::enable_if_number<T, bool> = true>
void AoS2SoAx3(
    const uint8_t* input, uint64_t insz,
    uint8_t* p1, uint8_t* p2, uint8_t* p3,
    uint64_t outsz, uint64_t datalen)
{
    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(insz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(p1 + outsz * i, input + insz * i,           outsz);
        std::memcpy(p2 + outsz * i, input + insz * i + outsz,   outsz);
        std::memcpy(p3 + outsz * i, input + insz * i + 2*outsz, outsz);
    }
}

template <typename T, arrow::enable_if_number<T, bool> = true>
void AoS2SoAx2(
    const uint8_t* input, uint64_t insz,
    uint8_t* p1, uint8_t* p2,
    uint64_t outsz, uint64_t datalen)
{
    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(insz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(p1 + outsz * i, input + insz * i,         outsz);
        std::memcpy(p2 + outsz * i, input + insz * i + outsz, outsz);
    }
}

template <typename T, arrow::enable_if_number<T, bool> = true>
void AoS2SoAx1(
    const uint8_t* input, uint64_t insz,
    uint8_t* p1,
    uint64_t outsz, uint64_t datalen)
{
    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(insz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(p1 + outsz * i, input + insz * i, outsz);
    }
}
