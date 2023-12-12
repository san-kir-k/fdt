#pragma once

#include <arrow/array/data.h>
#include <arrow/type_traits.h>
#include <cstring>
#include <memory>


template <typename T, arrow::enable_if_number<T, bool> = true>
void AoS2SoAx4(
    const uint8_t* input, uint64_t insz,
    std::shared_ptr<arrow::ArrayData>& p1,
    std::shared_ptr<arrow::ArrayData>& p2,
    std::shared_ptr<arrow::ArrayData>& p3,
    std::shared_ptr<arrow::ArrayData>& p4,
    uint64_t outsz, uint64_t datalen)
{
    uint8_t* rp1 = p1->GetMutableValues<uint8_t>(1);
    uint8_t* rp2 = p2->GetMutableValues<uint8_t>(1);
    uint8_t* rp3 = p3->GetMutableValues<uint8_t>(1);
    uint8_t* rp4 = p4->GetMutableValues<uint8_t>(1);

    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(insz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(rp1 + outsz * i, input + insz * i,           outsz);
        std::memcpy(rp2 + outsz * i, input + insz * i + outsz,   outsz);
        std::memcpy(rp3 + outsz * i, input + insz * i + 2*outsz, outsz);
        std::memcpy(rp4 + outsz * i, input + insz * i + 3*outsz, outsz);
    }
}

template <typename T, arrow::enable_if_number<T, bool> = true>
void AoS2SoAx3(
    const uint8_t* input, uint64_t insz,
    std::shared_ptr<arrow::ArrayData>& p1,
    std::shared_ptr<arrow::ArrayData>& p2,
    std::shared_ptr<arrow::ArrayData>& p3,
    uint64_t outsz, uint64_t datalen)
{
    uint8_t* rp1 = p1->GetMutableValues<uint8_t>(1);
    uint8_t* rp2 = p2->GetMutableValues<uint8_t>(1);
    uint8_t* rp3 = p3->GetMutableValues<uint8_t>(1);

    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(insz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(rp1 + outsz * i, input + insz * i,           outsz);
        std::memcpy(rp2 + outsz * i, input + insz * i + outsz,   outsz);
        std::memcpy(rp3 + outsz * i, input + insz * i + 2*outsz, outsz);
    }
}

template <typename T, arrow::enable_if_number<T, bool> = true>
void AoS2SoAx2(
    const uint8_t* input, uint64_t insz,
    std::shared_ptr<arrow::ArrayData>& p1,
    std::shared_ptr<arrow::ArrayData>& p2,
    uint64_t outsz, uint64_t datalen)
{
    uint8_t* rp1 = p1->GetMutableValues<uint8_t>(1);
    uint8_t* rp2 = p2->GetMutableValues<uint8_t>(1);

    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(insz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(rp1 + outsz * i, input + insz * i,         outsz);
        std::memcpy(rp2 + outsz * i, input + insz * i + outsz, outsz);
    }
}

template <typename T, arrow::enable_if_number<T, bool> = true>
void AoS2SoAx1(
    const uint8_t* input, uint64_t insz,
    std::shared_ptr<arrow::ArrayData>& p1,
    uint64_t outsz, uint64_t datalen)
{
    uint8_t* rp1 = p1->GetMutableValues<uint8_t>(1);

    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(insz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(rp1 + outsz * i, input + insz * i, outsz);
    }
}
