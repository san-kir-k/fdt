#pragma once

#include <arrow/array/data.h>
#include <arrow/type_traits.h>
#include <cstring>
#include <memory>

#include "simd/memcpy.h"


template <typename T, arrow::enable_if_number<T, bool> = true>
void AoS2SoAx4(
    const AoS& aos,
    std::shared_ptr<arrow::ArrayData>& p1,
    std::shared_ptr<arrow::ArrayData>& p2,
    std::shared_ptr<arrow::ArrayData>& p3,
    std::shared_ptr<arrow::ArrayData>& p4,
    uint64_t start_pos)
{
    uint8_t* rp1 = p1->GetMutableValues<uint8_t>(1);
    uint8_t* rp2 = p2->GetMutableValues<uint8_t>(1);
    uint8_t* rp3 = p3->GetMutableValues<uint8_t>(1);
    uint8_t* rp4 = p4->GetMutableValues<uint8_t>(1);

    uint64_t datalen = aos.GetLength();
    uint64_t arrow_type_sz = aos.GetFieldSize(start_pos);
    uint64_t aos_struct_sz = aos.GetStructSize();
    const uint8_t* input = aos.GetBuffer() + aos.GetOffset(start_pos);

    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(aos_struct_sz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        simd_utils::memcpy(rp1 + arrow_type_sz * i, input + aos_struct_sz * i,                   arrow_type_sz);
        simd_utils::memcpy(rp2 + arrow_type_sz * i, input + aos_struct_sz * i + arrow_type_sz,   arrow_type_sz);
        simd_utils::memcpy(rp3 + arrow_type_sz * i, input + aos_struct_sz * i + 2*arrow_type_sz, arrow_type_sz);
        simd_utils::memcpy(rp4 + arrow_type_sz * i, input + aos_struct_sz * i + 3*arrow_type_sz, arrow_type_sz);
    }
}

template <typename T, arrow::enable_if_number<T, bool> = true>
void AoS2SoAx3(
    const AoS& aos,
    std::shared_ptr<arrow::ArrayData>& p1,
    std::shared_ptr<arrow::ArrayData>& p2,
    std::shared_ptr<arrow::ArrayData>& p3,
    uint64_t start_pos)
{
    uint8_t* rp1 = p1->GetMutableValues<uint8_t>(1);
    uint8_t* rp2 = p2->GetMutableValues<uint8_t>(1);
    uint8_t* rp3 = p3->GetMutableValues<uint8_t>(1);

    uint64_t datalen = aos.GetLength();
    uint64_t arrow_type_sz = aos.GetFieldSize(start_pos);
    uint64_t aos_struct_sz = aos.GetStructSize();
    const uint8_t* input = aos.GetBuffer() + aos.GetOffset(start_pos);

    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(aos_struct_sz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        simd_utils::memcpy(rp1 + arrow_type_sz * i, input + aos_struct_sz * i,                   arrow_type_sz);
        simd_utils::memcpy(rp2 + arrow_type_sz * i, input + aos_struct_sz * i + arrow_type_sz,   arrow_type_sz);
        simd_utils::memcpy(rp3 + arrow_type_sz * i, input + aos_struct_sz * i + 2*arrow_type_sz, arrow_type_sz);
    }
}

template <typename T, arrow::enable_if_number<T, bool> = true>
void AoS2SoAx2(
    const AoS& aos,
    std::shared_ptr<arrow::ArrayData>& p1,
    std::shared_ptr<arrow::ArrayData>& p2,
    uint64_t start_pos)
{
    uint8_t* rp1 = p1->GetMutableValues<uint8_t>(1);
    uint8_t* rp2 = p2->GetMutableValues<uint8_t>(1);

    uint64_t datalen = aos.GetLength();
    uint64_t arrow_type_sz = aos.GetFieldSize(start_pos);
    uint64_t aos_struct_sz = aos.GetStructSize();
    const uint8_t* input = aos.GetBuffer() + aos.GetOffset(start_pos);

    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(aos_struct_sz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        simd_utils::memcpy(rp1 + arrow_type_sz * i, input + aos_struct_sz * i,                 arrow_type_sz);
        simd_utils::memcpy(rp2 + arrow_type_sz * i, input + aos_struct_sz * i + arrow_type_sz, arrow_type_sz);
    }
}

template <typename T, arrow::enable_if_number<T, bool> = true>
void AoS2SoAx1(
    const AoS& aos,
    std::shared_ptr<arrow::ArrayData>& p1,
    uint64_t start_pos)
{
    uint8_t* rp1 = p1->GetMutableValues<uint8_t>(1);

    uint64_t datalen = aos.GetLength();
    uint64_t arrow_type_sz = aos.GetFieldSize(start_pos);
    uint64_t aos_struct_sz = aos.GetStructSize();
    const uint8_t* input = aos.GetBuffer() + aos.GetOffset(start_pos);

    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(aos_struct_sz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        simd_utils::memcpy(rp1 + arrow_type_sz * i, input + aos_struct_sz * i, arrow_type_sz);
    }
}
