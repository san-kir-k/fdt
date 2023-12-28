#pragma once

#include <arrow/array/array_base.h>
#include <arrow/type_traits.h>
#include <cstring>
#include <memory>

#include "simd/memcpy.h"


template <typename T, arrow::enable_if_number<T, bool> = true>
void SoA2AoSx4(
    const std::shared_ptr<arrow::Array>& p1,
    const std::shared_ptr<arrow::Array>& p2,
    const std::shared_ptr<arrow::Array>& p3,
    const std::shared_ptr<arrow::Array>& p4,
    AoS& aos,
    uint64_t start_pos)
{
    const uint8_t* rp1 = p1->data()->GetValues<uint8_t>(1);
    const uint8_t* rp2 = p2->data()->GetValues<uint8_t>(1);
    const uint8_t* rp3 = p3->data()->GetValues<uint8_t>(1);
    const uint8_t* rp4 = p4->data()->GetValues<uint8_t>(1);

    uint64_t datalen = aos.GetLength();
    uint64_t aos_field_type_sz = aos.GetFieldSize(start_pos);
    uint64_t aos_struct_sz = aos.GetStructSize();
    uint8_t* output = aos.GetBuffer() + aos.GetOffset(start_pos);

    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(aos_field_type_sz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        simd_utils::memcpy(output + aos_struct_sz * i,                       rp1 + aos_field_type_sz * i, aos_field_type_sz);
        simd_utils::memcpy(output + aos_struct_sz * i + aos_field_type_sz,   rp2 + aos_field_type_sz * i, aos_field_type_sz);
        simd_utils::memcpy(output + aos_struct_sz * i + 2*aos_field_type_sz, rp3 + aos_field_type_sz * i, aos_field_type_sz);
        simd_utils::memcpy(output + aos_struct_sz * i + 3*aos_field_type_sz, rp4 + aos_field_type_sz * i, aos_field_type_sz);
    }
}

template <typename T, arrow::enable_if_number<T, bool> = true>
void SoA2AoSx3(
    const std::shared_ptr<arrow::Array>& p1,
    const std::shared_ptr<arrow::Array>& p2,
    const std::shared_ptr<arrow::Array>& p3,
    AoS& aos,
    uint64_t start_pos)
{
    const uint8_t* rp1 = p1->data()->GetValues<uint8_t>(1);
    const uint8_t* rp2 = p2->data()->GetValues<uint8_t>(1);
    const uint8_t* rp3 = p3->data()->GetValues<uint8_t>(1);

    uint64_t datalen = aos.GetLength();
    uint64_t aos_field_type_sz = aos.GetFieldSize(start_pos);
    uint64_t aos_struct_sz = aos.GetStructSize();
    uint8_t* output = aos.GetBuffer() + aos.GetOffset(start_pos);

    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(aos_field_type_sz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        simd_utils::memcpy(output + aos_struct_sz * i,                       rp1 + aos_field_type_sz * i, aos_field_type_sz);
        simd_utils::memcpy(output + aos_struct_sz * i + aos_field_type_sz,   rp2 + aos_field_type_sz * i, aos_field_type_sz);
        simd_utils::memcpy(output + aos_struct_sz * i + 2*aos_field_type_sz, rp3 + aos_field_type_sz * i, aos_field_type_sz);
    }
}

template <typename T, arrow::enable_if_number<T, bool> = true>
void SoA2AoSx2(
    const std::shared_ptr<arrow::Array>& p1,
    const std::shared_ptr<arrow::Array>& p2,
    AoS& aos,
    uint64_t start_pos)
{
    const uint8_t* rp1 = p1->data()->GetValues<uint8_t>(1);
    const uint8_t* rp2 = p2->data()->GetValues<uint8_t>(1);

    uint64_t datalen = aos.GetLength();
    uint64_t aos_field_type_sz = aos.GetFieldSize(start_pos);
    uint64_t aos_struct_sz = aos.GetStructSize();
    uint8_t* output = aos.GetBuffer() + aos.GetOffset(start_pos);

    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(aos_field_type_sz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        simd_utils::memcpy(output + aos_struct_sz * i,                     rp1 + aos_field_type_sz * i, aos_field_type_sz);
        simd_utils::memcpy(output + aos_struct_sz * i + aos_field_type_sz, rp2 + aos_field_type_sz * i, aos_field_type_sz);
    }
}

template <typename T, arrow::enable_if_number<T, bool> = true>
void SoA2AoSx1(
    const std::shared_ptr<arrow::Array>& p1,
    AoS& aos,
    uint64_t start_pos)
{
    const uint8_t* rp1 = p1->data()->GetValues<uint8_t>(1);

    uint64_t datalen = aos.GetLength();
    uint64_t aos_field_type_sz = aos.GetFieldSize(start_pos);
    uint64_t aos_struct_sz = aos.GetStructSize();
    uint8_t* output = aos.GetBuffer() + aos.GetOffset(start_pos);

    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(aos_field_type_sz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        simd_utils::memcpy(output + aos_struct_sz * i, rp1 + aos_field_type_sz * i, aos_field_type_sz);
    }
}
