#pragma once

#include <arrow/array/array_nested.h>
#include <arrow/type_traits.h>
#include <cstdint>
#include <cassert>

#include "aos/types/list.h"
#include "simd/memcpy.h"


static inline void BasicCopy(
    const std::shared_ptr<arrow::ListArray>& lp, uint8_t* output,
    std::shared_ptr<ListBuffer>& external_buf,
    uint64_t aos_struct_sz, uint64_t index)
{
    auto slice = lp->value_slice(index);
    uint8_t* external_output = external_buf->GetBuffer() + external_buf->GetLength();
    *(reinterpret_cast<uint16_t*>(external_output)) = slice->length();
    auto slice_data = slice->data();
    simd_utils::memcpy(external_output + sizeof(uint16_t), slice_data->GetValues<uint8_t>(1, lp->value_offset(index) * external_buf->GetElementSize()), slice->length() * external_buf->GetElementSize());
    external_buf->GetLength() += sizeof(uint16_t) + slice->length() * external_buf->GetElementSize();
    external_buf->GetElementsCount() += slice->length();
    simd_utils::memcpy(output + aos_struct_sz * index, &external_output, sizeof(external_output));
}


template <typename T, arrow::enable_if_list_type<T, bool> = true>
void SoA2AoSx4(
    const std::shared_ptr<arrow::Array>& p1,
    const std::shared_ptr<arrow::Array>& p2,
    const std::shared_ptr<arrow::Array>& p3,
    const std::shared_ptr<arrow::Array>& p4,
    AoS& aos,
    uint64_t start_pos)
{
    auto sp1 = std::dynamic_pointer_cast<arrow::ListArray>(p1);
    auto sp2 = std::dynamic_pointer_cast<arrow::ListArray>(p2);
    auto sp3 = std::dynamic_pointer_cast<arrow::ListArray>(p3);
    auto sp4 = std::dynamic_pointer_cast<arrow::ListArray>(p4);

    auto external_buf1 = std::dynamic_pointer_cast<ListBuffer>(aos.GetExtBuffer(start_pos));
    auto external_buf2 = std::dynamic_pointer_cast<ListBuffer>(aos.GetExtBuffer(start_pos + 1));
    auto external_buf3 = std::dynamic_pointer_cast<ListBuffer>(aos.GetExtBuffer(start_pos + 2));
    auto external_buf4 = std::dynamic_pointer_cast<ListBuffer>(aos.GetExtBuffer(start_pos + 3));

    uint64_t aos_list_size = aos.GetFieldSize(start_pos);

    uint64_t datalen = aos.GetLength();
    uint64_t aos_struct_sz = aos.GetStructSize();
    uint8_t* output = aos.GetBuffer() + aos.GetOffset(start_pos);

    uint64_t acc_offset = 0;
    for (uint64_t i = 0; i < datalen; ++i)
    {
        BasicCopy(sp1, output + acc_offset, external_buf1, aos_struct_sz, i);
        acc_offset += aos_list_size;
        BasicCopy(sp2, output + acc_offset, external_buf2, aos_struct_sz, i);
        acc_offset += aos_list_size;
        BasicCopy(sp3, output + acc_offset, external_buf3, aos_struct_sz, i);
        acc_offset += aos_list_size;
        BasicCopy(sp4, output + acc_offset, external_buf4, aos_struct_sz, i);
        acc_offset = 0;
    }
}

template <typename T, arrow::enable_if_list_type<T, bool> = true>
void SoA2AoSx3(
    const std::shared_ptr<arrow::Array>& p1,
    const std::shared_ptr<arrow::Array>& p2,
    const std::shared_ptr<arrow::Array>& p3,
    AoS& aos,
    uint64_t start_pos)
{
    auto sp1 = std::dynamic_pointer_cast<arrow::ListArray>(p1);
    auto sp2 = std::dynamic_pointer_cast<arrow::ListArray>(p2);
    auto sp3 = std::dynamic_pointer_cast<arrow::ListArray>(p3);

    auto external_buf1 = std::dynamic_pointer_cast<ListBuffer>(aos.GetExtBuffer(start_pos));
    auto external_buf2 = std::dynamic_pointer_cast<ListBuffer>(aos.GetExtBuffer(start_pos + 1));
    auto external_buf3 = std::dynamic_pointer_cast<ListBuffer>(aos.GetExtBuffer(start_pos + 2));

    uint64_t aos_list_size = aos.GetFieldSize(start_pos);

    uint64_t datalen = aos.GetLength();
    uint64_t aos_struct_sz = aos.GetStructSize();
    uint8_t* output = aos.GetBuffer() + aos.GetOffset(start_pos);

    uint64_t acc_offset = 0;
    for (uint64_t i = 0; i < datalen; ++i)
    {
        BasicCopy(sp1, output + acc_offset, external_buf1, aos_struct_sz, i);
        acc_offset += aos_list_size;
        BasicCopy(sp2, output + acc_offset, external_buf2, aos_struct_sz, i);
        acc_offset += aos_list_size;
        BasicCopy(sp3, output + acc_offset, external_buf3, aos_struct_sz, i);
        acc_offset = 0;
    }
}

template <typename T, arrow::enable_if_list_type<T, bool> = true>
void SoA2AoSx2(
    const std::shared_ptr<arrow::Array>& p1,
    const std::shared_ptr<arrow::Array>& p2,
    AoS& aos,
    uint64_t start_pos)
{
    auto sp1 = std::dynamic_pointer_cast<arrow::ListArray>(p1);
    auto sp2 = std::dynamic_pointer_cast<arrow::ListArray>(p2);

    auto external_buf1 = std::dynamic_pointer_cast<ListBuffer>(aos.GetExtBuffer(start_pos));
    auto external_buf2 = std::dynamic_pointer_cast<ListBuffer>(aos.GetExtBuffer(start_pos + 1));

    uint64_t aos_list_size = aos.GetFieldSize(start_pos);

    uint64_t datalen = aos.GetLength();
    uint64_t aos_struct_sz = aos.GetStructSize();
    uint8_t* output = aos.GetBuffer() + aos.GetOffset(start_pos);

    uint64_t acc_offset = 0;
    for (uint64_t i = 0; i < datalen; ++i)
    {
        BasicCopy(sp1, output + acc_offset, external_buf1, aos_struct_sz, i);
        acc_offset += aos_list_size;
        BasicCopy(sp2, output + acc_offset, external_buf2, aos_struct_sz, i);
        acc_offset = 0;
    }
}

template <typename T, arrow::enable_if_list_type<T, bool> = true>
void SoA2AoSx1(
    const std::shared_ptr<arrow::Array>& p1,
    AoS& aos,
    uint64_t start_pos)
{
    auto sp1 = std::dynamic_pointer_cast<arrow::ListArray>(p1);

    auto external_buf1 = std::dynamic_pointer_cast<ListBuffer>(aos.GetExtBuffer(start_pos));

    uint64_t datalen = aos.GetLength();
    uint64_t aos_struct_sz = aos.GetStructSize();
    uint8_t* output = aos.GetBuffer() + aos.GetOffset(start_pos);

    uint64_t acc_offset = 0;
    for (uint64_t i = 0; i < datalen; ++i)
    {
        BasicCopy(sp1, output + acc_offset, external_buf1, aos_struct_sz, i);
        acc_offset = 0;
    }
}
