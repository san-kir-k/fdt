#pragma once

#include <arrow/array/array_nested.h>
#include <arrow/type_traits.h>
#include <cstdint>
#include <cassert>

#include "aos/types/list.h"
#include "simd/memcpy.h"


static inline void CopyData(
    const std::shared_ptr<arrow::ListArray>& lp,
    std::shared_ptr<ListBuffer>& external_buf,
    uint64_t length)
{
    uint8_t* external_output = external_buf->GetBuffer();
    auto list_data = lp->values()->data();
    const uint8_t* raw_data = list_data->GetValues<uint8_t>(1);
    const auto* raw_offsets = lp->raw_value_offsets();
    uint64_t acc_raw_data_offset = 0;
    for (uint64_t i = 0; i < length; ++i)
    {
        uint64_t size = raw_offsets[i + 1] - raw_offsets[i];
        uint64_t size_to_copy = size * external_buf->GetElementSize();
        *(reinterpret_cast<uint16_t*>(external_output + external_buf->GetLength())) = size;
        simd_utils::memcpy(external_output + external_buf->GetLength() + sizeof(uint16_t), raw_data + acc_raw_data_offset, size_to_copy);
        acc_raw_data_offset += size_to_copy;
        external_buf->GetLength() += sizeof(uint16_t) + size_to_copy;
    }
}

static inline void SetPointers(
    uint8_t* output,
    std::shared_ptr<ListBuffer>& external_buf,
    uint64_t aos_struct_sz, uint64_t length)
{
    uint8_t* external_output = external_buf->GetBuffer();

    for (uint64_t i = 0; i < length; ++i)
    {
        simd_utils::memcpy(output + aos_struct_sz * i, &external_output, sizeof(external_output));
        external_output += *(reinterpret_cast<uint16_t*>(external_output)) * external_buf->GetElementSize() + sizeof(uint16_t);
    }
}

static inline void BasicCopy(
    const std::shared_ptr<arrow::ListArray>& lp, uint8_t* output,
    std::shared_ptr<ListBuffer>& external_buf,
    uint64_t aos_struct_sz, uint64_t length)
{
    CopyData(lp, external_buf, length);
    SetPointers(output, external_buf, aos_struct_sz, length);
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
    BasicCopy(sp1, output + acc_offset, external_buf1, aos_struct_sz, datalen);
    acc_offset += aos_list_size;
    BasicCopy(sp2, output + acc_offset, external_buf2, aos_struct_sz, datalen);
    acc_offset += aos_list_size;
    BasicCopy(sp3, output + acc_offset, external_buf3, aos_struct_sz, datalen);
    acc_offset += aos_list_size;
    BasicCopy(sp4, output + acc_offset, external_buf4, aos_struct_sz, datalen);
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
    BasicCopy(sp1, output + acc_offset, external_buf1, aos_struct_sz, datalen);
    acc_offset += aos_list_size;
    BasicCopy(sp2, output + acc_offset, external_buf2, aos_struct_sz, datalen);
    acc_offset += aos_list_size;
    BasicCopy(sp3, output + acc_offset, external_buf3, aos_struct_sz, datalen);
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
    BasicCopy(sp1, output + acc_offset, external_buf1, aos_struct_sz, datalen);
    acc_offset += aos_list_size;
    BasicCopy(sp2, output + acc_offset, external_buf2, aos_struct_sz, datalen);
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

    BasicCopy(sp1, output, external_buf1, aos_struct_sz, datalen);
}
