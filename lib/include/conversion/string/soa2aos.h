#pragma once

#include <arrow/array/array_binary.h>
#include <arrow/type_traits.h>
#include <cstdint>
#include <cassert>

#include "aos/types/string.h"
#include "simd/memcpy.h"


static inline void BasicCopy(
    const std::shared_ptr<arrow::StringArray>& sp, uint8_t* output,
    std::shared_ptr<StringBuffer>& external_buf,
    uint64_t aos_struct_sz, uint64_t index)
{
    auto view = sp->GetView(index);

    if (external_buf->IsEmbedded(view.size()))
    {
        *(output + aos_struct_sz * index) = view.size();
        simd_utils::memcpy(output + 1 + aos_struct_sz * index, view.data(), view.size());
    }
    else
    {
        *(output + aos_struct_sz * index) = 255;
        uint8_t* external_output = external_buf->GetBuffer() + external_buf->GetLength();
        *(reinterpret_cast<uint16_t*>(external_output)) = view.size();
        simd_utils::memcpy(external_output + sizeof(uint16_t), view.data(), view.size());
        external_buf->GetLength() += sizeof(uint16_t) + view.size();
        simd_utils::memcpy(output + 1 + aos_struct_sz * index, &external_output, sizeof(external_output));
    }
}


template <typename T, arrow::enable_if_string<T, bool> = true>
void SoA2AoSx4(
    const std::shared_ptr<arrow::Array>& p1,
    const std::shared_ptr<arrow::Array>& p2,
    const std::shared_ptr<arrow::Array>& p3,
    const std::shared_ptr<arrow::Array>& p4,
    AoS& aos,
    uint64_t start_pos)
{
    auto sp1 = std::dynamic_pointer_cast<arrow::StringArray>(p1);
    auto sp2 = std::dynamic_pointer_cast<arrow::StringArray>(p2);
    auto sp3 = std::dynamic_pointer_cast<arrow::StringArray>(p3);
    auto sp4 = std::dynamic_pointer_cast<arrow::StringArray>(p4);

    auto external_buf1 = std::dynamic_pointer_cast<StringBuffer>(aos.GetExtBuffer(start_pos));
    auto external_buf2 = std::dynamic_pointer_cast<StringBuffer>(aos.GetExtBuffer(start_pos + 1));
    auto external_buf3 = std::dynamic_pointer_cast<StringBuffer>(aos.GetExtBuffer(start_pos + 2));
    auto external_buf4 = std::dynamic_pointer_cast<StringBuffer>(aos.GetExtBuffer(start_pos + 3));

    uint64_t aos_str_size1 = aos.GetFieldSize(start_pos);
    uint64_t aos_str_size2 = aos.GetFieldSize(start_pos + 1);
    uint64_t aos_str_size3 = aos.GetFieldSize(start_pos + 2);

    uint64_t datalen = aos.GetLength();
    uint64_t aos_struct_sz = aos.GetStructSize();
    uint8_t* output = aos.GetBuffer() + aos.GetOffset(start_pos);

    uint64_t acc_offset = 0;
    for (uint64_t i = 0; i < datalen; ++i)
    {
        BasicCopy(sp1, output + acc_offset, external_buf1, aos_struct_sz, i);
        acc_offset += aos_str_size1;
        BasicCopy(sp2, output + acc_offset, external_buf2, aos_struct_sz, i);
        acc_offset += aos_str_size2;
        BasicCopy(sp3, output + acc_offset, external_buf3, aos_struct_sz, i);
        acc_offset += aos_str_size3;
        BasicCopy(sp4, output + acc_offset, external_buf4, aos_struct_sz, i);
        acc_offset = 0;
    }
}

template <typename T, arrow::enable_if_string<T, bool> = true>
void SoA2AoSx3(
    const std::shared_ptr<arrow::Array>& p1,
    const std::shared_ptr<arrow::Array>& p2,
    const std::shared_ptr<arrow::Array>& p3,
    AoS& aos,
    uint64_t start_pos)
{
    auto sp1 = std::dynamic_pointer_cast<arrow::StringArray>(p1);
    auto sp2 = std::dynamic_pointer_cast<arrow::StringArray>(p2);
    auto sp3 = std::dynamic_pointer_cast<arrow::StringArray>(p3);

    auto external_buf1 = std::dynamic_pointer_cast<StringBuffer>(aos.GetExtBuffer(start_pos));
    auto external_buf2 = std::dynamic_pointer_cast<StringBuffer>(aos.GetExtBuffer(start_pos + 1));
    auto external_buf3 = std::dynamic_pointer_cast<StringBuffer>(aos.GetExtBuffer(start_pos + 2));

    uint64_t aos_str_size1 = aos.GetFieldSize(start_pos);
    uint64_t aos_str_size2 = aos.GetFieldSize(start_pos + 1);

    uint64_t datalen = aos.GetLength();
    uint64_t aos_struct_sz = aos.GetStructSize();
    uint8_t* output = aos.GetBuffer() + aos.GetOffset(start_pos);

    uint64_t acc_offset = 0;
    for (uint64_t i = 0; i < datalen; ++i)
    {
        BasicCopy(sp1, output + acc_offset, external_buf1, aos_struct_sz, i);
        acc_offset += aos_str_size1;
        BasicCopy(sp2, output + acc_offset, external_buf2, aos_struct_sz, i);
        acc_offset += aos_str_size2;
        BasicCopy(sp3, output + acc_offset, external_buf3, aos_struct_sz, i);
        acc_offset = 0;
    }
}

template <typename T, arrow::enable_if_string<T, bool> = true>
void SoA2AoSx2(
    const std::shared_ptr<arrow::Array>& p1,
    const std::shared_ptr<arrow::Array>& p2,
    AoS& aos,
    uint64_t start_pos)
{
    auto sp1 = std::dynamic_pointer_cast<arrow::StringArray>(p1);
    auto sp2 = std::dynamic_pointer_cast<arrow::StringArray>(p2);

    auto external_buf1 = std::dynamic_pointer_cast<StringBuffer>(aos.GetExtBuffer(start_pos));
    auto external_buf2 = std::dynamic_pointer_cast<StringBuffer>(aos.GetExtBuffer(start_pos + 1));

    uint64_t aos_str_size1 = aos.GetFieldSize(start_pos);

    uint64_t datalen = aos.GetLength();
    uint64_t aos_struct_sz = aos.GetStructSize();
    uint8_t* output = aos.GetBuffer() + aos.GetOffset(start_pos);

    uint64_t acc_offset = 0;
    for (uint64_t i = 0; i < datalen; ++i)
    {
        BasicCopy(sp1, output + acc_offset, external_buf1, aos_struct_sz, i);
        acc_offset += aos_str_size1;
        BasicCopy(sp2, output + acc_offset, external_buf2, aos_struct_sz, i);
        acc_offset = 0;
    }
}

template <typename T, arrow::enable_if_string<T, bool> = true>
void SoA2AoSx1(
    const std::shared_ptr<arrow::Array>& p1,
    AoS& aos,
    uint64_t start_pos)
{
    auto sp1 = std::dynamic_pointer_cast<arrow::StringArray>(p1);

    auto external_buf1 = std::dynamic_pointer_cast<StringBuffer>(aos.GetExtBuffer(start_pos));

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
