#pragma once

#include <arrow/array/array_binary.h>
#include <arrow/type_traits.h>
#include <cstdint>
#include <cassert>

#include "aos/types/string.h"


static inline void BasicCopy(
    const uint8_t* input, uint8_t*& rp, uint32_t* ro,
    const std::shared_ptr<StringBuffer>& external_buf,
    uint64_t aos_struct_sz, uint64_t index)
{
    if (external_buf->IsEmbedded(*(input + aos_struct_sz * index)))
    {
        uint8_t sz = *(input + aos_struct_sz * index);
        ro[index + 1] = ro[index] + sz;
        std::memcpy(rp, input + 1 + aos_struct_sz * index, sz);
        rp += sz;
    }
    else
    {
        const uint8_t* external_p = *(reinterpret_cast<uint8_t* const*>(input + 1 + aos_struct_sz * index));
        uint16_t sz = *(reinterpret_cast<const uint16_t*>(external_p));
        ro[index + 1] = ro[index] + sz;
        std::memcpy(rp, external_p + sizeof(uint16_t), sz);
        rp += sz;
    }
}


template <typename T, arrow::enable_if_string<T, bool> = true>
void AoS2SoAx4(
    const AoS& aos,
    std::shared_ptr<arrow::ArrayData>& p1,
    std::shared_ptr<arrow::ArrayData>& p2,
    std::shared_ptr<arrow::ArrayData>& p3,
    std::shared_ptr<arrow::ArrayData>& p4,
    uint64_t start_pos)
{
    uint8_t* rp1 = p1->GetMutableValues<uint8_t>(2);
    uint8_t* rp2 = p2->GetMutableValues<uint8_t>(2);
    uint8_t* rp3 = p3->GetMutableValues<uint8_t>(2);
    uint8_t* rp4 = p4->GetMutableValues<uint8_t>(2);

    uint32_t* ro1 = p1->GetMutableValues<uint32_t>(1);
    uint32_t* ro2 = p2->GetMutableValues<uint32_t>(1);
    uint32_t* ro3 = p3->GetMutableValues<uint32_t>(1);
    uint32_t* ro4 = p4->GetMutableValues<uint32_t>(1);

    auto external_buf1 = std::static_pointer_cast<StringBuffer>(aos.GetExtBuffer(start_pos));
    auto external_buf2 = std::static_pointer_cast<StringBuffer>(aos.GetExtBuffer(start_pos + 1));
    auto external_buf3 = std::static_pointer_cast<StringBuffer>(aos.GetExtBuffer(start_pos + 2));
    auto external_buf4 = std::static_pointer_cast<StringBuffer>(aos.GetExtBuffer(start_pos + 3));

    uint64_t aos_str_size1 = aos.GetFieldSize(start_pos);
    uint64_t aos_str_size2 = aos.GetFieldSize(start_pos + 1);
    uint64_t aos_str_size3 = aos.GetFieldSize(start_pos + 2);

    uint64_t datalen = aos.GetLength();
    uint64_t aos_struct_sz = aos.GetStructSize();
    const uint8_t* input = aos.GetBuffer() + aos.GetOffset(start_pos);

    uint64_t acc_offset = 0;
    for (uint64_t i = 0; i < datalen; ++i)
    {
        BasicCopy(input + acc_offset, rp1, ro1, external_buf1, aos_struct_sz, i);
        acc_offset += aos_str_size1;
        BasicCopy(input + acc_offset, rp2, ro2, external_buf2, aos_struct_sz, i);
        acc_offset += aos_str_size2;
        BasicCopy(input + acc_offset, rp3, ro3, external_buf3, aos_struct_sz, i);
        acc_offset += aos_str_size3;
        BasicCopy(input + acc_offset, rp4, ro4, external_buf4, aos_struct_sz, i);
        acc_offset = 0;
    }
}

template <typename T, arrow::enable_if_string<T, bool> = true>
void AoS2SoAx3(
    const AoS& aos,
    std::shared_ptr<arrow::ArrayData>& p1,
    std::shared_ptr<arrow::ArrayData>& p2,
    std::shared_ptr<arrow::ArrayData>& p3,
    uint64_t start_pos)
{
    uint8_t* rp1 = p1->GetMutableValues<uint8_t>(2);
    uint8_t* rp2 = p2->GetMutableValues<uint8_t>(2);
    uint8_t* rp3 = p3->GetMutableValues<uint8_t>(2);

    uint32_t* ro1 = p1->GetMutableValues<uint32_t>(1);
    uint32_t* ro2 = p2->GetMutableValues<uint32_t>(1);
    uint32_t* ro3 = p3->GetMutableValues<uint32_t>(1);

    auto external_buf1 = std::static_pointer_cast<StringBuffer>(aos.GetExtBuffer(start_pos));
    auto external_buf2 = std::static_pointer_cast<StringBuffer>(aos.GetExtBuffer(start_pos + 1));
    auto external_buf3 = std::static_pointer_cast<StringBuffer>(aos.GetExtBuffer(start_pos + 2));

    uint64_t aos_str_size1 = aos.GetFieldSize(start_pos);
    uint64_t aos_str_size2 = aos.GetFieldSize(start_pos + 1);

    uint64_t datalen = aos.GetLength();
    uint64_t aos_struct_sz = aos.GetStructSize();
    const uint8_t* input = aos.GetBuffer() + aos.GetOffset(start_pos);

    uint64_t acc_offset = 0;
    for (uint64_t i = 0; i < datalen; ++i)
    {
        BasicCopy(input + acc_offset, rp1, ro1, external_buf1, aos_struct_sz, i);
        acc_offset += aos_str_size1;
        BasicCopy(input + acc_offset, rp2, ro2, external_buf2, aos_struct_sz, i);
        acc_offset += aos_str_size2;
        BasicCopy(input + acc_offset, rp3, ro3, external_buf3, aos_struct_sz, i);
        acc_offset = 0;
    }
}

template <typename T, arrow::enable_if_string<T, bool> = true>
void AoS2SoAx2(
    const AoS& aos,
    std::shared_ptr<arrow::ArrayData>& p1,
    std::shared_ptr<arrow::ArrayData>& p2,
    uint64_t start_pos)
{
    uint8_t* rp1 = p1->GetMutableValues<uint8_t>(2);
    uint8_t* rp2 = p2->GetMutableValues<uint8_t>(2);

    uint32_t* ro1 = p1->GetMutableValues<uint32_t>(1);
    uint32_t* ro2 = p2->GetMutableValues<uint32_t>(1);

    auto external_buf1 = std::static_pointer_cast<StringBuffer>(aos.GetExtBuffer(start_pos));
    auto external_buf2 = std::static_pointer_cast<StringBuffer>(aos.GetExtBuffer(start_pos + 1));

    uint64_t aos_str_size1 = aos.GetFieldSize(start_pos);

    uint64_t datalen = aos.GetLength();
    uint64_t aos_struct_sz = aos.GetStructSize();
    const uint8_t* input = aos.GetBuffer() + aos.GetOffset(start_pos);

    uint64_t acc_offset = 0;
    for (uint64_t i = 0; i < datalen; ++i)
    {
        BasicCopy(input + acc_offset, rp1, ro1, external_buf1, aos_struct_sz, i);
        acc_offset += aos_str_size1;
        BasicCopy(input + acc_offset, rp2, ro2, external_buf2, aos_struct_sz, i);
        acc_offset = 0;
    }
}

template <typename T, arrow::enable_if_string<T, bool> = true>
void AoS2SoAx1(
    const AoS& aos,
    std::shared_ptr<arrow::ArrayData>& p1,
    uint64_t start_pos)
{
    uint8_t* rp1 = p1->GetMutableValues<uint8_t>(2);

    uint32_t* ro1 = p1->GetMutableValues<uint32_t>(1);

    auto external_buf1 = std::static_pointer_cast<StringBuffer>(aos.GetExtBuffer(start_pos));

    uint64_t datalen = aos.GetLength();
    uint64_t aos_struct_sz = aos.GetStructSize();
    const uint8_t* input = aos.GetBuffer() + aos.GetOffset(start_pos);

    uint64_t acc_offset = 0;
    for (uint64_t i = 0; i < datalen; ++i)
    {
        BasicCopy(input + acc_offset, rp1, ro1, external_buf1, aos_struct_sz, i);
        acc_offset = 0;
    }
}
