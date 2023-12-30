#pragma once

#include <arrow/array/array_nested.h>
#include <arrow/type_traits.h>
#include <cstdint>
#include <cassert>

#include "aos/types/list.h"
#include "simd/memcpy.h"


static inline void BasicCopy(
    uint8_t* rp, uint32_t* ro,
    const std::shared_ptr<ListBuffer>& external_buf,
    uint64_t length)
{
    uint8_t* external_input = external_buf->GetBuffer();
    for (uint64_t i = 0; i < length; ++i)
    {
        uint16_t size = *(reinterpret_cast<const uint16_t*>(external_input));
        ro[i + 1] = ro[i] + size;
        simd_utils::memcpy(rp, external_input + sizeof(uint16_t), size * external_buf->GetElementSize());
        external_input += size * external_buf->GetElementSize() + sizeof(uint16_t);
        rp += size * external_buf->GetElementSize();
    }
}


template <typename T, arrow::enable_if_list_type<T, bool> = true>
void AoS2SoAx4(
    const AoS& aos,
    std::shared_ptr<arrow::ArrayData>& p1,
    std::shared_ptr<arrow::ArrayData>& p2,
    std::shared_ptr<arrow::ArrayData>& p3,
    std::shared_ptr<arrow::ArrayData>& p4,
    uint64_t start_pos)
{
    uint8_t* rp1 = p1->child_data[0]->GetMutableValues<uint8_t>(1);
    uint8_t* rp2 = p2->child_data[0]->GetMutableValues<uint8_t>(1);
    uint8_t* rp3 = p3->child_data[0]->GetMutableValues<uint8_t>(1);
    uint8_t* rp4 = p4->child_data[0]->GetMutableValues<uint8_t>(1);

    uint32_t* ro1 = p1->GetMutableValues<uint32_t>(1);
    uint32_t* ro2 = p2->GetMutableValues<uint32_t>(1);
    uint32_t* ro3 = p3->GetMutableValues<uint32_t>(1);
    uint32_t* ro4 = p4->GetMutableValues<uint32_t>(1);

    auto external_buf1 = std::dynamic_pointer_cast<ListBuffer>(aos.GetExtBuffer(start_pos));
    auto external_buf2 = std::dynamic_pointer_cast<ListBuffer>(aos.GetExtBuffer(start_pos + 1));
    auto external_buf3 = std::dynamic_pointer_cast<ListBuffer>(aos.GetExtBuffer(start_pos + 2));
    auto external_buf4 = std::dynamic_pointer_cast<ListBuffer>(aos.GetExtBuffer(start_pos + 3));

    uint64_t datalen = aos.GetLength();

    BasicCopy(rp1, ro1, external_buf1, datalen);
    BasicCopy(rp2, ro2, external_buf2, datalen);
    BasicCopy(rp3, ro3, external_buf3, datalen);
    BasicCopy(rp4, ro4, external_buf4, datalen);
}

template <typename T, arrow::enable_if_list_type<T, bool> = true>
void AoS2SoAx3(
    const AoS& aos,
    std::shared_ptr<arrow::ArrayData>& p1,
    std::shared_ptr<arrow::ArrayData>& p2,
    std::shared_ptr<arrow::ArrayData>& p3,
    uint64_t start_pos)
{
    uint8_t* rp1 = p1->child_data[0]->GetMutableValues<uint8_t>(1);
    uint8_t* rp2 = p2->child_data[0]->GetMutableValues<uint8_t>(1);
    uint8_t* rp3 = p3->child_data[0]->GetMutableValues<uint8_t>(1);

    uint32_t* ro1 = p1->GetMutableValues<uint32_t>(1);
    uint32_t* ro2 = p2->GetMutableValues<uint32_t>(1);
    uint32_t* ro3 = p3->GetMutableValues<uint32_t>(1);

    auto external_buf1 = std::dynamic_pointer_cast<ListBuffer>(aos.GetExtBuffer(start_pos));
    auto external_buf2 = std::dynamic_pointer_cast<ListBuffer>(aos.GetExtBuffer(start_pos + 1));
    auto external_buf3 = std::dynamic_pointer_cast<ListBuffer>(aos.GetExtBuffer(start_pos + 2));

    uint64_t datalen = aos.GetLength();

    BasicCopy(rp1, ro1, external_buf1, datalen);
    BasicCopy(rp2, ro2, external_buf2, datalen);
    BasicCopy(rp3, ro3, external_buf3, datalen);
}

template <typename T, arrow::enable_if_list_type<T, bool> = true>
void AoS2SoAx2(
    const AoS& aos,
    std::shared_ptr<arrow::ArrayData>& p1,
    std::shared_ptr<arrow::ArrayData>& p2,
    uint64_t start_pos)
{
    uint8_t* rp1 = p1->child_data[0]->GetMutableValues<uint8_t>(1);
    uint8_t* rp2 = p2->child_data[0]->GetMutableValues<uint8_t>(1);

    uint32_t* ro1 = p1->GetMutableValues<uint32_t>(1);
    uint32_t* ro2 = p2->GetMutableValues<uint32_t>(1);

    auto external_buf1 = std::dynamic_pointer_cast<ListBuffer>(aos.GetExtBuffer(start_pos));
    auto external_buf2 = std::dynamic_pointer_cast<ListBuffer>(aos.GetExtBuffer(start_pos + 1));

    uint64_t datalen = aos.GetLength();

    BasicCopy(rp1, ro1, external_buf1, datalen);
    BasicCopy(rp2, ro2, external_buf2, datalen);
}

template <typename T, arrow::enable_if_list_type<T, bool> = true>
void AoS2SoAx1(
    const AoS& aos,
    std::shared_ptr<arrow::ArrayData>& p1,
    uint64_t start_pos)
{
    uint8_t* rp1 = p1->child_data[0]->GetMutableValues<uint8_t>(1);

    uint32_t* ro1 = p1->GetMutableValues<uint32_t>(1);

    auto external_buf1 = std::dynamic_pointer_cast<ListBuffer>(aos.GetExtBuffer(start_pos));

    uint64_t datalen = aos.GetLength();

    BasicCopy(rp1, ro1, external_buf1, datalen);
}