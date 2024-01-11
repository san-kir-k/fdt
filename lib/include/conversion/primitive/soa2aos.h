#pragma once

#include <arrow/array/array_base.h>
#include <arrow/type_traits.h>
#include <cstring>
#include <memory>

#include <arm_neon.h>
#include <simd/memcpy.h>
#include <simd/store_lanes.h>


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

    static const void* gotoTable[] = {&&b1, &&b2, &&b4, &&b8};
    goto *gotoTable[std::countr_zero(aos_field_type_sz)];

    b1:
    {
        uint64_t count_of_blocks = datalen / 16;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint8x16x4_t from;
            from.val[0] = vld1q_u8(rp1 + 16 * i);
            from.val[1] = vld1q_u8(rp2 + 16 * i);
            from.val[2] = vld1q_u8(rp3 + 16 * i);
            from.val[3] = vld1q_u8(rp4 + 16 * i);

            FOR_16_STORE_LANES(vst4q_lane_u8, output + aos_struct_sz * 16 * i, from, aos_struct_sz, uint8_t);
        }

        for (uint64_t i = count_of_blocks * 16; i < datalen; ++i)
        {
            simd_utils::memcpy(output + aos_struct_sz * i,                       rp1 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + aos_field_type_sz,   rp2 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + 2*aos_field_type_sz, rp3 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + 3*aos_field_type_sz, rp4 + aos_field_type_sz * i, aos_field_type_sz);
        }

        goto end;
    }
    b2:
    {
        uint64_t count_of_blocks = datalen / 8;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint16x8x4_t from;
            from.val[0] = vreinterpretq_u16_u8(vld1q_u8(rp1 + 16 * i));
            from.val[1] = vreinterpretq_u16_u8(vld1q_u8(rp2 + 16 * i));
            from.val[2] = vreinterpretq_u16_u8(vld1q_u8(rp3 + 16 * i));
            from.val[3] = vreinterpretq_u16_u8(vld1q_u8(rp4 + 16 * i));

            FOR_8_STORE_LANES(vst4q_lane_u16, output + aos_struct_sz * 8 * i, from, aos_struct_sz, uint16_t);
        }

        for (uint64_t i = count_of_blocks * 8; i < datalen; ++i)
        {
            simd_utils::memcpy(output + aos_struct_sz * i,                       rp1 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + aos_field_type_sz,   rp2 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + 2*aos_field_type_sz, rp3 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + 3*aos_field_type_sz, rp4 + aos_field_type_sz * i, aos_field_type_sz);
        }

        goto end;
    }
    b4:
    {
        uint64_t count_of_blocks = datalen / 4;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint32x4x4_t from;
            from.val[0] = vreinterpretq_u32_u8(vld1q_u8(rp1 + 16 * i));
            from.val[1] = vreinterpretq_u32_u8(vld1q_u8(rp2 + 16 * i));
            from.val[2] = vreinterpretq_u32_u8(vld1q_u8(rp3 + 16 * i));
            from.val[3] = vreinterpretq_u32_u8(vld1q_u8(rp4 + 16 * i));

            FOR_4_STORE_LANES(vst4q_lane_u32, output + aos_struct_sz * 4 * i, from, aos_struct_sz, uint32_t);
        }

        for (uint64_t i = count_of_blocks * 4; i < datalen; ++i)
        {
            simd_utils::memcpy(output + aos_struct_sz * i,                       rp1 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + aos_field_type_sz,   rp2 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + 2*aos_field_type_sz, rp3 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + 3*aos_field_type_sz, rp4 + aos_field_type_sz * i, aos_field_type_sz);
        }

        goto end;
    }
    b8:
    {
        uint64_t count_of_blocks = datalen / 2;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint64x2x4_t from;
            from.val[0] = vreinterpretq_u64_u8(vld1q_u8(rp1 + 16 * i));
            from.val[1] = vreinterpretq_u64_u8(vld1q_u8(rp2 + 16 * i));
            from.val[2] = vreinterpretq_u64_u8(vld1q_u8(rp3 + 16 * i));
            from.val[3] = vreinterpretq_u64_u8(vld1q_u8(rp4 + 16 * i));

            FOR_2_STORE_LANES(vst4q_lane_u64, output + aos_struct_sz * 2 * i, from, aos_struct_sz, uint64_t);
        }

        for (uint64_t i = count_of_blocks * 2; i < datalen; ++i)
        {
            simd_utils::memcpy(output + aos_struct_sz * i,                       rp1 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + aos_field_type_sz,   rp2 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + 2*aos_field_type_sz, rp3 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + 3*aos_field_type_sz, rp4 + aos_field_type_sz * i, aos_field_type_sz);
        }

        goto end;
    }
    end:;
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

    static const void* gotoTable[] = {&&b1, &&b2, &&b4, &&b8};
    goto *gotoTable[std::countr_zero(aos_field_type_sz)];

    b1:
    {
        uint64_t count_of_blocks = datalen / 16;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint8x16x3_t from;
            from.val[0] = vld1q_u8(rp1 + 16 * i);
            from.val[1] = vld1q_u8(rp2 + 16 * i);
            from.val[2] = vld1q_u8(rp3 + 16 * i);

            FOR_16_STORE_LANES(vst3q_lane_u8, output + aos_struct_sz * 16 * i, from, aos_struct_sz, uint8_t);
        }

        for (uint64_t i = count_of_blocks * 16; i < datalen; ++i)
        {
            simd_utils::memcpy(output + aos_struct_sz * i,                       rp1 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + aos_field_type_sz,   rp2 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + 2*aos_field_type_sz, rp3 + aos_field_type_sz * i, aos_field_type_sz);
        }

        goto end;
    }
    b2:
    {
        uint64_t count_of_blocks = datalen / 8;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint16x8x3_t from;
            from.val[0] = vreinterpretq_u16_u8(vld1q_u8(rp1 + 16 * i));
            from.val[1] = vreinterpretq_u16_u8(vld1q_u8(rp2 + 16 * i));
            from.val[2] = vreinterpretq_u16_u8(vld1q_u8(rp3 + 16 * i));

            FOR_8_STORE_LANES(vst3q_lane_u16, output + aos_struct_sz * 8 * i, from, aos_struct_sz, uint16_t);
        }

        for (uint64_t i = count_of_blocks * 8; i < datalen; ++i)
        {
            simd_utils::memcpy(output + aos_struct_sz * i,                       rp1 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + aos_field_type_sz,   rp2 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + 2*aos_field_type_sz, rp3 + aos_field_type_sz * i, aos_field_type_sz);
        }

        goto end;
    }
    b4:
    {
        uint64_t count_of_blocks = datalen / 4;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint32x4x3_t from;
            from.val[0] = vreinterpretq_u32_u8(vld1q_u8(rp1 + 16 * i));
            from.val[1] = vreinterpretq_u32_u8(vld1q_u8(rp2 + 16 * i));
            from.val[2] = vreinterpretq_u32_u8(vld1q_u8(rp3 + 16 * i));

            FOR_4_STORE_LANES(vst3q_lane_u32, output + aos_struct_sz * 4 * i, from, aos_struct_sz, uint32_t);
        }

        for (uint64_t i = count_of_blocks * 4; i < datalen; ++i)
        {
            simd_utils::memcpy(output + aos_struct_sz * i,                       rp1 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + aos_field_type_sz,   rp2 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + 2*aos_field_type_sz, rp3 + aos_field_type_sz * i, aos_field_type_sz);
        }

        goto end;
    }
    b8:
    {
        uint64_t count_of_blocks = datalen / 2;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint64x2x3_t from;
            from.val[0] = vreinterpretq_u64_u8(vld1q_u8(rp1 + 16 * i));
            from.val[1] = vreinterpretq_u64_u8(vld1q_u8(rp2 + 16 * i));
            from.val[2] = vreinterpretq_u64_u8(vld1q_u8(rp3 + 16 * i));

            FOR_2_STORE_LANES(vst3q_lane_u64, output + aos_struct_sz * 2 * i, from, aos_struct_sz, uint64_t);
        }

        for (uint64_t i = count_of_blocks * 2; i < datalen; ++i)
        {
            simd_utils::memcpy(output + aos_struct_sz * i,                       rp1 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + aos_field_type_sz,   rp2 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + 2*aos_field_type_sz, rp3 + aos_field_type_sz * i, aos_field_type_sz);
        }

        goto end;
    }
    end:;
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

    static const void* gotoTable[] = {&&b1, &&b2, &&b4, &&b8};
    goto *gotoTable[std::countr_zero(aos_field_type_sz)];

    b1:
    {
        uint64_t count_of_blocks = datalen / 16;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint8x16x2_t from;
            from.val[0] = vld1q_u8(rp1 + 16 * i);
            from.val[1] = vld1q_u8(rp2 + 16 * i);

            FOR_16_STORE_LANES(vst2q_lane_u8, output + aos_struct_sz * 16 * i, from, aos_struct_sz, uint8_t);
        }

        for (uint64_t i = count_of_blocks * 16; i < datalen; ++i)
        {
            simd_utils::memcpy(output + aos_struct_sz * i,                     rp1 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + aos_field_type_sz, rp2 + aos_field_type_sz * i, aos_field_type_sz);
        }

        goto end;
    }
    b2:
    {
        uint64_t count_of_blocks = datalen / 8;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint16x8x2_t from;
            from.val[0] = vreinterpretq_u16_u8(vld1q_u8(rp1 + 16 * i));
            from.val[1] = vreinterpretq_u16_u8(vld1q_u8(rp2 + 16 * i));

            FOR_8_STORE_LANES(vst2q_lane_u16, output + aos_struct_sz * 8 * i, from, aos_struct_sz, uint16_t);
        }

        for (uint64_t i = count_of_blocks * 8; i < datalen; ++i)
        {
            simd_utils::memcpy(output + aos_struct_sz * i,                     rp1 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + aos_field_type_sz, rp2 + aos_field_type_sz * i, aos_field_type_sz);
        }

        goto end;
    }
    b4:
    {
        uint64_t count_of_blocks = datalen / 4;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint32x4x2_t from;
            from.val[0] = vreinterpretq_u32_u8(vld1q_u8(rp1 + 16 * i));
            from.val[1] = vreinterpretq_u32_u8(vld1q_u8(rp2 + 16 * i));

            FOR_4_STORE_LANES(vst2q_lane_u32, output + aos_struct_sz * 4 * i, from, aos_struct_sz, uint32_t);
        }

        for (uint64_t i = count_of_blocks * 4; i < datalen; ++i)
        {
            simd_utils::memcpy(output + aos_struct_sz * i,                     rp1 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + aos_field_type_sz, rp2 + aos_field_type_sz * i, aos_field_type_sz);
        }

        goto end;
    }
    b8:
    {
        uint64_t count_of_blocks = datalen / 2;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint64x2x2_t from;
            from.val[0] = vreinterpretq_u64_u8(vld1q_u8(rp1 + 16 * i));
            from.val[1] = vreinterpretq_u64_u8(vld1q_u8(rp2 + 16 * i));

            FOR_2_STORE_LANES(vst2q_lane_u64, output + aos_struct_sz * 2 * i, from, aos_struct_sz, uint64_t);
        }

        for (uint64_t i = count_of_blocks * 2; i < datalen; ++i)
        {
            simd_utils::memcpy(output + aos_struct_sz * i,                     rp1 + aos_field_type_sz * i, aos_field_type_sz);
            simd_utils::memcpy(output + aos_struct_sz * i + aos_field_type_sz, rp2 + aos_field_type_sz * i, aos_field_type_sz);
        }

        goto end;
    }
    end:;
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

    static const void* gotoTable[] = {&&b1, &&b2, &&b4, &&b8};
    goto *gotoTable[std::countr_zero(aos_field_type_sz)];

    b1:
    b2:
    b4:
    b8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        simd_utils::memcpy(output + aos_struct_sz * i, rp1 + aos_field_type_sz * i, aos_field_type_sz);
    }
}
