#pragma once

#include <arrow/array/data.h>
#include <arrow/type_traits.h>
#include <cstring>
#include <memory>

#include <arm_neon.h>
#include <simd/memcpy.h>
#include <simd/load_lanes.h>


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

    static const void* gotoTable[] = {&&b1, &&b2, &&b4, &&b8};
    goto *gotoTable[std::countr_zero(arrow_type_sz)];

    b1:
    {
        uint64_t count_of_blocks = datalen / 16;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint8x16x4_t to;
            auto res = FOR_16_LOAD_LANES(vld4q_lane_u8, input + aos_struct_sz * 16 * i, to, aos_struct_sz, uint8_t);

            vst1q_u8(rp1 + 16 * i, res.val[0]);
            vst1q_u8(rp2 + 16 * i, res.val[1]);
            vst1q_u8(rp3 + 16 * i, res.val[2]);
            vst1q_u8(rp4 + 16 * i, res.val[3]);
        }

        for (uint64_t i = count_of_blocks * 16; i < datalen; ++i)
        {
            simd_utils::memcpy(rp1 + arrow_type_sz * i, input + aos_struct_sz * i,                   arrow_type_sz);
            simd_utils::memcpy(rp2 + arrow_type_sz * i, input + aos_struct_sz * i + arrow_type_sz,   arrow_type_sz);
            simd_utils::memcpy(rp3 + arrow_type_sz * i, input + aos_struct_sz * i + 2*arrow_type_sz, arrow_type_sz);
            simd_utils::memcpy(rp4 + arrow_type_sz * i, input + aos_struct_sz * i + 3*arrow_type_sz, arrow_type_sz);
        }

        goto end;
    }
    b2:
    {
        uint64_t count_of_blocks = datalen / 8;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint16x8x4_t to;
            auto res = FOR_8_LOAD_LANES(vld4q_lane_u16, input + aos_struct_sz * 8 * i, to, aos_struct_sz, uint16_t);

            vst1q_u8(rp1 + 16 * i, vreinterpretq_u8_u16(res.val[0]));
            vst1q_u8(rp2 + 16 * i, vreinterpretq_u8_u16(res.val[1]));
            vst1q_u8(rp3 + 16 * i, vreinterpretq_u8_u16(res.val[2]));
            vst1q_u8(rp4 + 16 * i, vreinterpretq_u8_u16(res.val[3]));
        }

        for (uint64_t i = count_of_blocks * 8; i < datalen; ++i)
        {
            simd_utils::memcpy(rp1 + arrow_type_sz * i, input + aos_struct_sz * i,                   arrow_type_sz);
            simd_utils::memcpy(rp2 + arrow_type_sz * i, input + aos_struct_sz * i + arrow_type_sz,   arrow_type_sz);
            simd_utils::memcpy(rp3 + arrow_type_sz * i, input + aos_struct_sz * i + 2*arrow_type_sz, arrow_type_sz);
            simd_utils::memcpy(rp4 + arrow_type_sz * i, input + aos_struct_sz * i + 3*arrow_type_sz, arrow_type_sz);
        }

        goto end;
    }
    b4:
    {
        uint64_t count_of_blocks = datalen / 4;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint32x4x4_t to;
            auto res = FOR_4_LOAD_LANES(vld4q_lane_u32, input + aos_struct_sz * 4 * i, to, aos_struct_sz, uint32_t);

            vst1q_u8(rp1 + 16 * i, vreinterpretq_u8_u32(res.val[0]));
            vst1q_u8(rp2 + 16 * i, vreinterpretq_u8_u32(res.val[1]));
            vst1q_u8(rp3 + 16 * i, vreinterpretq_u8_u32(res.val[2]));
            vst1q_u8(rp4 + 16 * i, vreinterpretq_u8_u32(res.val[3]));
        }

        for (uint64_t i = count_of_blocks * 4; i < datalen; ++i)
        {
            simd_utils::memcpy(rp1 + arrow_type_sz * i, input + aos_struct_sz * i,                   arrow_type_sz);
            simd_utils::memcpy(rp2 + arrow_type_sz * i, input + aos_struct_sz * i + arrow_type_sz,   arrow_type_sz);
            simd_utils::memcpy(rp3 + arrow_type_sz * i, input + aos_struct_sz * i + 2*arrow_type_sz, arrow_type_sz);
            simd_utils::memcpy(rp4 + arrow_type_sz * i, input + aos_struct_sz * i + 3*arrow_type_sz, arrow_type_sz);
        }

        goto end;
    }
    b8:
    {
        uint64_t count_of_blocks = datalen / 2;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint64x2x4_t to;
            auto res = FOR_2_LOAD_LANES(vld4q_lane_u64, input + aos_struct_sz * 2 * i, to, aos_struct_sz, uint64_t);

            vst1q_u8(rp1 + 16 * i, vreinterpretq_u8_u64(res.val[0]));
            vst1q_u8(rp2 + 16 * i, vreinterpretq_u8_u64(res.val[1]));
            vst1q_u8(rp3 + 16 * i, vreinterpretq_u8_u64(res.val[2]));
            vst1q_u8(rp4 + 16 * i, vreinterpretq_u8_u64(res.val[3]));
        }

        for (uint64_t i = count_of_blocks * 2; i < datalen; ++i)
        {
            simd_utils::memcpy(rp1 + arrow_type_sz * i, input + aos_struct_sz * i,                   arrow_type_sz);
            simd_utils::memcpy(rp2 + arrow_type_sz * i, input + aos_struct_sz * i + arrow_type_sz,   arrow_type_sz);
            simd_utils::memcpy(rp3 + arrow_type_sz * i, input + aos_struct_sz * i + 2*arrow_type_sz, arrow_type_sz);
            simd_utils::memcpy(rp4 + arrow_type_sz * i, input + aos_struct_sz * i + 3*arrow_type_sz, arrow_type_sz);
        }

        goto end;
    }
    end:;
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

    static const void* gotoTable[] = {&&b1, &&b2, &&b4, &&b8};
    goto *gotoTable[std::countr_zero(arrow_type_sz)];

    b1:
    {
        uint64_t count_of_blocks = datalen / 16;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint8x16x3_t to;
            auto res = FOR_16_LOAD_LANES(vld3q_lane_u8, input + aos_struct_sz * 16 * i, to, aos_struct_sz, uint8_t);

            vst1q_u8(rp1 + 16 * i, res.val[0]);
            vst1q_u8(rp2 + 16 * i, res.val[1]);
            vst1q_u8(rp3 + 16 * i, res.val[2]);
        }

        for (uint64_t i = count_of_blocks * 16; i < datalen; ++i)
        {
            simd_utils::memcpy(rp1 + arrow_type_sz * i, input + aos_struct_sz * i,                   arrow_type_sz);
            simd_utils::memcpy(rp2 + arrow_type_sz * i, input + aos_struct_sz * i + arrow_type_sz,   arrow_type_sz);
            simd_utils::memcpy(rp3 + arrow_type_sz * i, input + aos_struct_sz * i + 2*arrow_type_sz, arrow_type_sz);
        }

        goto end;
    }
    b2:
    {
        uint64_t count_of_blocks = datalen / 8;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint16x8x3_t to;
            auto res = FOR_8_LOAD_LANES(vld3q_lane_u16, input + aos_struct_sz * 8 * i, to, aos_struct_sz, uint16_t);

            vst1q_u8(rp1 + 16 * i, vreinterpretq_u8_u16(res.val[0]));
            vst1q_u8(rp2 + 16 * i, vreinterpretq_u8_u16(res.val[1]));
            vst1q_u8(rp3 + 16 * i, vreinterpretq_u8_u16(res.val[2]));
        }

        for (uint64_t i = count_of_blocks * 8; i < datalen; ++i)
        {
            simd_utils::memcpy(rp1 + arrow_type_sz * i, input + aos_struct_sz * i,                   arrow_type_sz);
            simd_utils::memcpy(rp2 + arrow_type_sz * i, input + aos_struct_sz * i + arrow_type_sz,   arrow_type_sz);
            simd_utils::memcpy(rp3 + arrow_type_sz * i, input + aos_struct_sz * i + 2*arrow_type_sz, arrow_type_sz);
        }

        goto end;
    }
    b4:
    {
        uint64_t count_of_blocks = datalen / 4;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint32x4x3_t to;
            auto res = FOR_4_LOAD_LANES(vld3q_lane_u32, input + aos_struct_sz * 4 * i, to, aos_struct_sz, uint32_t);

            vst1q_u8(rp1 + 16 * i, vreinterpretq_u8_u32(res.val[0]));
            vst1q_u8(rp2 + 16 * i, vreinterpretq_u8_u32(res.val[1]));
            vst1q_u8(rp3 + 16 * i, vreinterpretq_u8_u32(res.val[2]));
        }

        for (uint64_t i = count_of_blocks * 4; i < datalen; ++i)
        {
            simd_utils::memcpy(rp1 + arrow_type_sz * i, input + aos_struct_sz * i,                   arrow_type_sz);
            simd_utils::memcpy(rp2 + arrow_type_sz * i, input + aos_struct_sz * i + arrow_type_sz,   arrow_type_sz);
            simd_utils::memcpy(rp3 + arrow_type_sz * i, input + aos_struct_sz * i + 2*arrow_type_sz, arrow_type_sz);
        }

        goto end;
    }
    b8:
    {
        uint64_t count_of_blocks = datalen / 2;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint64x2x3_t to;
            auto res = FOR_2_LOAD_LANES(vld3q_lane_u64, input + aos_struct_sz * 2 * i, to, aos_struct_sz, uint64_t);

            vst1q_u8(rp1 + 16 * i, vreinterpretq_u8_u64(res.val[0]));
            vst1q_u8(rp2 + 16 * i, vreinterpretq_u8_u64(res.val[1]));
            vst1q_u8(rp3 + 16 * i, vreinterpretq_u8_u64(res.val[2]));
        }

        for (uint64_t i = count_of_blocks * 2; i < datalen; ++i)
        {
            simd_utils::memcpy(rp1 + arrow_type_sz * i, input + aos_struct_sz * i,                   arrow_type_sz);
            simd_utils::memcpy(rp2 + arrow_type_sz * i, input + aos_struct_sz * i + arrow_type_sz,   arrow_type_sz);
            simd_utils::memcpy(rp3 + arrow_type_sz * i, input + aos_struct_sz * i + 2*arrow_type_sz, arrow_type_sz);
        }

        goto end;
    }
    end:;
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

    static const void* gotoTable[] = {&&b1, &&b2, &&b4, &&b8};
    goto *gotoTable[std::countr_zero(arrow_type_sz)];

    b1:
    {
        uint64_t count_of_blocks = datalen / 16;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint8x16x2_t to;
            auto res = FOR_16_LOAD_LANES(vld2q_lane_u8, input + aos_struct_sz * 16 * i, to, aos_struct_sz, uint8_t);

            vst1q_u8(rp1 + 16 * i, res.val[0]);
            vst1q_u8(rp2 + 16 * i, res.val[1]);
        }

        for (uint64_t i = count_of_blocks * 16; i < datalen; ++i)
        {
            simd_utils::memcpy(rp1 + arrow_type_sz * i, input + aos_struct_sz * i,                   arrow_type_sz);
            simd_utils::memcpy(rp2 + arrow_type_sz * i, input + aos_struct_sz * i + arrow_type_sz,   arrow_type_sz);
        }

        goto end;
    }
    b2:
    {
        uint64_t count_of_blocks = datalen / 8;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint16x8x2_t to;
            auto res = FOR_8_LOAD_LANES(vld2q_lane_u16, input + aos_struct_sz * 8 * i, to, aos_struct_sz, uint16_t);

            vst1q_u8(rp1 + 16 * i, vreinterpretq_u8_u16(res.val[0]));
            vst1q_u8(rp2 + 16 * i, vreinterpretq_u8_u16(res.val[1]));
        }

        for (uint64_t i = count_of_blocks * 8; i < datalen; ++i)
        {
            simd_utils::memcpy(rp1 + arrow_type_sz * i, input + aos_struct_sz * i,                   arrow_type_sz);
            simd_utils::memcpy(rp2 + arrow_type_sz * i, input + aos_struct_sz * i + arrow_type_sz,   arrow_type_sz);
        }

        goto end;
    }
    b4:
    {
        uint64_t count_of_blocks = datalen / 4;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint32x4x2_t to;
            auto res = FOR_4_LOAD_LANES(vld2q_lane_u32, input + aos_struct_sz * 4 * i, to, aos_struct_sz, uint32_t);

            vst1q_u8(rp1 + 16 * i, vreinterpretq_u8_u32(res.val[0]));
            vst1q_u8(rp2 + 16 * i, vreinterpretq_u8_u32(res.val[1]));
        }

        for (uint64_t i = count_of_blocks * 4; i < datalen; ++i)
        {
            simd_utils::memcpy(rp1 + arrow_type_sz * i, input + aos_struct_sz * i,                   arrow_type_sz);
            simd_utils::memcpy(rp2 + arrow_type_sz * i, input + aos_struct_sz * i + arrow_type_sz,   arrow_type_sz);
        }

        goto end;
    }
    b8:
    {
        uint64_t count_of_blocks = datalen / 2;
        for (uint64_t i = 0; i < count_of_blocks; ++i)
        {
            uint64x2x2_t to;
            auto res = FOR_2_LOAD_LANES(vld2q_lane_u64, input + aos_struct_sz * 2 * i, to, aos_struct_sz, uint64_t);

            vst1q_u8(rp1 + 16 * i, vreinterpretq_u8_u64(res.val[0]));
            vst1q_u8(rp2 + 16 * i, vreinterpretq_u8_u64(res.val[1]));
        }

        for (uint64_t i = count_of_blocks * 2; i < datalen; ++i)
        {
            simd_utils::memcpy(rp1 + arrow_type_sz * i, input + aos_struct_sz * i,                   arrow_type_sz);
            simd_utils::memcpy(rp2 + arrow_type_sz * i, input + aos_struct_sz * i + arrow_type_sz,   arrow_type_sz);
        }

        goto end;
    }
    end:;
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

    static const void* gotoTable[] = {&&b1, &&b2, &&b4, &&b8};
    goto *gotoTable[std::countr_zero(arrow_type_sz)];

    b1:
    b2:
    b4:
    b8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        simd_utils::memcpy(rp1 + arrow_type_sz * i, input + aos_struct_sz * i, arrow_type_sz);
    }
}
