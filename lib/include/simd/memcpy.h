#pragma once

#include <arm_neon.h>
#include <cstring>
#include <cstddef>

namespace simd_utils {

#ifdef SIMD_MEMCPY

void memcpy(void* dst, const void* src, size_t size)
{
    const uint8_t* byteSrc = reinterpret_cast<const uint8_t*>(src);
    uint8_t* byteDst = reinterpret_cast<uint8_t*>(dst);

    size_t simdSize = size / 16; // Count of 128-bit blocks

    for (size_t i = 0; i < simdSize; ++i)
    {
        vst1q_u8(byteDst + i * 16, vld1q_u8(byteSrc + i * 16));
    }

    // Buffer remainder
    uint8_t remainder = size % 16;

    if (remainder > 0)
    {
        std::memcpy(byteDst + simdSize * 16, byteSrc + simdSize * 16, remainder);
    }
}

#else

using std::memcpy;

#endif

} // namespace simd_utils
