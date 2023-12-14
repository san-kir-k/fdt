#pragma once

#include <arm_neon.h>
#include <cstddef>

namespace simd_utils {

// https://stackoverflow.com/questions/43521147/how-to-load-4-unsigned-chars-and-convert-them-to-signed-shorts-with-neon
void memcpy(void* dst, const void* src, size_t size)
{
    const int32_t* isrc = static_cast<const int32_t*>(src);
    int32_t* idst = static_cast<int32_t*>(dst);

    int32x4_t tmp;
    // copy 128 bits at a time
    for (size_t i = 0; i < size; i += 4)
    {
        tmp = vld1q_s32(isrc + i); // load 4 elements to tmp SIMD register
        vst1q_s32(&idst[i], tmp);  // copy 4 elements from tmp SIMD register
    }
}

}