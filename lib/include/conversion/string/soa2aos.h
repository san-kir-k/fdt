#pragma once

#include <arrow/type_traits.h>
#include <cassert>


template <typename T, arrow::enable_if_string<T, bool> = true>
void SoA2AoSx4(const uint8_t*, const uint8_t*, const uint8_t*, const uint8_t*,
               uint64_t, uint8_t*, uint64_t, uint64_t)
{
    assert(false && "Not implemented");
}

template <typename T, arrow::enable_if_string<T, bool> = true>
void SoA2AoSx3(const uint8_t*, const uint8_t*, const uint8_t*,
               uint64_t, uint8_t*, uint64_t, uint64_t)
{
    assert(false && "Not implemented");
}

template <typename T, arrow::enable_if_string<T, bool> = true>
void SoA2AoSx2(const uint8_t*, const uint8_t*,
               uint64_t, uint8_t*, uint64_t, uint64_t)
{
    assert(false && "Not implemented");
}

template <typename T, arrow::enable_if_string<T, bool> = true>
void SoA2AoSx1(const uint8_t*,
               uint64_t, uint8_t*, uint64_t, uint64_t)
{
    assert(false && "Not implemented");
}