#pragma once

#include <arrow/type_traits.h>
#include <cassert>


template <typename T, arrow::enable_if_string<T, bool> = true>
void AoS2SoAx4(const uint8_t*, uint64_t, uint8_t*, uint8_t*, uint8_t*, uint8_t*, uint64_t, uint64_t)
{
    assert(false && "Not implemented");
}

template <typename T, arrow::enable_if_string<T, bool> = true>
void AoS2SoAx3(const uint8_t*, uint64_t, uint8_t*, uint8_t*, uint8_t*, uint64_t, uint64_t)
{
    assert(false && "Not implemented");
}

template <typename T, arrow::enable_if_string<T, bool> = true>
void AoS2SoAx2(const uint8_t*, uint64_t, uint8_t*, uint8_t*, uint64_t, uint64_t)
{
    assert(false && "Not implemented");
}

template <typename T, arrow::enable_if_string<T, bool> = true>
void AoS2SoAx1(const uint8_t*, uint64_t, uint8_t*, uint64_t, uint64_t)
{
    assert(false && "Not implemented");
}
