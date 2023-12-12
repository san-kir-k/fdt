#pragma once

#include <arrow/type_traits.h>
#include <cassert>


template <typename T, arrow::enable_if_list_type<T, bool> = true>
void AoS2SoAx4(
    const uint8_t*, uint64_t,
    std::shared_ptr<arrow::ArrayData>&,
    std::shared_ptr<arrow::ArrayData>&,
    std::shared_ptr<arrow::ArrayData>&,
    std::shared_ptr<arrow::ArrayData>&,
    uint64_t, uint64_t)
{
    assert(false && "Not implemented");
}

template <typename T, arrow::enable_if_list_type<T, bool> = true>
void AoS2SoAx3(
    const uint8_t*, uint64_t,
    std::shared_ptr<arrow::ArrayData>&,
    std::shared_ptr<arrow::ArrayData>&,
    std::shared_ptr<arrow::ArrayData>&,
    uint64_t, uint64_t)
{
    assert(false && "Not implemented");
}

template <typename T, arrow::enable_if_list_type<T, bool> = true>
void AoS2SoAx2(
    const uint8_t*, uint64_t,
    std::shared_ptr<arrow::ArrayData>&,
    std::shared_ptr<arrow::ArrayData>&,
    uint64_t, uint64_t)
{
    assert(false && "Not implemented");
}

template <typename T, arrow::enable_if_list_type<T, bool> = true>
void AoS2SoAx1(
    const uint8_t*, uint64_t,
    std::shared_ptr<arrow::ArrayData>&,
    uint64_t, uint64_t)
{
    assert(false && "Not implemented");
}