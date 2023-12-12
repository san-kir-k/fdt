#pragma once

#include <arrow/type_traits.h>
#include <cassert>


template <typename T, arrow::enable_if_list_type<T, bool> = true>
void SoA2AoSx4(
    const std::shared_ptr<arrow::Array>&,
    const std::shared_ptr<arrow::Array>&,
    const std::shared_ptr<arrow::Array>&,
    const std::shared_ptr<arrow::Array>&,
    AoS&,
    uint64_t)
{
    assert(false && "Not implemented");
}

template <typename T, arrow::enable_if_list_type<T, bool> = true>
void SoA2AoSx3(
    const std::shared_ptr<arrow::Array>&,
    const std::shared_ptr<arrow::Array>&,
    const std::shared_ptr<arrow::Array>&,
    AoS&,
    uint64_t)
{
    assert(false && "Not implemented");
}

template <typename T, arrow::enable_if_list_type<T, bool> = true>
void SoA2AoSx2(
    const std::shared_ptr<arrow::Array>&,
    const std::shared_ptr<arrow::Array>&,
    AoS&,
    uint64_t)
{
    assert(false && "Not implemented");
}

template <typename T, arrow::enable_if_list_type<T, bool> = true>
void SoA2AoSx1(
    const std::shared_ptr<arrow::Array>&,
    AoS&,
    uint64_t)
{
    assert(false && "Not implemented");
}
