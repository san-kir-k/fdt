#pragma once

#include <cassert>
#include <memory>
#include <arrow/api.h>
#include <arrow/type_traits.h>

#include "conversion/primitive/soa2aos.h"
#include "conversion/string/soa2aos.h"
#include "conversion/list/soa2aos.h"


inline void SoA2AoSx4(
    const std::shared_ptr<arrow::Array>& p1,
    const std::shared_ptr<arrow::Array>& p2,
    const std::shared_ptr<arrow::Array>& p3,
    const std::shared_ptr<arrow::Array>& p4,
    AoS& aos,
    uint64_t start_pos)
{
    if (arrow::is_numeric(p1->type_id()))
    {
        SoA2AoSx4<arrow::NumberType>(p1, p2, p3, p4, aos, start_pos);
    }
    else if (arrow::is_string(p1->type_id()))
    {
        SoA2AoSx4<arrow::StringType>(p1, p2, p3, p4, aos, start_pos);
    }
    else if (arrow::is_list(p1->type_id()))
    {
        SoA2AoSx4<arrow::ListType>(p1, p2, p3, p4, aos, start_pos);
    }
    else
    {
        assert(false && "Not implemented");
    }
}

inline void SoA2AoSx3(
    const std::shared_ptr<arrow::Array>& p1,
    const std::shared_ptr<arrow::Array>& p2,
    const std::shared_ptr<arrow::Array>& p3,
    AoS& aos,
    uint64_t start_pos)
{
    if (arrow::is_numeric(p1->type_id()))
    {
        SoA2AoSx3<arrow::NumberType>(p1, p2, p3, aos, start_pos);
    }
    else if (arrow::is_string(p1->type_id()))
    {
        SoA2AoSx3<arrow::StringType>(p1, p2, p3, aos, start_pos);
    }
    else if (arrow::is_list(p1->type_id()))
    {
        SoA2AoSx3<arrow::ListType>(p1, p2, p3, aos, start_pos);
    }
    else
    {
        assert(false && "Not implemented");
    }
}

inline void SoA2AoSx2(
    const std::shared_ptr<arrow::Array>& p1,
    const std::shared_ptr<arrow::Array>& p2,
    AoS& aos,
    uint64_t start_pos)
{
    if (arrow::is_numeric(p1->type_id()))
    {
        SoA2AoSx2<arrow::NumberType>(p1, p2, aos, start_pos);
    }
    else if (arrow::is_string(p1->type_id()))
    {
        SoA2AoSx2<arrow::StringType>(p1, p2, aos, start_pos);
    }
    else if (arrow::is_list(p1->type_id()))
    {
        SoA2AoSx2<arrow::ListType>(p1, p2, aos, start_pos);
    }
    else
    {
        assert(false && "Not implemented");
    }
}

inline void SoA2AoSx1(
    const std::shared_ptr<arrow::Array>& p1,
    AoS& aos,
    uint64_t start_pos)
{
    if (arrow::is_numeric(p1->type_id()))
    {
        SoA2AoSx1<arrow::NumberType>(p1, aos, start_pos);
    }
    else if (arrow::is_string(p1->type_id()))
    {
        SoA2AoSx1<arrow::StringType>(p1, aos, start_pos);
    }
    else if (arrow::is_list(p1->type_id()))
    {
        SoA2AoSx1<arrow::ListType>(p1, aos, start_pos);
    }
    else
    {
        assert(false && "Not implemented");
    }
}
