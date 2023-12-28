#pragma once

#include <cassert>
#include <memory>
#include <arrow/api.h>
#include <arrow/type_traits.h>

#include "conversion/primitive/aos2soa.h"
#include "conversion/string/aos2soa.h"
#include "conversion/list/aos2soa.h"


inline void AoS2SoAx4(
    const AoS& aos,
    std::shared_ptr<arrow::ArrayData>& p1, 
    std::shared_ptr<arrow::ArrayData>& p2,
    std::shared_ptr<arrow::ArrayData>& p3,
    std::shared_ptr<arrow::ArrayData>& p4,
    uint64_t start_pos)
{
    auto type = aos.GetFields()[start_pos]->type();

    if (arrow::is_numeric(type->id()))
    {
        AoS2SoAx4<arrow::NumberType>(aos, p1, p2, p3, p4, start_pos);
    }
    else if (arrow::is_string(type->id()))
    {
        AoS2SoAx4<arrow::StringType>(aos, p1, p2, p3, p4, start_pos);
    }
    else if (arrow::is_list(type->id()))
    {
        AoS2SoAx4<arrow::ListType>(aos, p1, p2, p3, p4, start_pos);
    }
    else
    {
        assert(false && "Not implemented");
    }
}

inline void AoS2SoAx3(
    const AoS& aos,
    std::shared_ptr<arrow::ArrayData>& p1, 
    std::shared_ptr<arrow::ArrayData>& p2,
    std::shared_ptr<arrow::ArrayData>& p3,
    uint64_t start_pos)
{
    auto type = aos.GetFields()[start_pos]->type();

    if (arrow::is_numeric(type->id()))
    {
        AoS2SoAx3<arrow::NumberType>(aos, p1, p2, p3, start_pos);
    }
    else if (arrow::is_string(type->id()))
    {
        AoS2SoAx3<arrow::StringType>(aos, p1, p2, p3, start_pos);
    }
    else if (arrow::is_list(type->id()))
    {
        AoS2SoAx3<arrow::ListType>(aos, p1, p2, p3, start_pos);
    }
    else
    {
        assert(false && "Not implemented");
    }
}

inline void AoS2SoAx2(
    const AoS& aos,
    std::shared_ptr<arrow::ArrayData>& p1, 
    std::shared_ptr<arrow::ArrayData>& p2,
    uint64_t start_pos)
{
    auto type = aos.GetFields()[start_pos]->type();

    if (arrow::is_numeric(type->id()))
    {
        AoS2SoAx2<arrow::NumberType>(aos, p1, p2, start_pos);
    }
    else if (arrow::is_string(type->id()))
    {
        AoS2SoAx2<arrow::StringType>(aos, p1, p2, start_pos);
    }
    else if (arrow::is_list(type->id()))
    {
        AoS2SoAx2<arrow::ListType>(aos, p1, p2, start_pos);
    }
    else
    {
        assert(false && "Not implemented");
    }
}

inline void AoS2SoAx1(
    const AoS& aos,
    std::shared_ptr<arrow::ArrayData>& p1, 
    uint64_t start_pos)
{
    auto type = aos.GetFields()[start_pos]->type();

    if (arrow::is_numeric(type->id()))
    {
        AoS2SoAx1<arrow::NumberType>(aos, p1, start_pos);
    }
    else if (arrow::is_string(type->id()))
    {
        AoS2SoAx1<arrow::StringType>(aos, p1, start_pos);
    }
    else if (arrow::is_list(type->id()))
    {
        AoS2SoAx1<arrow::ListType>(aos, p1, start_pos);
    }
    else
    {
        assert(false && "Not implemented");
    }
}
