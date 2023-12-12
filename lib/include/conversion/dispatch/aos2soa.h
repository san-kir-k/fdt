#pragma once

#include <cassert>
#include <memory>
#include <arrow/api.h>
#include <arrow/type_traits.h>

#include "conversion/primitive/aos2soa.h"
#include "conversion/string/aos2soa.h"
#include "conversion/list/aos2soa.h"


inline void AoS2SoAx4(
    const uint8_t* input, uint64_t insz,
    std::shared_ptr<arrow::ArrayData>& p1, 
    std::shared_ptr<arrow::ArrayData>& p2,
    std::shared_ptr<arrow::ArrayData>& p3,
    std::shared_ptr<arrow::ArrayData>& p4,
    uint64_t outsz, uint64_t datalen,
    arrow::Type::type id)
{
    if (arrow::is_numeric(id))
    {
        AoS2SoAx4<arrow::NumberType>(input, insz, p1, p2, p3, p4, outsz, datalen);
    }
    else if (arrow::is_string(id))
    {
        AoS2SoAx4<arrow::StringType>(input, insz, p1, p2, p3, p4, outsz, datalen);
    }
    else if (arrow::is_list(id))
    {
        AoS2SoAx4<arrow::ListType>(input, insz, p1, p2, p3, p4, outsz, datalen);
    }
    else
    {
        assert(false && "Not implemented");
    }
}

inline void AoS2SoAx3(
    const uint8_t* input, uint64_t insz,
    std::shared_ptr<arrow::ArrayData>& p1,
    std::shared_ptr<arrow::ArrayData>& p2,
    std::shared_ptr<arrow::ArrayData>& p3,
    uint64_t outsz, uint64_t datalen,
    arrow::Type::type id)
{
    if (arrow::is_numeric(id))
    {
        AoS2SoAx3<arrow::NumberType>(input, insz, p1, p2, p3, outsz, datalen);
    }
    else if (arrow::is_string(id))
    {
        AoS2SoAx3<arrow::StringType>(input, insz, p1, p2, p3, outsz, datalen);
    }
    else if (arrow::is_list(id))
    {
        AoS2SoAx3<arrow::ListType>(input, insz, p1, p2, p3, outsz, datalen);
    }
    else
    {
        assert(false && "Not implemented");
    }
}

inline void AoS2SoAx2(
    const uint8_t* input, uint64_t insz,
    std::shared_ptr<arrow::ArrayData>& p1, 
    std::shared_ptr<arrow::ArrayData>& p2,
    uint64_t outsz, uint64_t datalen,
    arrow::Type::type id)
{
    if (arrow::is_numeric(id))
    {
        AoS2SoAx2<arrow::NumberType>(input, insz, p1, p2, outsz, datalen);
    }
    else if (arrow::is_string(id))
    {
        AoS2SoAx2<arrow::StringType>(input, insz, p1, p2, outsz, datalen);
    }
    else if (arrow::is_list(id))
    {
        AoS2SoAx2<arrow::ListType>(input, insz, p1, p2, outsz, datalen);
    }
    else
    {
        assert(false && "Not implemented");
    }
}

inline void AoS2SoAx1(
    const uint8_t* input, uint64_t insz,
    std::shared_ptr<arrow::ArrayData>& p1, 
    uint64_t outsz, uint64_t datalen,
    arrow::Type::type id)
{
    if (arrow::is_numeric(id))
    {
        AoS2SoAx1<arrow::NumberType>(input, insz, p1, outsz, datalen);
    }
    else if (arrow::is_string(id))
    {
        AoS2SoAx1<arrow::StringType>(input, insz, p1, outsz, datalen);
    }
    else if (arrow::is_list(id))
    {
        AoS2SoAx1<arrow::ListType>(input, insz, p1, outsz, datalen);
    }
    else
    {
        assert(false && "Not implemented");
    }
}
