#pragma once

#include <cassert>
#include <memory>
#include <arrow/api.h>
#include <arrow/type_traits.h>

#include "conversion/primitive/soa2aos.h"
#include "conversion/string/soa2aos.h"
#include "conversion/list/soa2aos.h"


inline void SoA2AoSx4(
    std::shared_ptr<arrow::Array> p1, 
    std::shared_ptr<arrow::Array> p2,
    std::shared_ptr<arrow::Array> p3,
    std::shared_ptr<arrow::Array> p4,
    uint64_t insz,
    uint8_t* out, uint64_t outsz, uint64_t datalen)
{
    auto* rp1 = p1->data()->GetMutableValues<uint8_t>(1);
    auto* rp2 = p2->data()->GetMutableValues<uint8_t>(1);
    auto* rp3 = p3->data()->GetMutableValues<uint8_t>(1);
    auto* rp4 = p4->data()->GetMutableValues<uint8_t>(1);

    if (arrow::is_numeric(p1->type_id()))
    {
        SoA2AoSx4<arrow::NumberType>(rp1, rp2, rp3, rp4, insz, out, outsz, datalen);
    }
    else if (arrow::is_string(p1->type_id()))
    {
        SoA2AoSx4<arrow::StringType>(rp1, rp2, rp3, rp4, insz, out, outsz, datalen);
    }
    else if (arrow::is_list(p1->type_id()))
    {
        SoA2AoSx4<arrow::ListType>(rp1, rp2, rp3, rp4, insz, out, outsz, datalen);
    }
    else
    {
        assert(false && "Not implemented");
    }
}

inline void SoA2AoSx3(
    std::shared_ptr<arrow::Array> p1, 
    std::shared_ptr<arrow::Array> p2,
    std::shared_ptr<arrow::Array> p3,
    uint64_t insz,
    uint8_t* out, uint64_t outsz, uint64_t datalen)
{
    auto* rp1 = p1->data()->GetMutableValues<uint8_t>(1);
    auto* rp2 = p2->data()->GetMutableValues<uint8_t>(1);
    auto* rp3 = p3->data()->GetMutableValues<uint8_t>(1);

    if (arrow::is_numeric(p1->type_id()))
    {
        SoA2AoSx3<arrow::NumberType>(rp1, rp2, rp3, insz, out, outsz, datalen);
    }
    else if (arrow::is_string(p1->type_id()))
    {
        SoA2AoSx3<arrow::StringType>(rp1, rp2, rp3, insz, out, outsz, datalen);
    }
    else if (arrow::is_list(p1->type_id()))
    {
        SoA2AoSx3<arrow::ListType>(rp1, rp2, rp3, insz, out, outsz, datalen);
    }
    else
    {
        assert(false && "Not implemented");
    }
}

inline void SoA2AoSx2(
    std::shared_ptr<arrow::Array> p1, 
    std::shared_ptr<arrow::Array> p2,
    uint64_t insz,
    uint8_t* out, uint64_t outsz, uint64_t datalen)
{
    auto* rp1 = p1->data()->GetMutableValues<uint8_t>(1);
    auto* rp2 = p2->data()->GetMutableValues<uint8_t>(1);

    if (arrow::is_numeric(p1->type_id()))
    {
        SoA2AoSx2<arrow::NumberType>(rp1, rp2, insz, out, outsz, datalen);
    }
    else if (arrow::is_string(p1->type_id()))
    {
        SoA2AoSx2<arrow::StringType>(rp1, rp2, insz, out, outsz, datalen);
    }
    else if (arrow::is_list(p1->type_id()))
    {
        SoA2AoSx2<arrow::ListType>(rp1, rp2, insz, out, outsz, datalen);
    }
    else
    {
        assert(false && "Not implemented");
    }
}

inline void SoA2AoSx1(
    std::shared_ptr<arrow::Array> p1, 
    uint64_t insz,
    uint8_t* out, uint64_t outsz, uint64_t datalen)
{
    auto* rp1 = p1->data()->GetMutableValues<uint8_t>(1);

    if (arrow::is_numeric(p1->type_id()))
    {
        SoA2AoSx1<arrow::NumberType>(rp1, insz, out, outsz, datalen);
    }
    else if (arrow::is_string(p1->type_id()))
    {
        SoA2AoSx1<arrow::StringType>(rp1, insz, out, outsz, datalen);
    }
    else if (arrow::is_list(p1->type_id()))
    {
        SoA2AoSx1<arrow::ListType>(rp1, insz, out, outsz, datalen);
    }
    else
    {
        assert(false && "Not implemented");
    }
}
