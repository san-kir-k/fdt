#pragma once

#include <cassert>
#include <memory>
#include <arrow/api.h>
#include <arrow/type_traits.h>

#include "conversion/primitive/aos2soa.h"
#include "conversion/string/aos2soa.h"
#include "conversion/list/aos2soa.h"

// TODO: remove const after preprocess add
inline void AoS2SoAx4(
    const uint8_t* input, uint64_t insz,
    std::shared_ptr<arrow::Array> p1, 
    std::shared_ptr<arrow::Array> p2,
    std::shared_ptr<arrow::Array> p3,
    std::shared_ptr<arrow::Array> p4,
    uint64_t outsz, uint64_t datalen)
{
    auto* rp1 = p1->data()->GetMutableValues<uint8_t>(1);
    auto* rp2 = p2->data()->GetMutableValues<uint8_t>(1);
    auto* rp3 = p3->data()->GetMutableValues<uint8_t>(1);
    auto* rp4 = p4->data()->GetMutableValues<uint8_t>(1);

    if (arrow::is_numeric(p1->type_id()))
    {
        AoS2SoAx4<arrow::NumberType>(input, insz, rp1, rp2, rp3, rp4, outsz, datalen);
    }
    else if (arrow::is_string(p1->type_id()))
    {
        AoS2SoAx4<arrow::StringType>(input, insz, rp1, rp2, rp3, rp4, outsz, datalen);
    }
    else if (arrow::is_list(p1->type_id()))
    {
        AoS2SoAx4<arrow::ListType>(input, insz, rp1, rp2, rp3, rp4, outsz, datalen);
    }
    else
    {
        assert(false && "Not implemented");
    }
}

inline void AoS2SoAx3(
    const uint8_t* input, uint64_t insz,
    std::shared_ptr<arrow::Array> p1,
    std::shared_ptr<arrow::Array> p2,
    std::shared_ptr<arrow::Array> p3,
    uint64_t outsz, uint64_t datalen)
{
    auto* rp1 = p1->data()->GetMutableValues<uint8_t>(1);
    auto* rp2 = p2->data()->GetMutableValues<uint8_t>(1);
    auto* rp3 = p3->data()->GetMutableValues<uint8_t>(1);

    if (arrow::is_numeric(p1->type_id()))
    {
        AoS2SoAx3<arrow::NumberType>(input, insz, rp1, rp2, rp3, outsz, datalen);
    }
    else if (arrow::is_string(p1->type_id()))
    {
        AoS2SoAx3<arrow::StringType>(input, insz, rp1, rp2, rp3, outsz, datalen);
    }
    else if (arrow::is_list(p1->type_id()))
    {
        AoS2SoAx3<arrow::ListType>(input, insz, rp1, rp2, rp3, outsz, datalen);
    }
    else
    {
        assert(false && "Not implemented");
    }
}

inline void AoS2SoAx2(
    const uint8_t* input, uint64_t insz,
    std::shared_ptr<arrow::Array> p1, 
    std::shared_ptr<arrow::Array> p2,
    uint64_t outsz, uint64_t datalen)
{
    auto* rp1 = p1->data()->GetMutableValues<uint8_t>(1);
    auto* rp2 = p2->data()->GetMutableValues<uint8_t>(1);

    if (arrow::is_numeric(p1->type_id()))
    {
        AoS2SoAx2<arrow::NumberType>(input, insz, rp1, rp2, outsz, datalen);
    }
    else if (arrow::is_string(p1->type_id()))
    {
        AoS2SoAx2<arrow::StringType>(input, insz, rp1, rp2, outsz, datalen);
    }
    else if (arrow::is_list(p1->type_id()))
    {
        AoS2SoAx2<arrow::ListType>(input, insz, rp1, rp2, outsz, datalen);
    }
    else
    {
        assert(false && "Not implemented");
    }
}

inline void AoS2SoAx1(
    const uint8_t* input, uint64_t insz,
    std::shared_ptr<arrow::Array> p1, 
    uint64_t outsz, uint64_t datalen)
{
    auto* rp1 = p1->data()->GetMutableValues<uint8_t>(1);

    if (arrow::is_numeric(p1->type_id()))
    {
        AoS2SoAx1<arrow::NumberType>(input, insz, rp1, outsz, datalen);
    }
    else if (arrow::is_string(p1->type_id()))
    {
        AoS2SoAx1<arrow::StringType>(input, insz, rp1, outsz, datalen);
    }
    else if (arrow::is_list(p1->type_id()))
    {
        AoS2SoAx1<arrow::ListType>(input, insz, rp1, outsz, datalen);
    }
    else
    {
        assert(false && "Not implemented");
    }
}
