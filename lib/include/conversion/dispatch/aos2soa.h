#pragma once

#include <cassert>
#include <memory>
#include <arrow/api.h>
#include <arrow/type_traits.h>
#include <iostream>

#include "conversion/primitive/aos2soa.h"
#include "conversion/string/aos2soa.h"
#include "conversion/list/aos2soa.h"

// TODO: remove const after preprocess add
inline void AoS2SoAx4(
    const uint8_t* input, uint64_t insz,
    std::shared_ptr<arrow::Buffer> p1, 
    std::shared_ptr<arrow::Buffer> p2,
    std::shared_ptr<arrow::Buffer> p3,
    std::shared_ptr<arrow::Buffer> p4,
    uint64_t outsz, uint64_t datalen,
    arrow::Type::type id)
{
    auto* rp1 = p1->mutable_data();
    auto* rp2 = p2->mutable_data();
    auto* rp3 = p3->mutable_data();
    auto* rp4 = p4->mutable_data();

    if (arrow::is_numeric(id))
    {
        AoS2SoAx4<arrow::NumberType>(input, insz, rp1, rp2, rp3, rp4, outsz, datalen);
    }
    else if (arrow::is_string(id))
    {
        AoS2SoAx4<arrow::StringType>(input, insz, rp1, rp2, rp3, rp4, outsz, datalen);
    }
    else if (arrow::is_list(id))
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
    std::shared_ptr<arrow::Buffer> p1,
    std::shared_ptr<arrow::Buffer> p2,
    std::shared_ptr<arrow::Buffer> p3,
    uint64_t outsz, uint64_t datalen,
    arrow::Type::type id)
{
    auto* rp1 = p1->mutable_data();
    auto* rp2 = p2->mutable_data();
    auto* rp3 = p3->mutable_data();

    if (arrow::is_numeric(id))
    {
        AoS2SoAx3<arrow::NumberType>(input, insz, rp1, rp2, rp3, outsz, datalen);
    }
    else if (arrow::is_string(id))
    {
        AoS2SoAx3<arrow::StringType>(input, insz, rp1, rp2, rp3, outsz, datalen);
    }
    else if (arrow::is_list(id))
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
    std::shared_ptr<arrow::Buffer> p1, 
    std::shared_ptr<arrow::Buffer> p2,
    uint64_t outsz, uint64_t datalen,
    arrow::Type::type id)
{
    auto* rp1 = p1->mutable_data();
    auto* rp2 = p2->mutable_data();

    if (arrow::is_numeric(id))
    {
        AoS2SoAx2<arrow::NumberType>(input, insz, rp1, rp2, outsz, datalen);
    }
    else if (arrow::is_string(id))
    {
        AoS2SoAx2<arrow::StringType>(input, insz, rp1, rp2, outsz, datalen);
    }
    else if (arrow::is_list(id))
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
    std::shared_ptr<arrow::Buffer> p1, 
    uint64_t outsz, uint64_t datalen,
    arrow::Type::type id)
{
    auto* rp1 = p1->mutable_data();

    if (arrow::is_numeric(id))
    {
        AoS2SoAx1<arrow::NumberType>(input, insz, rp1, outsz, datalen);
    }
    else if (arrow::is_string(id))
    {
        AoS2SoAx1<arrow::StringType>(input, insz, rp1, outsz, datalen);
    }
    else if (arrow::is_list(id))
    {
        AoS2SoAx1<arrow::ListType>(input, insz, rp1, outsz, datalen);
    }
    else
    {
        assert(false && "Not implemented");
    }
}
