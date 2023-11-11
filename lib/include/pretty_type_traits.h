#pragma once

#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <arrow/type.h>
#include <cassert>

template <typename DataType,
          typename CType = typename arrow::TypeTraits<DataType>::CType>
struct CTypeSizeTrait
{
    static constexpr uint64_t size = sizeof(CType);
};

template <typename DataType>
inline constexpr uint64_t CTypeSize = CTypeSizeTrait<DataType>::size;

// Only primitive supported yet
uint64_t GetCTypeSize(std::shared_ptr<arrow::DataType> data)
{
    using namespace arrow;
    auto type_id = data->id();

    switch (type_id)
    {
        case arrow::Type::INT8:
            return CTypeSize<arrow::TypeIdTraits<arrow::Type::INT8>::Type>;
        case arrow::Type::INT16:
            return CTypeSize<arrow::TypeIdTraits<arrow::Type::INT16>::Type>;
        case arrow::Type::INT32:
            return CTypeSize<arrow::TypeIdTraits<arrow::Type::INT32>::Type>;
        case arrow::Type::INT64:
            return CTypeSize<arrow::TypeIdTraits<arrow::Type::INT64>::Type>;
        case arrow::Type::UINT8:
            return CTypeSize<arrow::TypeIdTraits<arrow::Type::UINT8>::Type>;
        case arrow::Type::UINT16:
            return CTypeSize<arrow::TypeIdTraits<arrow::Type::UINT16>::Type>;
        case arrow::Type::UINT32:
            return CTypeSize<arrow::TypeIdTraits<arrow::Type::UINT32>::Type>;
        case arrow::Type::UINT64:
            return CTypeSize<arrow::TypeIdTraits<arrow::Type::UINT64>::Type>;
        case arrow::Type::FLOAT:
            return CTypeSize<arrow::TypeIdTraits<arrow::Type::FLOAT>::Type>;
        case arrow::Type::DOUBLE:
            return CTypeSize<arrow::TypeIdTraits<arrow::Type::DOUBLE>::Type>;
        default:
            assert(false);
            break;
    }
}
