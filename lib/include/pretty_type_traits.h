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
        case Type::BOOL:
            return CTypeSize<TypeIdTraits<Type::BOOL>::Type>;
        case Type::INT8:
            return CTypeSize<TypeIdTraits<Type::INT8>::Type>;
        case Type::INT16:
            return CTypeSize<TypeIdTraits<Type::INT16>::Type>;
        case Type::INT32:
            return CTypeSize<TypeIdTraits<Type::INT32>::Type>;
        case Type::INT64:
            return CTypeSize<TypeIdTraits<Type::INT64>::Type>;
        case Type::UINT8:
            return CTypeSize<TypeIdTraits<Type::UINT8>::Type>;
        case Type::UINT16:
            return CTypeSize<TypeIdTraits<Type::UINT16>::Type>;
        case Type::UINT32:
            return CTypeSize<TypeIdTraits<Type::UINT32>::Type>;
        case Type::UINT64:
            return CTypeSize<TypeIdTraits<Type::UINT64>::Type>;
        case Type::FLOAT:
            return CTypeSize<TypeIdTraits<Type::FLOAT>::Type>;
        case Type::DOUBLE:
            return CTypeSize<TypeIdTraits<Type::DOUBLE>::Type>;
        default:
            assert(false);
            break;
    }
}
