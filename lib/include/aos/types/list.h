#pragma once

#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <arrow/array/array_nested.h>
#include <memory>
#include <cstdint>
#include <span>

#include "aos/buffer/buffer.h"

class ListBuffer: public IBuffer
{
public:
    template <typename ValueType,
              typename CType = typename arrow::TypeTraits<ValueType>::CType>
    class Slice
    {
    public:
        Slice(uint8_t* data, const ListBuffer& parent)
            : m_data(data)
            , m_parent(parent)
        {
        }

        std::span<CType> DataView() const
        {
            uint8_t* externalPtr = *(reinterpret_cast<uint8_t**>(m_data));
            return std::span(reinterpret_cast<CType*>(externalPtr + sizeof(uint16_t)), *(reinterpret_cast<uint16_t*>(externalPtr)));            
        }

    private:
        uint8_t*            m_data;
        const ListBuffer&   m_parent;
    };

public:
    ListBuffer(std::shared_ptr<uint8_t[]> data, uint64_t capacity, uint64_t element_sz);
    ~ListBuffer() = default;

    uint64_t GetElementsCount() const;
    uint64_t& GetElementsCount();

    uint64_t GetLength() const;
    uint64_t& GetLength();

    uint8_t* GetBuffer();
    const uint8_t* GetBuffer() const;

    uint64_t GetSize() const;

    uint64_t GetCapacity() const;

    uint64_t GetElementSize() const;

private:
    std::shared_ptr<uint8_t[]> m_buffer;
    uint64_t                   m_length{};
    uint64_t                   m_capacity{};
    uint64_t                   m_elemCount{};
    uint64_t                   m_elemSize{};
};