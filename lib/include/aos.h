#pragma once

#include <vector>
#include <memory>
#include <unordered_map>
#include <string>
#include <algorithm>
#include <arrow/api.h>

#include "pretty_type_traits.h"

class AoS
{
private:
    using FieldsT = std::vector<std::shared_ptr<arrow::Field>>;
    using FieldOffsets = std::unordered_map<std::string, uint64_t>;
    using BufferT = std::unique_ptr<uint8_t[]>;

private:
    class Struct
    {
    public:
        Struct(const FieldOffsets& fields, uint8_t* buf, const std::vector<uint64_t>& offsets)
            : m_fieldOffsets(fields)
            , m_structBuf(buf)
            , m_offsets(offsets)
        {
        }

        template <typename DataType,
                  typename CType = typename arrow::TypeTraits<DataType>::CType>
        CType& Value(const std::string& fieldname)
        {
            auto offset = m_offsets[m_fieldOffsets.at(fieldname)];
            return *(reinterpret_cast<CType*>(m_structBuf + offset));
        }

        template <typename DataType,
                  typename CType = typename arrow::TypeTraits<DataType>::CType>
        const CType& Value(const std::string& fieldname) const
        {
            auto offset = m_offsets[m_fieldOffsets.at(fieldname)];
            return *(reinterpret_cast<CType*>(m_structBuf + offset));
        }

    private:
        const FieldOffsets&             m_fieldOffsets;
        uint8_t*                        m_structBuf;
        const std::vector<uint64_t>&    m_offsets;
    };

public:
    AoS() = delete;

    AoS(const std::shared_ptr<arrow::Schema>& schema, uint64_t length)
        : m_length(length)
        , m_fields(schema->fields())
        , m_offsets(1, 0)
    {
        std::sort(m_fields.begin(), m_fields.end(), [](const auto& lhs, const auto& rhs) -> bool {
            return GetCTypeSize(lhs->type()) < GetCTypeSize(rhs->type());
        });

        uint64_t totalSize = 0;
        for (const auto& field: m_fields)
        {
            uint64_t size = GetCTypeSize(field->type());
            m_fieldOffsets[field->name()] = m_offsets.size() - 1;
            m_offsets.push_back(m_offsets.back() + size);
            totalSize += size;
        }

        BufferT(new uint8_t[totalSize * length]).swap(m_buffer);
    }

    ~AoS() = default;

    uint8_t* GetBuffer()
    {
        return m_buffer.get();
    }

    const uint8_t* GetBuffer() const
    {
        return m_buffer.get();
    }

    uint64_t GetLength() const
    {
        return m_length;
    }

    uint64_t GetStructSize() const
    {
        return m_offsets.back();
    }

    const FieldsT& GetFields() const
    {
        return m_fields;
    }

    Struct operator[](uint64_t pos)
    {
        return Struct(m_fieldOffsets, m_buffer.get() + pos * m_offsets.back(), m_offsets);
    }

    const Struct operator[](uint64_t pos) const
    {
        return Struct(m_fieldOffsets, m_buffer.get() + pos * m_offsets.back(), m_offsets);
    }

private:
    uint64_t                m_length;
    FieldsT                 m_fields;   // vector of fields
    std::vector<uint64_t>   m_offsets;  // offsets for fields in physical layout
    FieldOffsets            m_fieldOffsets{}; // field_name : index in m_offsets
    BufferT                 m_buffer{}; // pointer to buffer
};