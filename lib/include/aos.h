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
    using SchemaP = std::shared_ptr<arrow::Schema>;
    using FieldsT = std::unordered_map<std::string, uint64_t>;
    using BufferT = std::unique_ptr<uint8_t[]>;

private:
    class Struct
    {
    public:
        Struct(const FieldsT& fields, const uint8_t* buf, const SchemaP& schema, const std::vector<uint64_t>& offsets)
            : m_fields(fields)
            , m_structBuf(buf)
            , m_schema(schema)
            , m_offsets(offsets)
        {
        }

        template <typename DataType,
                  typename CType = typename arrow::TypeTraits<DataType>::CType>
        CType operator()(const std::string& fieldname) const
        {
            auto offset = m_offsets[m_fields[fieldname]];
            return *(reinterpret_cast<CType*>(m_structBuf + offset));
        }

    private:
        const FieldsT&                  m_fields;
        const uint8_t*                  m_structBuf;
        const SchemaP&                  m_schema;
        const std::vector<uint64_t>&    m_offsets;
    };

public:
    AoS() = delete;
    AoS(const std::shared_ptr<arrow::Schema>& schema, uint64_t length)
        : m_length(length)
        , m_schema(schema)
        , m_offsets(1, 0)
    {
        auto fields = m_schema->fields();
        std::sort(fields.begin(), fields.end(), [](const auto& lhs, const auto& rhs) -> bool {
            return GetCTypeSize(lhs->type()) < GetCTypeSize(rhs->type());
        });

        uint64_t totalSize = 0;
        for (const auto& field: fields)
        {
            uint64_t size = GetCTypeSize(field->type());
            m_fields[field->name()] = m_offsets.size() - 1;
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

    uint64_t GetLength() const
    {
        return m_length;
    }

    uint64_t GetStructSize() const
    {
        return m_offsets.back();
    }

    const Struct& operator[](uint64_t pos) const
    {
        return Struct(m_fields, m_buffer.get() + pos * m_offsets.back(), m_schema, m_offsets);
    }

private:
    uint64_t                m_length;
    SchemaP                 m_schema;   // arrow::schema
    std::vector<uint64_t>   m_offsets;  // offsets for fields in physical layout
    FieldsT                 m_fields{}; // field_name : index in m_offsets
    BufferT                 m_buffer{}; // pointer to buffer
};