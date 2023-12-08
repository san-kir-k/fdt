#pragma once

#include <vector>
#include <memory>
#include <unordered_map>
#include <string>
#include <utility>
#include <algorithm>
#include <arrow/api.h>
#include <arrow/type.h>
#include <arrow/record_batch.h>
#include <iostream>

#include "pretty_type_traits.h"
#include "aos/types/string.h"
#include "buffer/buffer.h"


class AoS
{
private:
    using SchemaT = std::shared_ptr<arrow::Schema>;
    using FieldsT = std::vector<std::shared_ptr<arrow::Field>>;
    using BufferT = std::shared_ptr<uint8_t[]>;
    using ExtBuffersT = std::vector<std::shared_ptr<IBuffer>>;

private:
    class Struct
    {
    public:
        Struct(uint64_t pos, const AoS& parent);

        template <typename DataType,
                  typename CType = typename arrow::TypeTraits<DataType>::CType>
        const CType& Value(const std::string& fieldname) const
        {
            auto structBuf = m_parent.m_buffer.get() + m_pos * m_parent.m_offsets.back();
            auto offset = m_parent.m_offsets[m_parent.m_schema->GetFieldIndex(fieldname)];
            return *(reinterpret_cast<CType*>(structBuf + offset));
        }

        StringBuffer::String Value(const std::string& fieldname) const
        {
            auto structBuf = m_parent.m_buffer.get() + m_pos * m_parent.m_offsets.back();
            auto offset = m_parent.m_offsets[m_parent.m_schema->GetFieldIndex(fieldname)];
            auto* bufferPtr = dynamic_cast<StringBuffer*>(m_parent.m_extBuffers[m_pos].get());
            return StringBuffer::String(structBuf + offset, *bufferPtr);
        }

    private:
        uint64_t    m_pos;
        const AoS&  m_parent;
    };

public:
    AoS(const std::shared_ptr<arrow::Schema>& schema);

    static std::shared_ptr<AoS> Make(const std::shared_ptr<arrow::Schema>& schema,
                                     const std::vector<std::shared_ptr<arrow::Array>>& data);

    ~AoS() = default;

    void PrepareSelf(std::shared_ptr<arrow::RecordBatch> record_batch);
    std::vector<std::shared_ptr<arrow::Buffer>> PrepareSoA() const;

    uint8_t* GetBuffer();
    const uint8_t* GetBuffer() const;

    uint64_t GetLength() const;
    uint64_t GetStructSize() const;

    const FieldsT& GetFields() const;

    const SchemaT& GetSchema() const;

    Struct operator[](uint64_t pos) const;

private:
    uint64_t                m_length;
    std::vector<uint64_t>   m_offsets;     // offsets for fields in physical layout
    SchemaT                 m_schema;      // schema
    BufferT                 m_buffer;      // pointer to buffer
    ExtBuffersT             m_extBuffers;  // external buffers for variable-size types
};