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

#include "pretty_type_traits.h"
#include "aos/types/string.h"
#include "aos/types/list.h"
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
                  typename CType = typename arrow::TypeTraits<DataType>::CType,
                  arrow::enable_if_number<DataType, bool> = true>
        const CType& Value(const std::string& fieldname) const
        {
            auto structBuf = m_parent.m_buffer.get() + m_pos * m_parent.m_offsets.back();
            auto offset = m_parent.m_offsets[m_parent.m_schema->GetFieldIndex(fieldname)];
            return *(reinterpret_cast<CType*>(structBuf + offset));
        }

        template <typename DataType,
                  std::enable_if_t<std::is_same_v<DataType, arrow::StringArray>, bool> = true>
        StringBuffer::String Value(const std::string& fieldname) const
        {
            auto structBuf = m_parent.m_buffer.get() + m_pos * m_parent.m_offsets.back();
            auto field_pos = m_parent.m_schema->GetFieldIndex(fieldname);
            auto offset = m_parent.m_offsets[field_pos];
            auto* bufferPtr = dynamic_cast<StringBuffer*>(m_parent.m_extBuffers[field_pos].get());
            return StringBuffer::String(structBuf + offset, *bufferPtr);
        }

        template <typename DataType, typename ValueType,
                  std::enable_if_t<std::is_same_v<DataType, arrow::ListArray>, bool> = true>
        ListBuffer::Slice<ValueType> Value(const std::string& fieldname) const
        {
            auto structBuf = m_parent.m_buffer.get() + m_pos * m_parent.m_offsets.back();
            auto field_pos = m_parent.m_schema->GetFieldIndex(fieldname);
            auto offset = m_parent.m_offsets[field_pos];
            auto* bufferPtr = dynamic_cast<ListBuffer*>(m_parent.m_extBuffers[field_pos].get());
            return ListBuffer::Slice<ValueType>(structBuf + offset, *bufferPtr);
        }

        template <typename DataType,
                  typename CType = typename arrow::TypeTraits<DataType>::CType,
                  arrow::enable_if_number<DataType, bool> = true>
        const CType& Value(size_t idx) const
        {
            auto structBuf = m_parent.m_buffer.get() + m_pos * m_parent.m_offsets.back();
            auto offset = m_parent.m_offsets[idx];
            return *(reinterpret_cast<CType*>(structBuf + offset));
        }

        template <typename DataType,
                  std::enable_if_t<std::is_same_v<DataType, arrow::StringArray>, bool> = true>
        StringBuffer::String Value(size_t idx) const
        {
            auto structBuf = m_parent.m_buffer.get() + m_pos * m_parent.m_offsets.back();
            auto offset = m_parent.m_offsets[idx];
            auto* bufferPtr = dynamic_cast<StringBuffer*>(m_parent.m_extBuffers[idx].get());
            return StringBuffer::String(structBuf + offset, *bufferPtr);
        }

        template <typename DataType, typename ValueType,
                  std::enable_if_t<std::is_same_v<DataType, arrow::ListArray>, bool> = true>
        ListBuffer::Slice<ValueType> Value(size_t idx) const
        {
            auto structBuf = m_parent.m_buffer.get() + m_pos * m_parent.m_offsets.back();
            auto offset = m_parent.m_offsets[idx];
            auto* bufferPtr = dynamic_cast<ListBuffer*>(m_parent.m_extBuffers[idx].get());
            return ListBuffer::Slice<ValueType>(structBuf + offset, *bufferPtr);
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

    std::vector<std::shared_ptr<arrow::ArrayData>> PrepareArrayData() const;

    uint8_t* GetBuffer();
    const uint8_t* GetBuffer() const;

    std::shared_ptr<IBuffer> GetExtBuffer(uint64_t pos);
    const std::shared_ptr<IBuffer>& GetExtBuffer(uint64_t pos) const;

    uint64_t GetLength() const;
    uint64_t GetStructSize() const;

    const FieldsT& GetFields() const;
    uint64_t GetFieldSize(uint64_t pos) const;

    const SchemaT& GetSchema() const;

    uint64_t GetOffset(uint64_t pos) const;

    Struct operator[](uint64_t pos) const;

private:
    std::shared_ptr<arrow::Buffer> PrepareNotNullBitmap() const;

private:
    uint64_t                m_length;
    std::vector<uint64_t>   m_offsets;     // offsets for fields in physical layout
    SchemaT                 m_schema;      // schema
    BufferT                 m_buffer;      // pointer to buffer
    ExtBuffersT             m_extBuffers;  // external buffers for variable-size types
};