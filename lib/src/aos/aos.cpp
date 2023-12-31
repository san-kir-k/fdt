#include "aos/aos.h"

#include <utility>
#include <cassert>
#include <memory>

#include "conversion/soa2aos.h"
#include "aos/types/comparator.h"
#include "aos/types/string.h"
#include "aos/types/list.h"

AoS::Struct::Struct(uint64_t pos, const AoS& parent)
    : m_pos(pos)
    , m_parent(parent)
{
}

AoS::AoS(const std::shared_ptr<arrow::Schema>& schema)
    : m_length(0)
    , m_offsets(1, 0)
    , m_extBuffers(schema->num_fields())
{
    auto fields{schema->fields()};

    std::sort(fields.begin(), fields.end(), TypesComparator{});

    m_schema = std::make_shared<arrow::Schema>(fields);
}

std::shared_ptr<AoS> AoS::Make(const std::shared_ptr<arrow::Schema>& schema,
                               const std::vector<std::shared_ptr<arrow::Array>>& data)
{
    using TypeValuePair = std::pair<std::shared_ptr<arrow::Field>, std::shared_ptr<arrow::Array>>;

    auto dataCopy{data};
    auto fieldsCopy{schema->fields()};
    std::vector<TypeValuePair> toSort;

    for (uint64_t i = 0; i < fieldsCopy.size(); ++i)
    {
        toSort.emplace_back(fieldsCopy[i], dataCopy[i]);
    }

    std::sort(toSort.begin(), toSort.end(), TypesWithDataComparator{});

    for (uint64_t i = 0; i < toSort.size(); ++i)
    {
        fieldsCopy[i] = toSort[i].first;
        dataCopy[i] = toSort[i].second;
    }

    auto schemaCopy = std::make_shared<arrow::Schema>(fieldsCopy);

    return SoA2AoS(arrow::RecordBatch::Make(schemaCopy, dataCopy.front()->length(), dataCopy));
}

void AoS::PrepareSelf(std::shared_ptr<arrow::RecordBatch> record_batch)
{
    m_length = record_batch->num_rows();

    uint64_t totalSize = 0;
    for (uint64_t i = 0; i < m_schema->fields().size(); ++i)
    {
        const auto& field = m_schema->fields()[i];
        if (arrow::is_numeric(field->type()->id()))
        {
            uint64_t size = GetCTypeSize(field->type());
            m_offsets.push_back(m_offsets.back() + size);
            totalSize += size;
        }
        else if (arrow::is_string(field->type()->id()))
        {
            auto fieldName = field->name();
            auto strArray = std::dynamic_pointer_cast<arrow::StringArray>(record_batch->GetColumnByName(fieldName));
            // calc avg
            uint64_t avgStrSize = strArray->total_values_length() / strArray->length();
            // calc external buf size for string
            uint64_t externalBufSize = 2 * (2 + avgStrSize) * strArray->length(); // 2 -- count of bytes for size

            std::shared_ptr<uint8_t[]> extBuffer(new uint8_t[externalBufSize]);
            m_extBuffers[i] = std::shared_ptr<IBuffer>(new StringBuffer(extBuffer, externalBufSize, avgStrSize));

            uint64_t inStructSize = std::dynamic_pointer_cast<StringBuffer>(m_extBuffers[i])->GetSize();
            totalSize += inStructSize;
            m_offsets.push_back(m_offsets.back() + inStructSize);
        }
        else if (arrow::is_list(field->type()->id()))
        {
            auto fieldName = field->name();
            auto listArray = std::dynamic_pointer_cast<arrow::ListArray>(record_batch->GetColumnByName(fieldName));
            // calc external buf size for list, only primitive types are supported yet
            uint64_t externalBufSize = 2 * listArray->offsets()->length() + GetCTypeSize(listArray->value_type()) * listArray->values()->length(); // 2 -- count of bytes for size

            std::shared_ptr<uint8_t[]> extBuffer(new uint8_t[externalBufSize]);
            m_extBuffers[i] = std::shared_ptr<IBuffer>(new ListBuffer(extBuffer, externalBufSize, GetCTypeSize(listArray->value_type())));

            uint64_t inStructSize = std::dynamic_pointer_cast<ListBuffer>(m_extBuffers[i])->GetSize();
            totalSize += inStructSize;
            m_offsets.push_back(m_offsets.back() + inStructSize);
        }
        else
        {
            assert(false && "Unsupported type");
        }
    }

    BufferT(new uint8_t[totalSize * m_length]).swap(m_buffer);
}

std::vector<std::shared_ptr<arrow::ArrayData>> AoS::PrepareArrayData() const
{
    std::vector<std::shared_ptr<arrow::ArrayData>> arrays_data;

    auto nnull_buffer = PrepareNotNullBitmap();
    for (uint64_t i = 0; i < m_schema->fields().size(); ++i)
    {
        const auto& field = m_schema->fields()[i];
        if (arrow::is_numeric(field->type()->id()))
        {
            arrow::Result<std::shared_ptr<arrow::Buffer>> maybe_buffer = arrow::AllocateBuffer(m_length * GetFieldSize(i));
            if (!maybe_buffer.ok())
            {
                // TODO: do not ignore errors
            }
            arrays_data.push_back(arrow::ArrayData::Make(field->type(), m_length, {nnull_buffer, *maybe_buffer}));
        }
        else if (arrow::is_string(field->type()->id()))
        {
            auto external_len = dynamic_cast<StringBuffer*>(m_extBuffers[i].get())->GetLength();
            arrow::Result<std::shared_ptr<arrow::Buffer>> maybe_buffer = arrow::AllocateBuffer(m_length * GetFieldSize(i) + external_len);
            if (!maybe_buffer.ok())
            {
                // TODO: do not ignore errors
            }
            arrow::Result<std::shared_ptr<arrow::Buffer>> maybe_offsets_buffer = arrow::AllocateBuffer((m_length + 1) * sizeof(uint32_t));
            if (!maybe_offsets_buffer.ok())
            {
                // TODO: do not ignore errors
            }
            auto offsets_buffer = *maybe_offsets_buffer;
            offsets_buffer->mutable_data_as<uint32_t>()[0] = 0;
            arrays_data.push_back(arrow::ArrayData::Make(field->type(), m_length, {nnull_buffer, offsets_buffer, *maybe_buffer}));
        }
        else if (arrow::is_list(field->type()->id()))
        {
            auto* list_ptr = dynamic_cast<ListBuffer*>(m_extBuffers[i].get());
            auto external_len = list_ptr->GetLength();
            arrow::Result<std::shared_ptr<arrow::Buffer>> maybe_buffer = arrow::AllocateBuffer(external_len);
            if (!maybe_buffer.ok())
            {
                // TODO: do not ignore errors
            }
            auto child_data = arrow::ArrayData::Make(
                dynamic_pointer_cast<arrow::ListType>(field->type())->value_type(), list_ptr->GetElementsCount(), {nnull_buffer, *maybe_buffer});

            arrow::Result<std::shared_ptr<arrow::Buffer>> maybe_offsets_buffer = arrow::AllocateBuffer((m_length + 1) * sizeof(uint32_t));
            if (!maybe_offsets_buffer.ok())
            {
                // TODO: do not ignore errors
            }
            auto offsets_buffer = *maybe_offsets_buffer;
            offsets_buffer->mutable_data_as<uint32_t>()[0] = 0;
            arrays_data.push_back(arrow::ArrayData::Make(field->type(), m_length, {nnull_buffer, offsets_buffer}, {child_data}));
        }
        else
        {
            assert(false && "Unsupported type");
        }
    }

    return arrays_data;
}

std::shared_ptr<arrow::Buffer> AoS::PrepareNotNullBitmap() const
{
    // Assume that all values in record_batch are valid without nulls
    arrow::Result<std::shared_ptr<arrow::Buffer>> nnull_maybe_buffer = arrow::AllocateEmptyBitmap(m_length);
    if (!nnull_maybe_buffer.ok())
    {
        // TODO: do not ignore errors
    }
    auto nnull_buffer = *nnull_maybe_buffer;

    uint8_t* raw_nnull_buffer = nnull_buffer->mutable_data();
    std::memset(raw_nnull_buffer, 0xff, arrow::bit_util::BytesForBits(m_length));

    return nnull_buffer;
}

uint8_t* AoS::GetBuffer()
{
    return m_buffer.get();
}

const uint8_t* AoS::GetBuffer() const
{
    return m_buffer.get();
}

std::shared_ptr<IBuffer> AoS::GetExtBuffer(uint64_t pos)
{
    return m_extBuffers[pos];
}

const std::shared_ptr<IBuffer>& AoS::GetExtBuffer(uint64_t pos) const
{
    return m_extBuffers[pos];
}

uint64_t AoS::GetLength() const
{
    return m_length;
}

uint64_t AoS::GetOffset(uint64_t pos) const
{
    return m_offsets[pos];
}

uint64_t AoS::GetStructSize() const
{
    return m_offsets.back();
}

const AoS::FieldsT& AoS::GetFields() const
{
    return m_schema->fields();
}

uint64_t AoS::GetFieldSize(uint64_t pos) const
{
    const auto& field = m_schema->fields()[pos];
        if (arrow::is_numeric(field->type()->id()))
        {
            return GetCTypeSize(field->type());
        }
        else if (arrow::is_string(field->type()->id()))
        {
            auto* bufferPtr = dynamic_cast<StringBuffer*>(m_extBuffers[pos].get());
            return bufferPtr->GetSize();
        }
        else if (arrow::is_list(field->type()->id()))
        {
            auto* bufferPtr = dynamic_cast<ListBuffer*>(m_extBuffers[pos].get());
            return bufferPtr->GetSize();
        }
        else
        {
            assert(false && "Unsupported type");
        }
}

const AoS::SchemaT& AoS::GetSchema() const
{
    return m_schema;
}

AoS::Struct AoS::operator[](uint64_t pos) const
{
    return Struct(pos, *this);
}
