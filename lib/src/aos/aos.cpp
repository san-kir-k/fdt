#include "aos/aos.h"

#include <utility>
#include <cassert>
#include <memory>

#include "conversion/soa2aos.h"

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

    std::sort(fields.begin(), fields.end(), [](const auto& lhs, const auto& rhs) -> bool {
        bool isLhsPrimitive = IsPrimitive(lhs->type());
        bool isRhsPrimitive = IsPrimitive(rhs->type());

        if (isLhsPrimitive != isRhsPrimitive)
        {
            return isLhsPrimitive;
        }
        else
        {
            if (isLhsPrimitive)
            {
                return GetFixedSizeTypeWidth(lhs->type()) < GetFixedSizeTypeWidth(rhs->type());
            }
            else
            {
                return lhs->type()->id() < rhs->type()->id();
            }
        }
    });

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

    std::sort(toSort.begin(), toSort.end(), [](const auto& lhs, const auto& rhs) -> bool {
        bool isLhsPrimitive = IsPrimitive(lhs.first->type());
        bool isRhsPrimitive = IsPrimitive(rhs.first->type());

        if (isLhsPrimitive != isRhsPrimitive)
        {
            return isLhsPrimitive;
        }
        else
        {
            if (isLhsPrimitive)
            {
                return GetFixedSizeTypeWidth(lhs.first->type()) < GetFixedSizeTypeWidth(rhs.first->type());
            }
            else
            {
                return lhs.first->type()->id() < rhs.first->type()->id();
            }
        }
    });

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
        if (arrow::is_string(field->type()->id()))
        {
            auto fieldName = field->name();
            auto strArray = std::static_pointer_cast<arrow::StringArray>(record_batch->GetColumnByName(fieldName));
            // calc avg
            uint64_t avgStrSize = strArray->total_values_length() / strArray->length();
            // calc external buf size for string
            uint64_t externalBufSize = (2 + avgStrSize) * strArray->length(); // 2 -- count of bytes for size


            m_offsets.push_back(m_offsets.back() + avgStrSize + 1); // 1 -- len of str [0, 254], 255 -> external
            totalSize += avgStrSize + 1;

            std::shared_ptr<uint8_t[]> extBuffer(new uint8_t[externalBufSize]);
            m_extBuffers[i] = std::shared_ptr<IBuffer>(new StringBuffer(extBuffer, externalBufSize, avgStrSize));
        }
        else
        {
            uint64_t size = GetCTypeSize(field->type());
            m_offsets.push_back(m_offsets.back() + size);
            totalSize += size;
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
            arrow::Result<std::shared_ptr<arrow::Buffer>> maybe_buffer = arrow::AllocateBuffer(m_length * GetCTypeSize(field->type()));
            if (!maybe_buffer.ok())
            {
                // TODO: do not ignore errors
            }
            arrays_data.push_back(arrow::ArrayData::Make(field->type(), m_length, {nnull_buffer, *maybe_buffer}));
        }
        else if (arrow::is_string(field->type()->id()))
        {
            // dynamic_cast<StringBuffer*>(m_extBuffers[i].get())->GetCapacity()
            assert(false && "Not implemented yet");
        }
        else if (arrow::is_list(field->type()->id()))
        {
            assert(false && "Not implemented yet");
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

uint64_t AoS::GetLength() const
{
    return m_length;
}

uint64_t AoS::GetStructSize() const
{
    return m_offsets.back();
}

const AoS::FieldsT& AoS::GetFields() const
{
    return m_schema->fields();
}

const AoS::SchemaT& AoS::GetSchema() const
{
    return m_schema;
}

AoS::Struct AoS::operator[](uint64_t pos) const
{
    return Struct(pos, *this);
}
