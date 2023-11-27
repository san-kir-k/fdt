#include "aos/aos.h"

#include "soa2aos.h"

AoS::Struct::Struct(uint64_t pos, const AoS& parent)
    : m_pos(pos)
    , m_parent(parent)
{
}

AoS::AoS(const std::shared_ptr<arrow::Schema>& schema, uint64_t length)
    : m_length(length)
    , m_offsets(1, 0)
    , m_extBuffers(m_length)
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

    uint64_t totalSize = 0;
    for (const auto& field: fields)
    {
        uint64_t size = GetCTypeSize(field->type());
        m_offsets.push_back(m_offsets.back() + size);
        totalSize += size;
    }

    BufferT(new uint8_t[totalSize * m_length]).swap(m_buffer);
}

AoS::AoS(const std::shared_ptr<arrow::Schema>& schema, const std::vector<std::shared_ptr<arrow::Array>>& data)
    : m_length(data.front()->length())
    , m_offsets(1, 0)
    , m_extBuffers(m_length)
{
    using TypeValuePair = std::pair<std::shared_ptr<arrow::Field>, std::shared_ptr<arrow::Array>>;

    auto dataCopy{data};
    auto fields{schema->fields()};
    std::vector<TypeValuePair> toSort;

    for (uint64_t i = 0; i < fields.size(); ++i)
    {
        toSort.emplace_back(fields[i], dataCopy[i]);
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
        fields[i] = toSort[i].first;
        dataCopy[i] = toSort[i].second;
    }

    m_schema = std::make_shared<arrow::Schema>(fields);

    uint64_t totalSize = 0;
    for (const auto& field: fields)
    {
        uint64_t size = GetCTypeSize(field->type());
        m_offsets.push_back(m_offsets.back() + size);
        totalSize += size;
    }

    BufferT(new uint8_t[totalSize * m_length]).swap(m_buffer);
    SoA2AoS(arrow::RecordBatch::Make(m_schema, m_length, dataCopy), *this);
}

std::shared_ptr<AoS> AoS::Make(const std::shared_ptr<arrow::Schema>& schema,
                               const std::vector<std::shared_ptr<arrow::Array>>& data)
{
    return std::make_shared<AoS>(schema, data);
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

AoS::Struct AoS::operator[](uint64_t pos) const
{
    return Struct(pos, *this);
}
