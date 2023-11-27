#include "aos/types/string.h"

StringBuffer::String::String(uint8_t* data, uint64_t pos, const StringBuffer& parent)
    : m_data(data)
    , m_pos(pos)
    , m_parent(parent)
{
}

std::string_view StringBuffer::String::DataView() const
{
    uint8_t size = *m_data;

    if (m_parent.IsEmbedded(size))
    {
        return std::string_view(reinterpret_cast<char*>(m_data + 1), size);
    }
    else
    {
        return m_parent.m_buffer->GetView(m_pos);
    }
}

StringBuffer::StringBuffer(const std::shared_ptr<arrow::StringArray>& data, uint64_t threshold)
    : m_threshold(threshold)
    , m_buffer(data)
{
}

bool StringBuffer::IsEmbedded(uint64_t size) const
{
    return size <= m_threshold;
}

uint64_t StringBuffer::GetThreshold() const
{
    return m_threshold;
}
