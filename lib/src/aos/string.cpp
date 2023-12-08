#include "aos/types/string.h"

#include <utility>

StringBuffer::String::String(uint8_t* data, const StringBuffer& parent)
    : m_data(data)
    , m_parent(parent)
{
}

std::string_view StringBuffer::String::DataView() const
{
    uint8_t size = *m_data;

    if (m_parent.IsEmbedded(size))
    {
        uint8_t* internalPtr = m_data + 1;
        return std::string_view(reinterpret_cast<char*>(internalPtr), size);
    }
    else
    {
        uint8_t* externalPtr = m_data + 1;
        return std::string_view(reinterpret_cast<char*>(externalPtr + 2), *reinterpret_cast<uint16_t*>(externalPtr));
    }
}

StringBuffer::StringBuffer(const std::shared_ptr<uint8_t[]>& data, uint64_t capacity, uint64_t threshold)
    : m_threshold(std::min(threshold, MAX_EMBEDDED_SIZE))
    , m_buffer(std::move(data))
    , m_capacity(capacity)
{
}

uint64_t StringBuffer::GetThreshold() const
{
    return m_threshold;
}

uint64_t& StringBuffer::GetSize()
{
    return m_size;
}

uint64_t StringBuffer::GetSize() const
{
    return m_size;
}

uint64_t StringBuffer::GetCapacity() const
{
    return m_capacity;
}

bool StringBuffer::IsEmbedded(uint64_t size) const
{
    return size <= m_threshold;
}
