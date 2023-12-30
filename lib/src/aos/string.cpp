#include "aos/types/string.h"

#include <utility>

StringBuffer::String::String(uint8_t* data, const StringBuffer& parent)
    : m_data(data)
    , m_parent(parent)
{
}

std::string_view StringBuffer::String::DataView() const
{
    uint8_t length = m_data[0];

    if (m_parent.IsEmbedded(length))
    {
        uint8_t* internalPtr = m_data + 1;
        return std::string_view(reinterpret_cast<char*>(internalPtr), length);
    }
    else
    {
        uint8_t* externalPtr = *(reinterpret_cast<uint8_t**>(m_data + 1));
        return std::string_view(reinterpret_cast<char*>(externalPtr + sizeof(uint16_t)), *(reinterpret_cast<uint16_t*>(externalPtr)));
    }
}

StringBuffer::StringBuffer(std::shared_ptr<uint8_t[]> data, uint64_t capacity, uint64_t threshold)
    : m_threshold(std::min(threshold, MAX_EMBEDDED_LENGTH))
    , m_buffer(std::move(data))
    , m_capacity(capacity)
{
}

uint64_t StringBuffer::GetThreshold() const
{
    return m_threshold;
}

uint64_t StringBuffer::GetLength() const
{
    return m_length;
}

uint64_t& StringBuffer::GetLength()
{
    return m_length;
}

uint8_t* StringBuffer::GetBuffer()
{
    return m_buffer.get();
}

const uint8_t* StringBuffer::GetBuffer() const
{
    return m_buffer.get();
}

uint64_t StringBuffer::GetSize() const
{
    uint64_t embedded_size = m_threshold;
    uint64_t pointer_to_external_size = sizeof(uint8_t*);
    return std::max(embedded_size, pointer_to_external_size) + 1; // 1  -- len of str [0, 254], 255 -> external 
}

uint64_t StringBuffer::GetCapacity() const
{
    return m_capacity;
}

bool StringBuffer::IsEmbedded(uint64_t length) const
{
    return length <= m_threshold;
}
