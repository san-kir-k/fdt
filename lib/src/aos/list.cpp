#include "aos/types/list.h"

#include <utility>


ListBuffer::ListBuffer(std::shared_ptr<uint8_t[]> data, uint64_t capacity, uint64_t element_sz)
    : m_buffer(std::move(data))
    , m_capacity(capacity)
    , m_elemSize(element_sz)
{
}

uint64_t ListBuffer::GetElementsCount() const
{
    return m_elemCount;
}

uint64_t& ListBuffer::GetElementsCount()
{
    return m_elemCount;
}

uint64_t ListBuffer::GetLength() const
{
    return m_length;
}

uint64_t& ListBuffer::GetLength()
{
    return m_length;
}

uint8_t* ListBuffer::GetBuffer()
{
    return m_buffer.get();
}

const uint8_t* ListBuffer::GetBuffer() const
{
    return m_buffer.get();
}

uint64_t ListBuffer::GetSize() const
{
    uint64_t pointer_to_external_size = sizeof(uint8_t*);
    return pointer_to_external_size;
}

uint64_t ListBuffer::GetCapacity() const
{
    return m_capacity;
}

uint64_t ListBuffer::GetElementSize() const
{
    return m_elemSize;
}
