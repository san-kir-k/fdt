#pragma once

#include <arrow/type_fwd.h>
#include <arrow/array/array_binary.h>
#include <memory>
#include <string_view>
#include <cstdint>
#include <cstring>

#include "aos/buffer/buffer.h"

class StringBuffer: public IBuffer
{
public:
    class String
    {
    public:
        String(uint8_t* data, const StringBuffer& parent);

        std::string_view DataView() const;

    private:
        uint8_t*            m_data;
        const StringBuffer& m_parent;
    };

public:
    StringBuffer(const std::shared_ptr<uint8_t[]>& data, uint64_t capacity, uint64_t threshold);
    ~StringBuffer() = default;

    uint64_t GetThreshold() const;

    uint64_t GetLength() const;
    uint64_t& GetLength();

    uint8_t* GetBuffer();
    const uint8_t* GetBuffer() const;

    uint64_t GetSize() const;

    uint64_t GetCapacity() const;

    bool IsEmbedded(uint64_t length) const;

private:
    constexpr static uint64_t MAX_EMBEDDED_LENGTH = 254;

private:
    uint64_t                   m_threshold;
    std::shared_ptr<uint8_t[]> m_buffer;
    uint64_t                   m_length{};
    uint64_t                   m_capacity{};
};