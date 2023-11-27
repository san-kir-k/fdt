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
        String(uint8_t* data, uint64_t pos, const StringBuffer& parent);

        std::string_view DataView() const;

    private:
        uint8_t*            m_data;
        uint64_t            m_pos;
        const StringBuffer& m_parent;
    };

public:
    StringBuffer(const std::shared_ptr<arrow::StringArray>& data, uint64_t threshold);

    ~StringBuffer() = default;

    uint64_t GetThreshold() const;

private:
    bool IsEmbedded(uint64_t size) const;

private:
    uint64_t                            m_threshold;
    std::shared_ptr<arrow::StringArray> m_buffer;
};