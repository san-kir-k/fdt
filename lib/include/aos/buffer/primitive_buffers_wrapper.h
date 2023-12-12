#pragma once

#include <utility>
#include "aos/buffer/buffer.h"

class PrimitiveBuffersWrapper: public IBuffersWrapper
{
public:
    PrimitiveBuffersWrapper(const std::shared_ptr<arrow::Buffer>& bitmap,
                            const std::shared_ptr<arrow::Buffer>& data)
        : m_bitmap(std::move(bitmap))
        , m_data(std::move(data))
    {
    }

    virtual std::shared_ptr<arrow::Buffer> Get(uint64_t pos)
    {
        if (pos == 0)
        {
            return m_bitmap;
        }
        else
        {
            return m_data;
        }
    }

private:
    std::shared_ptr<arrow::Buffer>  m_bitmap;
    std::shared_ptr<arrow::Buffer>  m_data;
};