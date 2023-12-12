#pragma once

#include <cstdint>
#include <memory>
#include <arrow/buffer.h>

// Iface for variable-szied types like string in AoS format
class IBuffer
{
public:
    virtual ~IBuffer() = default;
};

// Wrapper for buffers needed to construct arrow::Array
class IBuffersWrapper
{
public:
    virtual ~IBuffersWrapper() = default;
    virtual std::shared_ptr<arrow::Buffer> Get(uint64_t pos) = 0;
};