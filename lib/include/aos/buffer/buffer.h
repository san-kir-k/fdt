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
