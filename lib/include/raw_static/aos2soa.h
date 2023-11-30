#pragma once

#include <cstdint>
#include <cstring>
#include <vector>  // instead of arrow
#include <algorithm>

#include "soa_field.h"

void AoS2SoAx4( uint8_t* p1,     uint64_t sz1,
                uint8_t* p2,     uint64_t sz2,
                uint8_t* p3,     uint64_t sz3,
                uint8_t* p4,     uint64_t sz4,
                uint8_t* out,    uint64_t outsz,
                uint64_t datalen)
{
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(p1 + sz1 * i, out + outsz * i, sz1);
        std::memcpy(p2 + sz2 * i, out + outsz * i + sz1, sz2);
        std::memcpy(p3 + sz3 * i, out + outsz * i + sz1 + sz2, sz3);
        std::memcpy(p4 + sz4 * i, out + outsz * i + sz1 + sz2 + sz3, sz4);
    }
}

void AoS2SoAx3( uint8_t* p1,     uint64_t sz1,
                uint8_t* p2,     uint64_t sz2,
                uint8_t* p3,     uint64_t sz3,
                uint8_t* out,    uint64_t outsz,
                uint64_t datalen)
{
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(p1 + sz1 * i, out + outsz * i, sz1);
        std::memcpy(p2 + sz2 * i, out + outsz * i + sz1, sz2);
        std::memcpy(p3 + sz3 * i, out + outsz * i + sz1 + sz2, sz3);
    }
}

void AoS2SoAx2( uint8_t* p1,     uint64_t sz1,
                uint8_t* p2,     uint64_t sz2,
                uint8_t* out,    uint64_t outsz,
                uint64_t datalen)
{
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(p1 + sz1 * i, out + outsz * i, sz1);
        std::memcpy(p2 + sz2 * i, out + outsz * i + sz1, sz2);
    }
}

void AoS2SoAx1( uint8_t* p1,     uint64_t sz1,
                uint8_t* out,    uint64_t outsz,
                uint64_t datalen)
{
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(p1 + sz1 * i, out + outsz * i, sz1);
    }
}

void AoS2SoA(uint8_t* table, std::vector<SoAField>& out, uint64_t datalen)
{
    std::sort(out.begin(), out.end(), [](const auto& l, const auto& r) -> bool {
        return l.size < r.size;
    });

    uint64_t outsz = 0;
    for (const auto& field: out)
    {
        outsz += field.size;
    }

    static const void* gotoTable[] = {&&transpose1, &&transpose2, &&transpose3, &&transpose4};
    uint64_t i = 0;
    uint64_t offset = 0;

    while (i < out.size())
    {
        uint64_t rbound = 1;
        for (; rbound < 4 && i + rbound < out.size(); ++rbound)
        {
            if (out[i + rbound].size != out[i + rbound - 1].size)
            {
                break;
            }
        }

        goto *gotoTable[rbound - 1];

        transpose4:
            AoS2SoAx4(out[i].ptr, out[i].size,
                      out[i + 1].ptr, out[i + 1].size,
                      out[i + 2].ptr, out[i + 2].size,
                      out[i + 3].ptr, out[i + 3].size,
                      table + offset, outsz,
                      datalen);
            goto update;
        transpose3:
            AoS2SoAx3(out[i].ptr, out[i].size,
                      out[i + 1].ptr, out[i + 1].size,
                      out[i + 2].ptr, out[i + 2].size,
                      table + offset, outsz,
                      datalen);
            goto update;
        transpose2:
            AoS2SoAx2(out[i].ptr, out[i].size,
                      out[i + 1].ptr, out[i + 1].size,
                      table + offset, outsz,
                      datalen);
            goto update;
        transpose1:
            AoS2SoAx1(out[i].ptr, out[i].size,
                      table + offset, outsz,
                      datalen);
            goto update;
        update:
            offset += rbound * out[i].size;
            i += rbound;
    }
}
