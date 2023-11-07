#pragma once

#include <cstdint>
#include <cstring>
#include <vector>  // instead of arrow
#include <algorithm>

void SoA2AoSx4( unsigned char* p1,     uint64_t sz1,
                unsigned char* p2,     uint64_t sz2,
                unsigned char* p3,     uint64_t sz3,
                unsigned char* p4,     uint64_t sz4,
                unsigned char* out,    uint64_t outsz,
                uint64_t datalen)
{
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(out + outsz * i, p1 + sz1 * i, sz1);
        std::memcpy(out + outsz * i + sz1, p2 + sz2 * i, sz2);
        std::memcpy(out + outsz * i + sz1 + sz2, p3 + sz3 * i, sz3);
        std::memcpy(out + outsz * i + sz1 + sz2 + sz3, p4 + sz4 * i, sz4);
    }
}

void SoA2AoSx3( unsigned char* p1,     uint64_t sz1,
                unsigned char* p2,     uint64_t sz2,
                unsigned char* p3,     uint64_t sz3,
                unsigned char* out,    uint64_t outsz,
                uint64_t datalen)
{
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(out + outsz * i, p1 + sz1 * i, sz1);
        std::memcpy(out + outsz * i + sz1, p2 + sz2 * i, sz2);
        std::memcpy(out + outsz * i + sz1 + sz2, p3 + sz3 * i, sz3);
    }
}

void SoA2AoSx2( unsigned char* p1,     uint64_t sz1,
                unsigned char* p2,     uint64_t sz2,
                unsigned char* out,    uint64_t outsz,
                uint64_t datalen)
{
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(out + outsz * i, p1 + sz1 * i, sz1);
        std::memcpy(out + outsz * i + sz1, p2 + sz2 * i, sz2);
    }
}

void SoA2AoSx1( unsigned char* p1,     uint64_t sz1,
                unsigned char* out,    uint64_t outsz,
                uint64_t datalen)
{
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(out + outsz * i, p1 + sz1 * i, sz1);
    }
}

struct AoSField
{
    unsigned char*  ptr;
    uint64_t        size;
};

void SoA2AoS(std::vector<AoSField>& table, unsigned char* out, uint64_t datalen)
{
    // counting sort?
    std::sort(table.begin(), table.end(), [](const auto& l, const auto& r) -> bool {
        return l.size < r.size;
    });

    uint64_t outsz = 0;
    for (const auto& field: table)
    {
        outsz += field.size;
    }

    static const void* gotoTable[] = {&&transpose1, &&transpose2, &&transpose3, &&transpose4};
    uint64_t i = 0;
    uint64_t offset = 0;

    while (i < table.size())
    {
        uint64_t rbound = 1;
        for (; rbound < 4 && i + rbound < table.size(); ++rbound)
        {
            if (table[i + rbound].size != table[i + rbound - 1].size)
            {
                break;
            }
        }

        goto *gotoTable[rbound - 1];

        transpose4:
            SoA2AoSx4(table[i].ptr, table[i].size,
                      table[i + 1].ptr, table[i + 1].size,
                      table[i + 2].ptr, table[i + 2].size,
                      table[i + 3].ptr, table[i + 3].size,
                      out + offset, outsz,
                      datalen);
            goto update;
        transpose3:
            SoA2AoSx3(table[i].ptr, table[i].size,
                      table[i + 1].ptr, table[i + 1].size,
                      table[i + 2].ptr, table[i + 2].size,
                      out + offset, outsz,
                      datalen);
            goto update;
        transpose2:
            SoA2AoSx2(table[i].ptr, table[i].size,
                      table[i + 1].ptr, table[i + 1].size,
                      out + offset, outsz,
                      datalen);
            goto update;
        transpose1:
            SoA2AoSx1(table[i].ptr, table[i].size,
                      out + offset, outsz,
                      datalen);
            goto update;
        update:
            offset += rbound * table[i].size;
            i += rbound;
    }
}
