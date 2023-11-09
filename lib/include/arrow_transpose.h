#pragma once

#include <cstdint>
#include <cstring>
#include <vector>
#include <algorithm>
#include <memory>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/type_traits.h>

#include "pretty_type_traits.h"

void SoA2AoSx4( const unsigned char* p1,
                const unsigned char* p2,
                const unsigned char* p3,
                const unsigned char* p4,
                uint64_t insz,
                unsigned char* out,
                uint64_t outsz,
                uint64_t datalen)
{
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(out + outsz * i,          p1 + insz * i, insz);
        std::memcpy(out + outsz * i + insz,   p2 + insz * i, insz);
        std::memcpy(out + outsz * i + 2*insz, p3 + insz * i, insz);
        std::memcpy(out + outsz * i + 3*insz, p4 + insz * i, insz);
    }
}

void SoA2AoSx3( const unsigned char* p1,
                const unsigned char* p2,
                const unsigned char* p3,
                uint64_t insz,
                unsigned char* out,
                uint64_t outsz,
                uint64_t datalen)
{
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(out + outsz * i,          p1 + insz * i, insz);
        std::memcpy(out + outsz * i + insz,   p2 + insz * i, insz);
        std::memcpy(out + outsz * i + 2*insz, p3 + insz * i, insz);
    }
}

void SoA2AoSx2( const unsigned char* p1,
                const unsigned char* p2,
                uint64_t insz,
                unsigned char* out,
                uint64_t outsz,
                uint64_t datalen)
{
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(out + outsz * i,        p1 + insz * i, insz);
        std::memcpy(out + outsz * i + insz, p2 + insz * i, insz);
    }
}

void SoA2AoSx1( const unsigned char* p1,
                uint64_t insz,
                unsigned char* out,
                uint64_t outsz,
                uint64_t datalen)
{
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(out + outsz * i, p1 + insz * i, insz);
    }
}

void SoA2AoS(const std::shared_ptr<arrow::RecordBatch>& record_batch, unsigned char* out)
{
    uint64_t datalen = record_batch->num_rows();
    auto columns = record_batch->columns();

    // counting sort?
    std::sort(columns.begin(), columns.end(), [](const auto& lhs, const auto& rhs) -> bool {
        return GetCTypeSize(lhs->type()) < GetCTypeSize(rhs->type());
    });

    uint64_t outsz = 0;
    for (const auto& col: columns)
    {
        outsz += GetCTypeSize(col->type());
    }

    static const void* gotoTable[] = {&&transpose1, &&transpose2, &&transpose3, &&transpose4};
    uint64_t i = 0;
    uint64_t offset = 0;

    while (i < columns.size())
    {
        uint64_t rbound = 1;
        for (; rbound < 4 && i + rbound < columns.size(); ++rbound)
        {
            if (GetCTypeSize(columns[i + rbound]->type()) != GetCTypeSize(columns[i + rbound - 1]->type()))
            {
                break;
            }
        }

        uint64_t insz = GetCTypeSize(columns[i]->type());

        goto *gotoTable[rbound - 1];

        transpose4:
        {
            auto* p1 = columns[i]->data()->GetValues<unsigned char>(1, 0);
            auto* p2 = columns[i + 1]->data()->GetValues<unsigned char>(1, 0);
            auto* p3 = columns[i + 2]->data()->GetValues<unsigned char>(1, 0);
            auto* p4 = columns[i + 3]->data()->GetValues<unsigned char>(1, 0);

            SoA2AoSx4(p1, p2, p3, p4, insz, out + offset, outsz, datalen);
            goto update;
        }
        transpose3:
        {
            auto* p1 = columns[i]->data()->GetValues<unsigned char>(1, 0);
            auto* p2 = columns[i + 1]->data()->GetValues<unsigned char>(1, 0);
            auto* p3 = columns[i + 2]->data()->GetValues<unsigned char>(1, 0);

            SoA2AoSx3(p1, p2, p3, insz, out + offset, outsz, datalen);
            goto update;
        }
        transpose2:
        {
            auto* p1 = columns[i]->data()->GetValues<unsigned char>(1, 0);
            auto* p2 = columns[i + 1]->data()->GetValues<unsigned char>(1, 0);

            SoA2AoSx2(p1, p2, insz, out + offset, outsz, datalen);
            goto update;
        }
        transpose1:
        {
            auto* p1 = columns[i]->data()->GetValues<unsigned char>(1, 0);

            SoA2AoSx1(p1, insz, out + offset, outsz, datalen);
            goto update;
        }
        update:
            offset += rbound * insz;
            i += rbound;
    }
}
