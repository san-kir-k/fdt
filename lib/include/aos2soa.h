#pragma once

#include <cstdint>
#include <cstring>
#include <vector>
#include <algorithm>
#include <memory>
#include <bit>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/type_traits.h>

#include "pretty_type_traits.h"
#include "aos.h"

void AoS2SoAx4( const uint8_t* out,
                uint64_t outsz,
                uint8_t* p1,
                uint8_t* p2,
                uint8_t* p3,
                uint8_t* p4,
                uint64_t insz,
                uint64_t datalen)
{
    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(insz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(p1 + insz * i, out + outsz * i,          insz);
        std::memcpy(p2 + insz * i, out + outsz * i + insz,   insz);
        std::memcpy(p3 + insz * i, out + outsz * i + 2*insz, insz);
        std::memcpy(p4 + insz * i, out + outsz * i + 3*insz, insz);
    }
}

void AoS2SoAx3( const uint8_t* out,
                uint64_t outsz,
                uint8_t* p1,
                uint8_t* p2,
                uint8_t* p3,
                uint64_t insz,
                uint64_t datalen)
{
    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(insz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(p1 + insz * i, out + outsz * i,          insz);
        std::memcpy(p2 + insz * i, out + outsz * i + insz,   insz);
        std::memcpy(p3 + insz * i, out + outsz * i + 2*insz, insz);
    }
}

void AoS2SoAx2( const uint8_t* out,
                uint64_t outsz,
                uint8_t* p1,
                uint8_t* p2,
                uint64_t insz,
                uint64_t datalen)
{
    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(insz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(p1 + insz * i, out + outsz * i,        insz);
        std::memcpy(p2 + insz * i, out + outsz * i + insz, insz);
    }
}

void AoS2SoAx1( const uint8_t* out,
                uint64_t outsz,
                uint8_t* p1,
                uint64_t insz,
                uint64_t datalen)
{
    static const void* gotoTable[] = {&&b8x1, &&b8x2, &&b8x4, &&b8x8};
    goto *gotoTable[std::countr_zero(insz)];

    b8x1:
    b8x2:
    b8x4:
    b8x8:
    for (uint64_t i = 0; i < datalen; ++i)
    {
        std::memcpy(p1 + insz * i, out + outsz * i, insz);
    }
}

void AoS2SoA(const AoS& aos, std::shared_ptr<arrow::RecordBatch>& record_batch)
{
    uint64_t datalen = record_batch->num_rows();
    uint64_t aossz = aos.GetStructSize();
    auto fields = aos.GetFields();

    static const void* gotoTable[] = {&&transpose1, &&transpose2, &&transpose3, &&transpose4};
    uint64_t i = 0;
    uint64_t offset = 0;

    while (i < fields.size())
    {
        uint64_t rbound = 1;
        for (; rbound < 4 && i + rbound < fields.size(); ++rbound)
        {
            if (GetCTypeSize(fields[i + rbound]->type()) != GetCTypeSize(fields[i + rbound - 1]->type()))
            {
                break;
            }
        }

        uint64_t insz = GetCTypeSize(fields[i]->type());

        goto *gotoTable[rbound - 1];

        transpose4:
        {
            auto* p1 = record_batch->GetColumnByName(fields[i]->name())->data()->GetMutableValues<uint8_t>(1);
            auto* p2 = record_batch->GetColumnByName(fields[i + 1]->name())->data()->GetMutableValues<uint8_t>(1);
            auto* p3 = record_batch->GetColumnByName(fields[i + 2]->name())->data()->GetMutableValues<uint8_t>(1);
            auto* p4 = record_batch->GetColumnByName(fields[i + 3]->name())->data()->GetMutableValues<uint8_t>(1);

            AoS2SoAx4(aos.GetBuffer() + offset, aossz, p1, p2, p3, p4, insz, datalen);
            goto update;
        }
        transpose3:
        {
            auto* p1 = record_batch->GetColumnByName(fields[i]->name())->data()->GetMutableValues<uint8_t>(1);
            auto* p2 = record_batch->GetColumnByName(fields[i + 1]->name())->data()->GetMutableValues<uint8_t>(1);
            auto* p3 = record_batch->GetColumnByName(fields[i + 2]->name())->data()->GetMutableValues<uint8_t>(1);

            AoS2SoAx3(aos.GetBuffer() + offset, aossz, p1, p2, p3, insz, datalen);
            goto update;
        }
        transpose2:
        {
            auto* p1 = record_batch->GetColumnByName(fields[i]->name())->data()->GetMutableValues<uint8_t>(1);
            auto* p2 = record_batch->GetColumnByName(fields[i + 1]->name())->data()->GetMutableValues<uint8_t>(1);

            AoS2SoAx2(aos.GetBuffer() + offset, aossz, p1, p2, insz, datalen);
            goto update;
        }
        transpose1:
        {
            auto* p1 = record_batch->GetColumnByName(fields[i]->name())->data()->GetMutableValues<uint8_t>(1);

            AoS2SoAx1(aos.GetBuffer() + offset, aossz, p1, insz, datalen);
            goto update;
        }
        update:
            offset += rbound * insz;
            i += rbound;
    }
}
