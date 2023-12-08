#pragma once

#include <cstdint>
#include <iostream>

#include <arrow/api.h>

#include "conversion/dispatch/aos2soa.h"
#include "pretty_type_traits.h"
#include "aos/aos.h"


inline std::shared_ptr<arrow::RecordBatch> AoS2SoA(const AoS& aos)
{
    auto record_batch = aos.PrepareSoA();

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

        uint64_t fieldsz = GetCTypeSize(fields[i]->type());

        goto *gotoTable[rbound - 1];

        transpose4:
        {
            auto p1 = record_batch->GetColumnByName(fields[i]->name());
            auto p2 = record_batch->GetColumnByName(fields[i + 1]->name());
            auto p3 = record_batch->GetColumnByName(fields[i + 2]->name());
            auto p4 = record_batch->GetColumnByName(fields[i + 3]->name());

            AoS2SoAx4(aos.GetBuffer() + offset, aossz, p1, p2, p3, p4, fieldsz, datalen);
            goto update;
        }
        transpose3:
        {
            auto p1 = record_batch->GetColumnByName(fields[i]->name());
            auto p2 = record_batch->GetColumnByName(fields[i + 1]->name());
            auto p3 = record_batch->GetColumnByName(fields[i + 2]->name());

            AoS2SoAx3(aos.GetBuffer() + offset, aossz, p1, p2, p3, fieldsz, datalen);
            goto update;
        }
        transpose2:
        {
            auto p1 = record_batch->GetColumnByName(fields[i]->name());
            auto p2 = record_batch->GetColumnByName(fields[i + 1]->name());

            AoS2SoAx2(aos.GetBuffer() + offset, aossz, p1, p2, fieldsz, datalen);
            goto update;
        }
        transpose1:
        {
            auto p1 = record_batch->GetColumnByName(fields[i]->name());;;

            AoS2SoAx1(aos.GetBuffer() + offset, aossz, p1, fieldsz, datalen);
            goto update;
        }
        update:
            offset += rbound * fieldsz;
            i += rbound;
    }
    return record_batch;
}
