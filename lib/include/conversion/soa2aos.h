#pragma once

#include <cstdint>

#include <arrow/api.h>

#include "conversion/dispatch/soa2aos.h"
#include "pretty_type_traits.h"
#include "aos/aos.h"
#include "aos/types/comparator.h"


inline std::shared_ptr<AoS> SoA2AoS(std::shared_ptr<arrow::RecordBatch> record_batch)
{
    std::shared_ptr<AoS> aos(new AoS(record_batch->schema()));
    aos->PrepareSelf(record_batch);

    auto fields = aos->GetFields();

    static const void* gotoTable[] = {&&transpose1, &&transpose2, &&transpose3, &&transpose4};
    uint64_t i = 0;

    while (i < fields.size())
    {
        uint64_t rbound = 1;
        for (; rbound < 4 && i + rbound < fields.size(); ++rbound)
        {
            if (TypesComparator{}.neq(fields[i + rbound], fields[i + rbound - 1]))
            {
                break;
            }
        }

        goto *gotoTable[rbound - 1];

        transpose4:
        {
            auto p1 = record_batch->GetColumnByName(fields[i]->name());
            auto p2 = record_batch->GetColumnByName(fields[i + 1]->name());
            auto p3 = record_batch->GetColumnByName(fields[i + 2]->name());
            auto p4 = record_batch->GetColumnByName(fields[i + 3]->name());

            SoA2AoSx4(p1, p2, p3, p4, *aos, i);
            goto update;
        }
        transpose3:
        {
            auto p1 = record_batch->GetColumnByName(fields[i]->name());
            auto p2 = record_batch->GetColumnByName(fields[i + 1]->name());
            auto p3 = record_batch->GetColumnByName(fields[i + 2]->name());

            SoA2AoSx3(p1, p2, p3, *aos, i);
            goto update;
        }
        transpose2:
        {
            auto p1 = record_batch->GetColumnByName(fields[i]->name());
            auto p2 = record_batch->GetColumnByName(fields[i + 1]->name());

            SoA2AoSx2(p1, p2, *aos, i);
            goto update;
        }
        transpose1:
        {
            auto p1 = record_batch->GetColumnByName(fields[i]->name());

            SoA2AoSx1(p1, *aos, i);
            goto update;
        }
        update:
            i += rbound;
    }

    return aos;
}
