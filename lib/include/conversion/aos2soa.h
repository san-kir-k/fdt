#pragma once

#include <cstdint>

#include <arrow/api.h>

#include "conversion/dispatch/aos2soa.h"
#include "pretty_type_traits.h"
#include "aos/aos.h"
#include "aos/types/comparator.h"


inline std::shared_ptr<arrow::RecordBatch> AoS2SoA(const AoS& aos)
{
    auto arrays_data = aos.PrepareArrayData();

    uint64_t datalen = aos.GetLength();
    auto fields = aos.GetFields();

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
            auto p1 = arrays_data[i];
            auto p2 = arrays_data[i + 1];
            auto p3 = arrays_data[i + 2];
            auto p4 = arrays_data[i + 3];

            AoS2SoAx4(aos, p1, p2, p3, p4, i);
            goto update;
        }
        transpose3:
        {
            auto p1 = arrays_data[i];
            auto p2 = arrays_data[i + 1];
            auto p3 = arrays_data[i + 2];

            AoS2SoAx3(aos, p1, p2, p3, i);
            goto update;
        }
        transpose2:
        {
            auto p1 = arrays_data[i];
            auto p2 = arrays_data[i + 1];

            AoS2SoAx2(aos, p1, p2, i);
            goto update;
        }
        transpose1:
        {
            auto p1 = arrays_data[i];

            AoS2SoAx1(aos, p1, i);
            goto update;
        }
        update:
            i += rbound;
    }

    return arrow::RecordBatch::Make(aos.GetSchema(), datalen, arrays_data);
}
