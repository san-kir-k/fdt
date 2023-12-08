#pragma once

#include <cstdint>
#include <iostream>

#include <arrow/api.h>

#include "conversion/dispatch/aos2soa.h"
#include "pretty_type_traits.h"
#include "aos/aos.h"


inline std::shared_ptr<arrow::RecordBatch> AoS2SoA(const AoS& aos)
{
    auto buffers = aos.PrepareSoA();

    uint64_t datalen = aos.GetLength();
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
            auto p1 = buffers[i];
            std::cerr << "4: " << p1->size() << ", fieldsz: " << fieldsz << "\n";
            auto p2 = buffers[i + 1];
            auto p3 = buffers[i + 2];
            auto p4 = buffers[i + 3];

            AoS2SoAx4(aos.GetBuffer() + offset, aossz, p1, p2, p3, p4, fieldsz, datalen, fields[i]->type()->id());
            goto update;
        }
        transpose3:
        {
            auto p1 = buffers[i];
            std::cerr << "3: " << p1->size() << ", fieldsz: " << fieldsz << "\n";
            auto p2 = buffers[i + 1];
            auto p3 = buffers[i + 2];

            AoS2SoAx3(aos.GetBuffer() + offset, aossz, p1, p2, p3, fieldsz, datalen, fields[i]->type()->id());
            goto update;
        }
        transpose2:
        {
            auto p1 = buffers[i];
            std::cerr << "2: " << p1->size() << ", fieldsz: " << fieldsz << "\n";
            auto p2 = buffers[i + 1];

            AoS2SoAx2(aos.GetBuffer() + offset, aossz, p1, p2, fieldsz, datalen, fields[i]->type()->id());
            goto update;
        }
        transpose1:
        {
            auto p1 = buffers[i];
            std::cerr << "1: " << p1->size() << ", fieldsz: " << fieldsz << "\n";

            AoS2SoAx1(aos.GetBuffer() + offset, aossz, p1, fieldsz, datalen, fields[i]->type()->id());
            goto update;
        }
        update:
            offset += rbound * fieldsz;
            i += rbound;
    }

    std::vector<std::shared_ptr<arrow::ArrayData>> columns;

    for (uint64_t bi = 0; bi < buffers.size(); ++bi)
    {
        const auto& buffer = buffers[bi];
        if (arrow::is_numeric(fields[bi]->type()->id()))
        {
            columns.push_back(std::make_shared<arrow::ArrayData>(fields[bi]->type(), datalen, std::vector(1, buffer)));
        }
        else
        {
            assert(false && "Not implemented");
        }
    }

    return arrow::RecordBatch::Make(aos.GetSchema(), datalen, columns);
}
