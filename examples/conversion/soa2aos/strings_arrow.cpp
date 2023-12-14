#include <arrow/api.h>
#include <arrow/result.h>

#include <cstdlib>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <vector>
#include <memory>
#include <string>
#include <format>
#include <random>

#include "aos/aos.h"
#include "conversion/soa2aos.h"
#include "utility.h"

static uint64_t TOTAL_SIZE = 0;

// -------------------------------------------------------------------------

arrow::Status RunConversion(std::shared_ptr<arrow::RecordBatch> record, std::shared_ptr<AoS>& out)
{
    BENCHMARK_RET("SoA -> AoS: Strings arrow speed: ", TOTAL_SIZE, out, SoA2AoS, record);
    return arrow::Status::OK();
}

// -------------------------------------------------------------------------

template <typename ArrayDataType>
arrow::Status CheckField(const std::shared_ptr<arrow::RecordBatch>& record, const AoS& aos, const std::string& fieldname)
{
    if (static_cast<uint64_t>(record->num_rows()) != aos.GetLength())
    {
        return arrow::Status::RError(std::format("Sizes must be equal: {} != {}", record->num_rows(), aos.GetLength()));
    }

    auto lhs = std::static_pointer_cast<ArrayDataType>(record->GetColumnByName(fieldname));

    for (int64_t i = 0; i < record->num_rows(); i++)
    {
        const auto& s = aos[i];
        if (lhs->GetView(i) != s.Value(fieldname).DataView())
        {
            return arrow::Status::RError(std::format("Values must be equal: i = {}, field = {}, {} != {}",
                                         i, fieldname, lhs->GetView(i), s.Value(fieldname).DataView()));
        }
    }

    return arrow::Status::OK();
}

arrow::Status CheckEquality(const std::shared_ptr<arrow::RecordBatch>& record, const AoS& aos)
{
    if (auto status = CheckField<arrow::StringArray>(record, aos, "a"); !status.ok())
    {
        return status;
    }
    if (auto status = CheckField<arrow::StringArray>(record, aos, "b"); !status.ok())
    {
        return status;
    }
    if (auto status = CheckField<arrow::StringArray>(record, aos, "c"); !status.ok())
    {
        return status;
    }
    return arrow::Status::OK();
}

// -------------------------------------------------------------------------

arrow::Status CreateAndFillRecordBatch(std::shared_ptr<arrow::RecordBatch>& record, uint64_t size)
{
    arrow::StringBuilder str_builder;

    // a
    std::shared_ptr<arrow::Array> array_a;
    ARROW_RETURN_NOT_OK(str_builder.Resize(size));
    uint64_t bound_a = 10;
    ARROW_RETURN_NOT_OK(str_builder.ReserveData(size * bound_a));
    for (uint64_t i = 0; i < size; ++i)
    {
        auto sz = std::rand() % bound_a + 1;
        TOTAL_SIZE += sz;
        str_builder.UnsafeAppend(std::string(sz, 'a'));
    }
    ARROW_RETURN_NOT_OK(str_builder.Finish(&array_a));
    str_builder.Reset();

    // b
    std::shared_ptr<arrow::Array> array_b;
    ARROW_RETURN_NOT_OK(str_builder.Resize(size));
    uint64_t bound_b = 100;
    ARROW_RETURN_NOT_OK(str_builder.ReserveData(size * bound_b));
    for (uint64_t i = 0; i < size; ++i)
    {
        auto sz = std::rand() % bound_b + 1;
        TOTAL_SIZE += sz;
        str_builder.UnsafeAppend(std::string(sz, 'b'));
    }
    ARROW_RETURN_NOT_OK(str_builder.Finish(&array_b));
    str_builder.Reset();

    // c
    std::shared_ptr<arrow::Array> array_c;
    ARROW_RETURN_NOT_OK(str_builder.Resize(size));
    uint64_t bound_c = 1000;
    ARROW_RETURN_NOT_OK(str_builder.ReserveData(size * bound_c));
    for (uint64_t i = 0; i < size; ++i)
    {
        auto sz = std::rand() % bound_c + 1;
        TOTAL_SIZE += sz;
        str_builder.UnsafeAppend(std::string(sz, 'c'));
    }
    ARROW_RETURN_NOT_OK(str_builder.Finish(&array_c));
    str_builder.Reset();

    // Cast the arrays to their actual types
    auto str_array_a  = std::static_pointer_cast<arrow::StringArray>(array_a);
    auto str_array_b  = std::static_pointer_cast<arrow::StringArray>(array_b);
    auto str_array_c  = std::static_pointer_cast<arrow::StringArray>(array_c);

    // Create a table for the output
    auto schema = arrow::schema({
        arrow::field("a", arrow::utf8()),
        arrow::field("b", arrow::utf8()),
        arrow::field("c", arrow::utf8()),
    });

    record = arrow::RecordBatch::Make(schema, size, {array_a, array_b, array_c});
    return arrow::Status::OK();
}

// -------------------------------------------------------------------------

int main()
{
    std::srand(std::time(nullptr));

    auto schema = arrow::schema({
        arrow::field("a", arrow::utf8()),
        arrow::field("b", arrow::utf8()),
        arrow::field("c", arrow::utf8()),
    });

    constexpr uint64_t size = 1'000'000;

    std::shared_ptr<AoS> aos;
    std::shared_ptr<arrow::RecordBatch> record;

    auto status = CreateAndFillRecordBatch(record, size);
    if (!status.ok())
    {
        std::cerr << status.ToString() << std::endl;
        return EXIT_FAILURE;
    }

    status = RunConversion(record, aos);
    if (!status.ok())
    {
        std::cerr << status.ToString() << std::endl;
        return EXIT_FAILURE;
    }

    status = CheckEquality(record, *aos);
    if (!status.ok())
    {
        std::cerr << status.ToString() << std::endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}