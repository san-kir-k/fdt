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
#include "conversion/aos2soa.h"
#include "utility.h"

#include <iostream>

static uint64_t TOTAL_SIZE = 0;

// -------------------------------------------------------------------------

arrow::Status RunConversion(const AoS& aos, std::shared_ptr<arrow::RecordBatch>& out)
{
    BENCHMARK_RET("AoS -> SoA: Lists arrow speed: ", TOTAL_SIZE, out, AoS2SoA, aos);
    return arrow::Status::OK();
}

// -------------------------------------------------------------------------

template <typename ListValueType>
arrow::Status CheckField(const std::shared_ptr<arrow::RecordBatch>& record, const AoS& aos, const std::string& fieldname)
{
    if (static_cast<uint64_t>(record->num_rows()) != aos.GetLength())
    {
        return arrow::Status::RError(std::format("Sizes must be equal: {} != {}", record->num_rows(), aos.GetLength()));
    }

    auto lhs = std::dynamic_pointer_cast<arrow::ListArray>(record->GetColumnByName(fieldname));

    for (int64_t i = 0; i < record->num_rows(); i++)
    {
        const auto& l = aos[i];
        const auto* lhs_data = std::dynamic_pointer_cast<typename arrow::TypeTraits<ListValueType>::ArrayType>(lhs->values())->raw_values();
        std::vector<typename arrow::TypeTraits<ListValueType>::CType> lhs_slice(lhs_data + lhs->value_offset(i), lhs_data + lhs->value_offset(i + 1));
        auto rhs_slice = l.Value<arrow::ListArray, ListValueType>(fieldname).DataView();
        if (lhs_slice.size() != rhs_slice.size())
        {
            return arrow::Status::RError(std::format("Sizes of lists must be equal: i = {}, field = {}, {} != {}",
                                         i, fieldname, lhs_slice.size(), rhs_slice.size()));
        }
        if (!std::equal(lhs_slice.begin(), lhs_slice.end(), rhs_slice.begin()))
        {
            return arrow::Status::RError(std::format("Lists must be equal: i = {}, field = {}", i, fieldname));
        }
    }

    return arrow::Status::OK();
}

arrow::Status CheckEquality(const std::shared_ptr<arrow::RecordBatch>& record, const AoS& aos)
{
    if (auto status = CheckField<arrow::DoubleType>(record, aos, "a"); !status.ok())
    {
        return status;
    }
    if (auto status = CheckField<arrow::Int32Type>(record, aos, "b"); !status.ok())
    {
        return status;
    }
    if (auto status = CheckField<arrow::UInt8Type>(record, aos, "c"); !status.ok())
    {
        return status;
    }
    return arrow::Status::OK();
}

// -------------------------------------------------------------------------

arrow::Status FillAoS(std::shared_ptr<AoS>& aos, uint64_t size)
{
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    arrow::ListBuilder list_double_builder(pool, std::make_shared<arrow::DoubleBuilder>(pool));
    arrow::DoubleBuilder* double_builder = (dynamic_cast<arrow::DoubleBuilder*>(list_double_builder.value_builder()));

    arrow::ListBuilder list_int32_builder(pool, std::make_shared<arrow::Int32Builder>(pool));
    arrow::Int32Builder* int32_builder = (dynamic_cast<arrow::Int32Builder*>(list_int32_builder.value_builder()));

    arrow::ListBuilder list_uint8_builder(pool, std::make_shared<arrow::UInt8Builder>(pool));
    arrow::UInt8Builder* uint8_builder = (dynamic_cast<arrow::UInt8Builder*>(list_uint8_builder.value_builder()));

    // a
    std::shared_ptr<arrow::Array> array_a;
    uint64_t bound_a = 8;
    for (uint64_t i = 0; i < size; ++i)
    {
        auto sz = std::rand() % bound_a + 1;
        TOTAL_SIZE += sz * sizeof(double);
        ARROW_RETURN_NOT_OK(list_double_builder.Append());
        ARROW_RETURN_NOT_OK(double_builder->AppendValues(std::vector<double>(sz, 1.0)));
    }
    ARROW_RETURN_NOT_OK(list_double_builder.Finish(&array_a));

    // b
    std::shared_ptr<arrow::Array> array_b;
    uint64_t bound_b = 32;
    for (uint64_t i = 0; i < size; ++i)
    {
        auto sz = std::rand() % bound_b + 1;
        TOTAL_SIZE += sz * sizeof(int32_t);
        ARROW_RETURN_NOT_OK(list_int32_builder.Append());
        ARROW_RETURN_NOT_OK(int32_builder->AppendValues(std::vector<int32_t>(sz, 2)));
    }
    ARROW_RETURN_NOT_OK(list_int32_builder.Finish(&array_b));

    // c
    std::shared_ptr<arrow::Array> array_c;
    uint64_t bound_c = 128;
    for (uint64_t i = 0; i < size; ++i)
    {
        auto sz = std::rand() % bound_c + 1;
        TOTAL_SIZE += sz * sizeof(uint8_t);
        ARROW_RETURN_NOT_OK(list_uint8_builder.Append());
        ARROW_RETURN_NOT_OK(uint8_builder->AppendValues(std::vector<uint8_t>(sz, 3)));
    }
    ARROW_RETURN_NOT_OK(list_uint8_builder.Finish(&array_c));

    // Cast the arrays to their actual types
    auto list_array_a = std::dynamic_pointer_cast<arrow::ListArray>(array_a);
    auto list_array_b = std::dynamic_pointer_cast<arrow::ListArray>(array_b);
    auto list_array_c = std::dynamic_pointer_cast<arrow::ListArray>(array_c);

    // Create a table for the output
    auto schema = arrow::schema({
        arrow::field("a", arrow::list(arrow::float64())),
        arrow::field("b", arrow::list(arrow::int32())),
        arrow::field("c", arrow::list(arrow::uint8())),
    });

    aos = AoS::Make(schema, {list_array_a, list_array_b, list_array_c});
    return arrow::Status::OK();
}

// -------------------------------------------------------------------------

int main()
{
    std::srand(std::time(nullptr));

    constexpr uint64_t size = 1'000'000;

    std::shared_ptr<AoS> aos;
    std::shared_ptr<arrow::RecordBatch> record;

    auto status = FillAoS(aos, size);
    if (!status.ok())
    {
        std::cerr << status.ToString() << std::endl;
        return EXIT_FAILURE;
    }

    status = RunConversion(*aos, record);
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