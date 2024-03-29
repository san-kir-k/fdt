#include <arrow/api.h>
#include <arrow/result.h>

#include <cstdint>
#include <iomanip>
#include <iostream>
#include <vector>
#include <memory>
#include <string>
#include <format>

#include "aos/aos.h"
#include "conversion/aos2soa.h"
#include "utility.h"

// -------------------------------------------------------------------------

arrow::Status RunConversion(const AoS& aos, std::shared_ptr<arrow::RecordBatch>& out)
{
    BENCHMARK_RET("AoS -> SoA: Primitive arrow speed: ", aos.GetLength() * aos.GetStructSize(), out, AoS2SoA, aos);
    return arrow::Status::OK();
}

// -------------------------------------------------------------------------

template <typename ArrayDataType>
arrow::Status CheckField(const std::shared_ptr<arrow::RecordBatch>& record, const AoS& aos, const std::string& fieldname)
{
    using DataType = ArrayDataType::TypeClass;

    if (static_cast<uint64_t>(record->num_rows()) != aos.GetLength())
    {
        return arrow::Status::RError(std::format("Sizes must be equal: {} != {}", record->num_rows(), aos.GetLength()));
    }

    auto lhs = std::dynamic_pointer_cast<ArrayDataType>(record->GetColumnByName(fieldname));

    for (int64_t i = 0; i < record->num_rows(); i++)
    {
        const auto& s = aos[i];
        if (lhs->Value(i) != s.Value<DataType>(fieldname))
        {
            return arrow::Status::RError(std::format("Values must be equal: i = {}, field = {}, {} != {}",
                                         i, fieldname, lhs->Value(i), s.Value<DataType>(fieldname)));
        }
    }

    return arrow::Status::OK();
}

arrow::Status CheckEquality(const std::shared_ptr<arrow::RecordBatch>& record, const AoS& aos)
{
    if (auto status = CheckField<arrow::UInt8Array>(record, aos, "x"); !status.ok())
    {
        return status;
    }
    if (auto status = CheckField<arrow::UInt8Array>(record, aos, "y"); !status.ok())
    {
        return status;
    }
    if (auto status = CheckField<arrow::UInt8Array>(record, aos, "z"); !status.ok())
    {
        return status;
    }
    if (auto status = CheckField<arrow::Int32Array>(record, aos, "a"); !status.ok())
    {
        return status;
    }
    if (auto status = CheckField<arrow::Int32Array>(record, aos, "b"); !status.ok())
    {
        return status;
    }
    if (auto status = CheckField<arrow::DoubleArray>(record, aos, "n"); !status.ok())
    {
        return status;
    }
    return arrow::Status::OK();
}

// -------------------------------------------------------------------------

/*
struct SoA
{
    alignas(8) int*    a{};
    alignas(8) int*    b{};
    alignas(8) char*   x{};
    alignas(8) char*   y{};
    alignas(8) char*   z{};
    alignas(8) double* n{};
};
*/

arrow::Status FillAoS(std::shared_ptr<AoS>& aos, uint64_t size)
{
    arrow::NumericBuilder<arrow::Int32Type> int32_builder;
    ARROW_RETURN_NOT_OK(int32_builder.Resize(size));
    arrow::NumericBuilder<arrow::UInt8Type> uint8_builder;
    ARROW_RETURN_NOT_OK(uint8_builder.Resize(size));
    arrow::NumericBuilder<arrow::DoubleType> double_builder;
    ARROW_RETURN_NOT_OK(double_builder.Resize(size));

    // a
    std::shared_ptr<arrow::Array> array_a;
    std::vector<int32_t> values_a(size, 1);
    ARROW_RETURN_NOT_OK(int32_builder.AppendValues(values_a));
    ARROW_RETURN_NOT_OK(int32_builder.Finish(&array_a));
    int32_builder.Reset();

    // b
    std::shared_ptr<arrow::Array> array_b;
    std::vector<int32_t> values_b(size, 2);
    ARROW_RETURN_NOT_OK(int32_builder.AppendValues(values_b));
    ARROW_RETURN_NOT_OK(int32_builder.Finish(&array_b));
    int32_builder.Reset();

    // x
    std::shared_ptr<arrow::Array> array_x;
    std::vector<uint8_t> values_x(size, 3);
    ARROW_RETURN_NOT_OK(uint8_builder.AppendValues(values_x));
    ARROW_RETURN_NOT_OK(uint8_builder.Finish(&array_x));
    uint8_builder.Reset();

    // y
    std::shared_ptr<arrow::Array> array_y;
    std::vector<uint8_t> values_y(size, 4);
    ARROW_RETURN_NOT_OK(uint8_builder.AppendValues(values_y));
    ARROW_RETURN_NOT_OK(uint8_builder.Finish(&array_y));
    uint8_builder.Reset();

    // z
    std::shared_ptr<arrow::Array> array_z;
    std::vector<uint8_t> values_z(size, 5);
    ARROW_RETURN_NOT_OK(uint8_builder.AppendValues(values_z));
    ARROW_RETURN_NOT_OK(uint8_builder.Finish(&array_z));
    uint8_builder.Reset();

    // n
    std::shared_ptr<arrow::Array> array_n;
    std::vector<double> values_n(size, 42.0);
    ARROW_RETURN_NOT_OK(double_builder.AppendValues(values_n));
    ARROW_RETURN_NOT_OK(double_builder.Finish(&array_n));
    double_builder.Reset();

    // Cast the arrays to their actual types
    auto int64_array_a  = std::dynamic_pointer_cast<arrow::Int32Array>(array_a);
    auto int64_array_b  = std::dynamic_pointer_cast<arrow::Int32Array>(array_b);
    auto uint8t_array_x = std::dynamic_pointer_cast<arrow::UInt8Array>(array_x);
    auto uint8t_array_y = std::dynamic_pointer_cast<arrow::UInt8Array>(array_y);
    auto uint8t_array_z = std::dynamic_pointer_cast<arrow::UInt8Array>(array_z);
    auto double_array_n = std::dynamic_pointer_cast<arrow::DoubleArray>(array_n);

    // Create a table for the output
    auto schema = arrow::schema({
        arrow::field("a", arrow::int32()),
        arrow::field("b", arrow::int32()),
        arrow::field("x", arrow::uint8()),
        arrow::field("y", arrow::uint8()),
        arrow::field("z", arrow::uint8()),
        arrow::field("n", arrow::float64())
    });

    aos = AoS::Make(schema, {int64_array_a, int64_array_b, uint8t_array_x, uint8t_array_y, uint8t_array_z, double_array_n});
    return arrow::Status::OK();
}

// -------------------------------------------------------------------------

int main()
{
    constexpr uint64_t size = 10'000'000;

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