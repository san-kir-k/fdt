#include <arrow/api.h>
#include <arrow/result.h>

#include <cstdint>
#include <iomanip>
#include <iostream>
#include <vector>
#include <memory>

#include "arrow_transpose.h"
#include "utility.h"

// -------------------------------------------------------------------------

arrow::Status RunConversion(const std::shared_ptr<arrow::RecordBatch>& record, unsigned char* out)
{
    BENCHMARK("Naive arrow speed: ", record->num_rows() * (sizeof(int32_t) * 2 + sizeof(uint8_t) * 3 + sizeof(double)), SoA2AoS, record, out);
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

arrow::Status CreateAndFillRecordBatch(std::shared_ptr<arrow::RecordBatch>& record, uint64_t size)
{
    using arrow::Int64Builder;

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
    auto int64_array_a  = std::static_pointer_cast<arrow::Int64Array>(array_a);
    auto int64_array_b  = std::static_pointer_cast<arrow::Int64Array>(array_b);
    auto uint8t_array_x = std::static_pointer_cast<arrow::UInt8Array>(array_x);
    auto uint8t_array_y = std::static_pointer_cast<arrow::UInt8Array>(array_y);
    auto uint8t_array_z = std::static_pointer_cast<arrow::UInt8Array>(array_z);
    auto double_array_n = std::static_pointer_cast<arrow::DoubleArray>(array_n);

    // Create a table for the output
    auto schema = arrow::schema({
        arrow::field("a", arrow::int64()),
        arrow::field("b", arrow::int64()),
        arrow::field("x", arrow::uint8()),
        arrow::field("y", arrow::uint8()),
        arrow::field("z", arrow::uint8()),
        arrow::field("n", arrow::float64())
    });   

    record = arrow::RecordBatch::Make(schema, size, {array_a, array_b, array_x, array_y, array_z, array_n});
    return arrow::Status::OK();
}

// -------------------------------------------------------------------------

int main()
{
    constexpr uint64_t size = 10'000'000;
    alignas(8) unsigned char* dst = new unsigned char[size * (sizeof(int32_t) * 2 + sizeof(uint8_t) * 3 + sizeof(double))]{};

    std::shared_ptr<arrow::RecordBatch> record;

    auto status = CreateAndFillRecordBatch(record, size);
    if (!status.ok())
    {
        std::cerr << status.ToString() << std::endl;
        return EXIT_FAILURE;
    }

    status = RunConversion(record, dst);
    if (!status.ok())
    {
        std::cerr << status.ToString() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}