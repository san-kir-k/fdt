#include <arrow/api.h>
#include <arrow/result.h>

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
#include "conversion/soa2aos.h"
#include "utility.h"

// -------------------------------------------------------------------------

arrow::Status ProcessDataPureAoS(const AoS& aos)
{
    double ans = 0;
    auto process = [&aos, &ans] () {
        double sum = 0.0;
        auto idx = aos.GetSchema()->GetFieldIndex("f_0");
        for (uint64_t i = 0; i < aos.GetLength(); i++)
        {
            auto s = aos[i];
            sum += s.Value<arrow::DoubleType>(idx);
        }
        ans = sum;
    };
    BENCHMARK_VOID_TIME("Pure AoS: ", process);
    std::cout << "Answer is: " << ans << "\n";
    return arrow::Status::OK();
}

arrow::Status ProcessDataPureSoA(const std::shared_ptr<arrow::RecordBatch>& soa)
{
    double ans = 0;
    auto process = [&soa, &ans] () {
        double sum = 0.0;
        auto col = std::dynamic_pointer_cast<arrow::DoubleArray>(soa->GetColumnByName("f_0"));
        for (int64_t i = 0; i < soa->num_rows(); i++)
        {
            sum += col->Value(i);
        }
        ans = sum;
    };
    BENCHMARK_VOID_TIME("Pure SoA: ", process);
    std::cout << "Answer is: " << ans << "\n";
    return arrow::Status::OK();
}

arrow::Status ProcessDataAoSConvertedToSoA(const AoS& aos)
{
    double ans = 0;
    auto process = [&aos, &ans] () {
        auto soa = AoS2SoA(aos);
        double sum = 0.0;
        auto col = std::dynamic_pointer_cast<arrow::DoubleArray>(soa->GetColumnByName("f_0"));
        for (int64_t i = 0; i < soa->num_rows(); i++)
        {
            sum += col->Value(i);
        }
        ans = sum;
    };
    BENCHMARK_VOID_TIME("AoS converted to SoA: ", process);
    std::cout << "Answer is: " << ans << "\n";
    return arrow::Status::OK();
}

// ------------------------------------------------------------------------

arrow::Status FillArays(uint64_t size, std::vector<std::shared_ptr<arrow::Array>>& arrays, arrow::FieldVector& fields)
{
    arrow::NumericBuilder<arrow::DoubleType> double_builder;
    ARROW_RETURN_NOT_OK(double_builder.Resize(size));

    uint8_t cols = 4;
    arrays.resize(cols);
    for (int i = 0; i < cols; ++i)
    {
        std::vector<double> values(size);
        for (auto& v: values)
        {
            v = static_cast<double>(std::rand() % 5);
        }
        fields.push_back(arrow::field(std::format("f_{}", i), arrow::float64()));
        ARROW_RETURN_NOT_OK(double_builder.AppendValues(values));
        ARROW_RETURN_NOT_OK(double_builder.Finish(&arrays[i]));
        double_builder.Reset();
    }

    return arrow::Status::OK();
}

// ------------------------------------------------------------------------

arrow::Status FillAoS(std::shared_ptr<AoS>& aos, uint64_t size)
{
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrow::FieldVector fields;

    if (auto status = FillArays(size, arrays, fields); !status.ok())
    {
        return status;
    }

    // Create a table for the output
    auto schema = arrow::schema(fields);
    aos = AoS::Make(schema, arrays);
    return arrow::Status::OK();
}

arrow::Status FillSoA(std::shared_ptr<arrow::RecordBatch>& soa, uint64_t size)
{
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrow::FieldVector fields;

    if (auto status = FillArays(size, arrays, fields); !status.ok())
    {
        return status;
    }

    // Create a table for the output
    auto schema = arrow::schema(fields);
    soa = arrow::RecordBatch::Make(schema, size, arrays);
    return arrow::Status::OK();
}

// -------------------------------------------------------------------------

int main()
{
    std::srand(std::time(nullptr));

    constexpr uint64_t size = 10'000'000;

    std::shared_ptr<AoS> aos;
    std::shared_ptr<arrow::RecordBatch> soa;

    if (auto status = FillAoS(aos, size); !status.ok())
    {
        std::cerr << status.ToString() << std::endl;
        return EXIT_FAILURE;
    }

    if (auto status = FillSoA(soa, size); !status.ok())
    {
        std::cerr << status.ToString() << std::endl;
        return EXIT_FAILURE;
    }

    if (auto status = ProcessDataPureAoS(*aos); !status.ok())
    {
        std::cerr << status.ToString() << std::endl;
        return EXIT_FAILURE;
    }

    if (auto status = ProcessDataPureSoA(soa); !status.ok())
    {
        std::cerr << status.ToString() << std::endl;
        return EXIT_FAILURE;
    }

    if (auto status = ProcessDataAoSConvertedToSoA(*aos); !status.ok())
    {
        std::cerr << status.ToString() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}