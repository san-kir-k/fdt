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
    auto process = [&aos] () {
        [[maybe_unused]] double sum = 0.0;
        for (uint64_t i = 0; i < aos.GetLength(); i++)
        {
            auto s = aos[i];
            sum += s.Value<arrow::DoubleType>("x");
        }
    };
    BENCHMARK_VOID_TIME("Pure AoS: ", process);
    return arrow::Status::OK();
}

arrow::Status ProcessDataPureSoA(const std::shared_ptr<arrow::RecordBatch>& soa)
{
    auto process = [&soa] () {
        [[maybe_unused]] double sum = 0.0;
        auto col = std::dynamic_pointer_cast<arrow::DoubleArray>(soa->GetColumnByName("x"));
        for (int64_t i = 0; i < soa->num_rows(); i++)
        {
            sum += col->Value(i);
        }
    };
    BENCHMARK_VOID_TIME("Pure SoA: ", process);
    return arrow::Status::OK();
}

arrow::Status ProcessDataAoSConvertedToSoA(const AoS& aos)
{
    auto process = [&aos] () {
        auto soa = AoS2SoA(aos);
        [[maybe_unused]] double sum = 0.0;
        auto col = std::dynamic_pointer_cast<arrow::DoubleArray>(soa->GetColumnByName("x"));
        for (int64_t i = 0; i < soa->num_rows(); i++)
        {
            sum += col->Value(i);
        }
    };
    BENCHMARK_VOID_TIME("AoS converted to SoA: ", process);
    return arrow::Status::OK();
}

// ------------------------------------------------------------------------

arrow::Status FillArays(uint64_t size, std::vector<std::shared_ptr<arrow::Array>>& arrays)
{
    arrow::NumericBuilder<arrow::DoubleType> double_builder;
    ARROW_RETURN_NOT_OK(double_builder.Resize(size));

    arrays.resize(4);
    for (int i = 0; i < 4; ++i)
    {
        std::vector<double> values(size);
        for (auto& v: values)
        {
            v = static_cast<double>(std::rand() % 100);
        }
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

    if (auto status = FillArays(size, arrays); !status.ok())
    {
        return status;
    }

    // Create a table for the output
    auto schema = arrow::schema({
        arrow::field("x", arrow::float64()),
        arrow::field("y", arrow::float64()),
        arrow::field("z", arrow::float64()),
        arrow::field("w", arrow::float64())
    });

    aos = AoS::Make(schema, arrays);
    return arrow::Status::OK();
}

arrow::Status FillSoA(std::shared_ptr<arrow::RecordBatch>& soa, uint64_t size)
{
    std::vector<std::shared_ptr<arrow::Array>> arrays;

    if (auto status = FillArays(size, arrays); !status.ok())
    {
        return status;
    }

    // Create a table for the output
    auto schema = arrow::schema({
        arrow::field("x", arrow::float64()),
        arrow::field("y", arrow::float64()),
        arrow::field("z", arrow::float64()),
        arrow::field("w", arrow::float64())
    });

    soa = arrow::RecordBatch::Make(schema, size, arrays);
    return arrow::Status::OK();
}

// -------------------------------------------------------------------------

int main()
{
    std::srand(std::time(nullptr));

    constexpr uint64_t size = 1'000'000;

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