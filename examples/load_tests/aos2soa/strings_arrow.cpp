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

static uint64_t TOTAL_SIZE = 0;

// -------------------------------------------------------------------------

arrow::Status RunConversion(const AoS& aos, std::shared_ptr<arrow::RecordBatch>& out)
{
    BENCHMARK_RET("   AoS -> SoA: Strings arrow speed: ", TOTAL_SIZE, out, AoS2SoA, aos);
    return arrow::Status::OK();
}

// -------------------------------------------------------------------------

arrow::Status FillAoS(std::shared_ptr<AoS>& aos, uint64_t size, uint64_t cols, uint64_t str_len)
{
    arrow::StringBuilder str_builder;
    arrow::FieldVector to_schema;
    arrow::ArrayVector to_record;

    for (uint64_t il = 0; il < cols; ++il)
    {
        std::shared_ptr<arrow::Array> array;
        ARROW_RETURN_NOT_OK(str_builder.Resize(size));
        ARROW_RETURN_NOT_OK(str_builder.ReserveData(size * str_len));
        for (uint64_t i = 0; i < size; ++i)
        {
            auto sz = std::rand() % str_len + 1;
            TOTAL_SIZE += sz;
            str_builder.UnsafeAppend(std::string(sz, 'a'));
        }
        ARROW_RETURN_NOT_OK(str_builder.Finish(&array));
        str_builder.Reset();

        to_record.push_back(std::dynamic_pointer_cast<arrow::StringArray>(array));
        to_schema.push_back(arrow::field(std::format("f_{}", il), arrow::utf8()));
    }

    auto schema = arrow::schema(to_schema);
    aos = AoS::Make(schema, to_record);
    return arrow::Status::OK();
}

// -------------------------------------------------------------------------

arrow::Status RunLongTests()
{
    std::cout << ">>> Running long tests (cols = 10) <<<\n";

    std::vector<uint64_t> rows_lens = {1'000, 10'000, 100'000, 1'000'000};
    std::vector<uint64_t> str_len_bounds = {32, 64, 128, 256};
    constexpr uint64_t cols_size = 10;

    for (uint64_t str_len: str_len_bounds)
    {
        std::cout << std::format(" str_len: {}\n", str_len);
        for (uint64_t rows: rows_lens)
        {
            std::cout << std::format("  length: {}\n", rows);
            std::shared_ptr<AoS> aos;
            std::shared_ptr<arrow::RecordBatch> record;

            auto status = FillAoS(aos, rows, cols_size, str_len);
            if (!status.ok())
            {
                std::cerr << status.ToString() << std::endl;
                return status;
            }

            status = RunConversion(*aos, record);
            if (!status.ok())
            {
                std::cerr << status.ToString() << std::endl;
                return status;
            }
            TOTAL_SIZE = 0;
        }
    }

    std::cout << "Done...\n\n";

    return arrow::Status::OK();
}

// -------------------------------------------------------------------------

arrow::Status RunWideTests()
{
    std::cout << ">>> Running wide tests (rows = 100'000) <<<\n";

    std::vector<uint64_t> cols_lens = {16, 32, 64, 128};
    std::vector<uint64_t> str_len_bounds = {32, 64, 128, 256};
    constexpr uint64_t size = 100'000;

    for (uint64_t str_len: str_len_bounds)
    {
        std::cout << std::format(" str_len: {}\n", str_len);
        for (uint64_t cols: cols_lens)
        {
            std::cout << std::format("  columns: {}\n", cols);
            std::shared_ptr<AoS> aos;
            std::shared_ptr<arrow::RecordBatch> record;

            auto status = FillAoS(aos, size, cols, str_len);
            if (!status.ok())
            {
                std::cerr << status.ToString() << std::endl;
                return status;
            }

            status = RunConversion(*aos, record);
            if (!status.ok())
            {
                std::cerr << status.ToString() << std::endl;
                return status;
            }
            TOTAL_SIZE = 0;
        }
    }

    std::cout << "Done...\n\n";

    return arrow::Status::OK();
}

// -------------------------------------------------------------------------

int main()
{
    std::cout << "------------------------ AoS -> SoA ------------------------\n";
    std::srand(std::time(nullptr));

    auto status = RunLongTests();
    if (!status.ok())
    {
        std::cerr << status.ToString() << std::endl;
        return EXIT_FAILURE;
    }

    status = RunWideTests();
    if (!status.ok())
    {
        std::cerr << status.ToString() << std::endl;
        return EXIT_FAILURE;
    }
    std::cout << "------------------------------------------------------------\n\n";

    return EXIT_SUCCESS;
}