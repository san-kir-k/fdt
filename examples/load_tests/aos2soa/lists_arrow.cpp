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

arrow::Status FillAoS(std::shared_ptr<AoS>& aos, uint64_t size, uint64_t cols, uint64_t list_len)
{
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    arrow::ListBuilder list_int64_builder(pool, std::make_shared<arrow::Int64Builder>(pool));
    arrow::Int64Builder* int64_builder = (dynamic_cast<arrow::Int64Builder*>(list_int64_builder.value_builder()));

    arrow::FieldVector to_schema;
    arrow::ArrayVector to_record;

    for (uint64_t il = 0; il < cols; ++il)
    {
        std::shared_ptr<arrow::Array> array;
        for (uint64_t i = 0; i < size; ++i)
        {
            auto sz = std::rand() % list_len + 1;
            TOTAL_SIZE += sz * sizeof(int64_t);
            ARROW_RETURN_NOT_OK(list_int64_builder.Append());
            ARROW_RETURN_NOT_OK(int64_builder->AppendValues(std::vector<int64_t>(sz, 42)));
        }
        ARROW_RETURN_NOT_OK(list_int64_builder.Finish(&array));
        int64_builder->Reset();
        list_int64_builder.Reset();

        to_record.push_back(std::dynamic_pointer_cast<arrow::ListArray>(array));
        to_schema.push_back(arrow::field(std::format("f_{}", il), arrow::list(arrow::int64())));
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
    std::vector<uint64_t> list_len_bounds = {16, 32, 64, 128};
    constexpr uint64_t cols_size = 10;

    for (uint64_t list_len: list_len_bounds)
    {
        std::cout << std::format(" list_len: {}\n", list_len);
        for (uint64_t rows: rows_lens)
        {
            std::cout << std::format("  length: {}\n", rows);
            std::shared_ptr<AoS> aos;
            std::shared_ptr<arrow::RecordBatch> record;

            auto status = FillAoS(aos, rows, cols_size, list_len);
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
    std::vector<uint64_t> list_len_bounds = {16, 32, 64, 128};
    constexpr uint64_t size = 100'000;

    for (uint64_t list_len: list_len_bounds)
    {
        std::cout << std::format(" list_len: {}\n", list_len);
        for (uint64_t cols: cols_lens)
        {
            std::cout << std::format("  columns: {}\n", cols);
            std::shared_ptr<AoS> aos;
            std::shared_ptr<arrow::RecordBatch> record;

            auto status = FillAoS(aos, size, cols, list_len);
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