import random
import argparse

base_types: list[str] = ["uint8_t", "uint16_t", "uint32_t", "uint64_t"]
arrow_mapping: dict = {"uint8_t": "UInt8", "uint16_t": "UInt16", "uint32_t": "UInt32", "uint64_t": "UInt64"}

includes_header: str = """
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
#include "conversion/soa2aos.h"
#include "utility.h"

"""

def construct_run_func(field_types: list[str], types_map: dict) -> str:
    run_func_str: str = "arrow::Status RunConversion(std::shared_ptr<arrow::RecordBatch> record, std::shared_ptr<AoS>& out)\n{\n"
    run_func_str += '    BENCHMARK_RET("SoA -> AoS: Primitive arrow speed: ", out->GetLength() * out->GetStructSize(), out, SoA2AoS, record);\n'
    run_func_str += '    return arrow::Status::OK();\n}\n\n'
    return run_func_str


def construct_fill_func(field_types: list[str], types_map: dict) -> str:
    fill_func_str: str = 'arrow::Status CreateAndFillRecordBatch(std::shared_ptr<arrow::RecordBatch>& record, uint64_t size)\n{\n'
    fill_func_str += '    arrow::NumericBuilder<arrow::UInt8Type> uint8_t_builder;\n    ARROW_RETURN_NOT_OK(uint8_t_builder.Resize(size));\n'
    fill_func_str += '    arrow::NumericBuilder<arrow::UInt16Type> uint16_t_builder;\n    ARROW_RETURN_NOT_OK(uint16_t_builder.Resize(size));\n'
    fill_func_str += '    arrow::NumericBuilder<arrow::UInt32Type> uint32_t_builder;\n    ARROW_RETURN_NOT_OK(uint32_t_builder.Resize(size));\n'
    fill_func_str += '    arrow::NumericBuilder<arrow::UInt64Type> uint64_t_builder;\n    ARROW_RETURN_NOT_OK(uint64_t_builder.Resize(size));\n\n'
    
    for i, type in enumerate(field_types):
        fill_func_str += f'    std::shared_ptr<arrow::Array> array_f{i};\n'
        fill_func_str += f'    std::vector<{type}> values_f{i}(size, 1);\n'
        fill_func_str += f'    ARROW_RETURN_NOT_OK({type}_builder.AppendValues(values_f{i}));\n'
        fill_func_str += f'    ARROW_RETURN_NOT_OK({type}_builder.Finish(&array_f{i}));\n'
        fill_func_str += f'    {type}_builder.Reset();\n'
        fill_func_str += f'    auto {type}_array_f{i}  = std::dynamic_pointer_cast<arrow::{arrow_mapping[type]}Array>(array_f{i});\n\n'
    
    fill_func_str += f'    auto schema = arrow::schema({{\n'
    for i, type in enumerate(field_types):
        fill_func_str += f'        arrow::field("f{i}", arrow::{type[:-2]}()),\n'
    fill_func_str += f'    }});\n\n'

    fill_func_str += '    record = arrow::RecordBatch::Make(schema, size, {'
    for i, type in enumerate(field_types):
        fill_func_str += f'{type}_array_f{i}, '
    fill_func_str += '});\n'
    fill_func_str += '    return arrow::Status::OK();\n}\n\n'
    
    return fill_func_str


def construct_main_func(field_types: list[str], types_map: dict) -> str:
    main_func_str: str = "int main()\n{\n"
    
    main_func_str += f'    auto schema = arrow::schema({{\n'
    for i, type in enumerate(field_types):
        main_func_str += f'        arrow::field("f{i}", arrow::{type[:-2]}()),\n'
    main_func_str += f'    }});\n\n'
    
    main_func_str += "    constexpr uint64_t size = 100000;\n    std::shared_ptr<AoS> aos;\n    std::shared_ptr<arrow::RecordBatch> record;\n"
    main_func_str += "    auto status = CreateAndFillRecordBatch(record, size);\n    if (!status.ok())\n    {\n"
    main_func_str += "        std::cerr << status.ToString() << std::endl;\n        return EXIT_FAILURE;\n    }\n\n"
    
    main_func_str += "    status = RunConversion(record, aos);\n    if (!status.ok())\n    {\n        std::cerr << status.ToString() << std::endl;\n"
    main_func_str += "        return EXIT_FAILURE;\n    }\n"
    
    main_func_str += '    return EXIT_SUCCESS;\n}\n'
    
    return main_func_str


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--number', help='number of fields in table')
    args = parser.parse_args()

    types: list[str] = []
    types_map: dict= {
        "uint8_t":  0,
        "uint16_t": 0,
        "uint32_t": 0,
        "uint64_t": 0,
    }
    
    
    for i in range(int(args.number)):
        types.append(base_types[i % 4])
        types_map[types[-1]] += 1
    
    with open(f'arrow_n{args.number}.gen.cpp', 'w') as out:
        out.write(includes_header)
        out.write(construct_run_func(types, types_map))
        out.write(construct_fill_func(types, types_map))
        out.write(construct_main_func(types, types_map))
    