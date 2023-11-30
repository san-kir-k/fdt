import random
import argparse

base_types: list[str] = ["uint8_t", "uint16_t", "uint32_t", "uint64_t"]

includes_header: str = """
#include <iostream>
#include <cstdint>
#include <cstring>
#include <cassert>

#include "utility.h"
#include "raw_static/soa2aos.h"

"""

def construct_soa_struct(field_types: list[str], types_map: dict) -> str:
    struct_str: str = "struct alignas(8) SoA\n{\n"
    
    for i, type in enumerate(field_types):
        struct_str += f"    {type}*     f{i}{{}};\n"
    
    struct_str += "};\n\n"
    return struct_str


def construct_run_func(field_types: list[str], types_map: dict) -> str:
    run_func_str: str = "void Run(SoA& src, uint8_t* dst, uint64_t size)\n{\n    std::vector<SoAField> table = {\n"
    
    for i, type in enumerate(field_types):
        run_func_str += f"        SoAField{{.ptr = reinterpret_cast<uint8_t*>(src.f{i}), .size = sizeof({type})}},\n"
    run_func_str += "    };\n"
    
    run_func_str += f'    BENCHMARK("SoA -> AoS: Raw static speed: ", size * (sizeof({base_types[0]}) * {types_map[base_types[0]]}'\
                    f' + sizeof({base_types[1]}) * {types_map[base_types[1]]} + sizeof({base_types[2]}) * {types_map[base_types[2]]}'\
                    f' + sizeof({base_types[3]}) * {types_map[base_types[3]]}), SoA2AoS, table, dst, size);\n' + '}\n\n'
    return run_func_str


def construct_main_func(field_types: list[str], types_map: dict) -> str:
    main_func_str: str = "int main()\n{\n    constexpr uint64_t size = 100000;\n    SoA soa;\n"
    
    for i, type in enumerate(field_types):
        main_func_str += f"    soa.f{i} = new {type}[size]{{}};\n"
    
    main_func_str += f'    alignas(8) uint8_t* dst = new unsigned char[size * ('\
                     f'sizeof({base_types[0]}) * {types_map[base_types[0]]} + sizeof({base_types[1]}) * {types_map[base_types[1]]} + '\
                     f'sizeof({base_types[2]}) * {types_map[base_types[2]]} + sizeof({base_types[3]}) * {types_map[base_types[3]]})]{{}};\n'
    
    main_func_str += '    Run(soa, dst, size);\n    delete[] dst;\n'
    
    for i, _ in enumerate(field_types):
        main_func_str += f"    delete[] soa.f{i};\n"
    
    main_func_str += '    return 0;\n}\n'
    
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
    
    with open(f'static_raw_n{args.number}.gen.cpp', 'w') as out:
        out.write(includes_header)
        out.write(construct_soa_struct(types, types_map))
        out.write(construct_run_func(types, types_map))
        out.write(construct_main_func(types, types_map))
    