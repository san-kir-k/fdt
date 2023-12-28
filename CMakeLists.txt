cmake_minimum_required (VERSION 3.20)

project (fdt)

message (STATUS ${CMAKE_CXX_COMPILER})

set (CMAKE_CXX_STANDARD 20)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Arrow REQUIRED)

if (CMAKE_BUILD_TYPE EQUAL "DEBUG")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -Wall -Wextra -Werror -g -fsanitize=address -fno-omit-frame-pointer")
else ()
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcpu=apple-m1 -march=native -O3 -Wall -Wextra -Werror")
endif ()

# ------------------------------------------------------------------------------------------

macro (prepare_exec TARGET SOURCE)
    add_executable (${TARGET}
        ${SOURCE}
    )

    target_include_directories (${TARGET} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/lib/include
    )

    target_link_libraries (${TARGET} PRIVATE
        AoSlib
        Arrow::arrow_shared
    )

    # target_compile_definitions (${TARGET} PRIVATE
    #     SIMD_MEMCPY
    # )
endmacro ()

macro (prepare_wide_bench)
    execute_process (COMMAND cp -rf ${CMAKE_SOURCE_DIR}/examples/load_tests/generated_tests/ ${CMAKE_BINARY_DIR}/generated_tests)
    execute_process (COMMAND bash -c ./wide_generator.sh
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/generated_tests
    )

    set (field_nums "1;10;100;1000")
    set (filenames_t "static_n@.gen.cpp")
    foreach (filename_t IN LISTS filenames_t)
        foreach (field_num IN LISTS field_nums)
            string (REPLACE "@" "${field_num}" filename ${filename_t})

            prepare_exec (soa2aos_wide_static_${field_num}
                ${CMAKE_BINARY_DIR}/generated_tests/soa2aos/${filename}
            )
            prepare_exec (aos2soa_wide_static_${field_num}
                ${CMAKE_BINARY_DIR}/generated_tests/aos2soa/${filename}
            )

            prepare_exec (soa2aos_wide_arrow_${field_num}
                ${CMAKE_BINARY_DIR}/generated_tests/soa2aos/${filename}
            )
            prepare_exec (aos2soa_wide_arrow_${field_num}
                ${CMAKE_BINARY_DIR}/generated_tests/aos2soa/${filename}
            )
        endforeach ()
    endforeach ()
endmacro()

macro (prepare_long_bench)
    execute_process (COMMAND bash -c ./long_generator.sh
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/generated_tests
    )

    set (field_nums "10000;100000;1000000;10000000")
    set (filenames_t "static_l@.gen.cpp")
    foreach (filename_t IN LISTS filenames_t)
        foreach (field_num IN LISTS field_nums)
            string (REPLACE "@" "${field_num}" filename ${filename_t})

            prepare_exec (soa2aos_long_static_${field_num}
                ${CMAKE_BINARY_DIR}/generated_tests/soa2aos/${filename}
            )
            prepare_exec (aos2soa_long_static_${field_num}
                ${CMAKE_BINARY_DIR}/generated_tests/aos2soa/${filename}
            )

            prepare_exec (soa2aos_long_arrow_${field_num}
                ${CMAKE_BINARY_DIR}/generated_tests/soa2aos/${filename}
            )
            prepare_exec (aos2soa_long_arrow_${field_num}
                ${CMAKE_BINARY_DIR}/generated_tests/aos2soa/${filename}
            )
        endforeach ()
    endforeach ()
endmacro()

# ------------------------------------------------------------------------------------------

add_library (AoSlib OBJECT
    ${CMAKE_CURRENT_SOURCE_DIR}/lib/src/aos/aos.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lib/src/aos/string.cpp
)

target_include_directories (AoSlib PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/lib/include
)

target_link_libraries (AoSlib PRIVATE
    Arrow::arrow_shared
)

# ------------------------------------------------------------------------------------------

prepare_exec (memset
    ${CMAKE_CURRENT_SOURCE_DIR}/examples/memset/memset.cpp
)
prepare_exec (soa2aos_primitive_static
    ${CMAKE_CURRENT_SOURCE_DIR}/examples/conversion/soa2aos/primitive_static.cpp
)
prepare_exec (soa2aos_primitive_arrow
    ${CMAKE_CURRENT_SOURCE_DIR}/examples/conversion/soa2aos/primitive_arrow.cpp
)
prepare_exec (soa2aos_strings_arrow
    ${CMAKE_CURRENT_SOURCE_DIR}/examples/conversion/soa2aos/strings_arrow.cpp
)
prepare_exec (aos2soa_primitive_static
    ${CMAKE_CURRENT_SOURCE_DIR}/examples/conversion/aos2soa/primitive_static.cpp
)
prepare_exec (aos2soa_primitive_arrow
    ${CMAKE_CURRENT_SOURCE_DIR}/examples/conversion/aos2soa/primitive_arrow.cpp
)
prepare_exec (aos2soa_strings_arrow
    ${CMAKE_CURRENT_SOURCE_DIR}/examples/conversion/aos2soa/strings_arrow.cpp
)

# ------------------------------------------------------------------------------------------

prepare_wide_bench()
prepare_long_bench()

prepare_exec (soa2aos_strings_arrow_load
    ${CMAKE_CURRENT_SOURCE_DIR}/examples/load_tests/soa2aos/strings_arrow.cpp
)
prepare_exec (aos2soa_strings_arrow_load
    ${CMAKE_CURRENT_SOURCE_DIR}/examples/load_tests/aos2soa/strings_arrow.cpp
)
