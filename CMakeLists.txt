cmake_minimum_required (VERSION 3.20)

project (fdt)

message (STATUS ${CMAKE_CXX_COMPILER})

set (CMAKE_CXX_STANDARD 20)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

if (CMAKE_BUILD_TYPE EQUAL "DEBUG")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -Wall -Wextra -Werror -g -fsanitize=address -fno-omit-frame-pointer")
else ()
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -O3 -Wall -Wextra -Werror")
endif ()

macro (prepare_exec TARGET SOURCE)
    add_executable (${TARGET}
        ${SOURCE}
    )

    target_include_directories (${TARGET} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/lib/include
    )
endmacro ()

prepare_exec (memset
    ${CMAKE_CURRENT_SOURCE_DIR}/examples/memset.cpp
)

prepare_exec (memcpy_fixed
    ${CMAKE_CURRENT_SOURCE_DIR}/examples/memcpy_fixed_structs.cpp
)

prepare_exec (memcpy
    ${CMAKE_CURRENT_SOURCE_DIR}/examples/memcpy.cpp
)
