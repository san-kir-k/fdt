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

macro (prepare_exec TARGET SOURCE)
    add_executable (${TARGET}
        ${SOURCE}
    )

    target_include_directories (${TARGET} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/lib/include
    )
endmacro ()

prepare_exec (memset
    ${CMAKE_CURRENT_SOURCE_DIR}/examples/simple/memset.cpp
)
prepare_exec (soa2aos_naive_raw
    ${CMAKE_CURRENT_SOURCE_DIR}/examples/simple/soa2aos/naive_raw_transpose.cpp
)
prepare_exec (soa2aos_naive_arrow
    ${CMAKE_CURRENT_SOURCE_DIR}/examples/simple/soa2aos/naive_arrow.cpp
)

if(ARROW_LINK_SHARED)
    target_link_libraries(soa2aos_naive_arrow PRIVATE Arrow::arrow_shared)
else()
    target_link_libraries(soa2aos_naive_arrow PRIVATE Arrow::arrow_static)
endif()
