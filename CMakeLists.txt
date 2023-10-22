cmake_minimum_required (VERSION 3.8)

project (fdt)

message (STATUS ${CMAKE_CXX_COMPILER})

set (CMAKE_CXX_STANDARD 20)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

if (CMAKE_BUILD_TYPE EQUAL "DEBUG")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -Wall -Wextra -Werror -g -fsanitize=address -fno-omit-frame-pointer")
else ()
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -O3 -Wall -Wextra -Werror")
endif ()

add_executable (naive
    ${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp
)
target_include_directories (naive PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)