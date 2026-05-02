cmake_minimum_required(VERSION 3.20)

project(gatidb VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(gatidb_core
    src/gatidb.cpp
)
target_include_directories(gatidb_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(gatidb_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

add_executable(gatidb
    src/main.cpp
)
target_link_libraries(gatidb PRIVATE gatidb_core)

enable_testing()

add_executable(gatidb_tests
    tests/test_gatidb.cpp
)
target_link_libraries(gatidb_tests PRIVATE gatidb_core)

add_test(NAME gatidb_tests COMMAND gatidb_tests)
