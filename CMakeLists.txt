cmake_minimum_required(VERSION 3.24)

project(gatidb LANGUAGES CXX)

add_library(gatidb
    src/btree.cpp
)

target_include_directories(gatidb
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)

target_compile_features(gatidb PUBLIC cxx_std_20)

target_compile_options(gatidb PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -Wconversion
    -Wshadow
)

enable_testing()

add_executable(gatidb_tests
    tests/btree_tests.cpp
)

target_link_libraries(gatidb_tests PRIVATE gatidb)

add_test(NAME gatidb_tests COMMAND gatidb_tests)
