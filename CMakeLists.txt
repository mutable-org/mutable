cmake_minimum_required(VERSION 3.2.3)

project("dbms" C CXX)

include(ExternalProject)
enable_testing()

# Threads
find_package(Threads REQUIRED)

# Build flags
SET(EXECUTABLE_OUTPUT_PATH      "${PROJECT_BINARY_DIR}/bin")
SET(LIBRARY_OUTPUT_PATH         "${PROJECT_BINARY_DIR}/lib")

# Compile flags
if (APPLE)
    execute_process(COMMAND sysctl -n hw.cachelinesize
        OUTPUT_VARIABLE LEVEL1_DCACHE_LINESIZE
        OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND sysctl -n hw.pagesize
        OUTPUT_VARIABLE PAGESIZE
        OUTPUT_STRIP_TRAILING_WHITESPACE)
else()
    execute_process(COMMAND getconf LEVEL1_DCACHE_LINESIZE
        OUTPUT_VARIABLE LEVEL1_DCACHE_LINESIZE
        OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND getconf PAGESIZE
        OUTPUT_VARIABLE PAGESIZE
        OUTPUT_STRIP_TRAILING_WHITESPACE)
endif()
SET(CMAKE_COMPILE_FLAGS         "-W -Wall -pedantic -DLEVEL1_DCACHE_LINESIZE=${LEVEL1_DCACHE_LINESIZE} -DPAGESIZE=${PAGESIZE} -march=native -ggdb -Wno-variadic-macros -Wno-gnu-zero-variadic-macro-arguments -Wno-gnu-label-as-value")
SET(CMAKE_C_FLAGS               "${CMAKE_C_FLAGS} ${CMAKE_COMPILE_FLAGS}")
SET(CMAKE_CXX_FLAGS             "-std=c++17 ${CMAKE_CXX_FLAGS} ${CMAKE_COMPILE_FLAGS}")
SET(CMAKE_CXX_FLAGS_DEBUG       "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address")

# Third-Party
ExternalProject_Add(
    Replxx
    PREFIX third-party
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/replxx"
    CMAKE_ARGS -DBUILD_STATIC_LIBS=ON -DCMAKE_BUILD_TYPE=Release
    BUILD_BYPRODUCTS "${CMAKE_CURRENT_BINARY_DIR}/third-party/src/Replxx-build/libreplxx.a"
    INSTALL_COMMAND ":"
    )
include_directories(third-party/replxx/include)
add_library(replxx STATIC IMPORTED)
set_target_properties(replxx PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/third-party/src/Replxx-build/libreplxx.a")

# Directories
include_directories(src)
add_subdirectory(src)
add_subdirectory(unittest)
