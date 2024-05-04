cmake_minimum_required(VERSION 3.21)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

project(
    "mutable"
    DESCRIPTION "A Modern DBMS for Research and Fast Prototyping"
    HOMEPAGE_URL "https://bigdata.uni-saarland.de/projects/mutable/"
    LANGUAGES C CXX
)

set(EXECUTABLE_OUTPUT_PATH  "${PROJECT_BINARY_DIR}/bin")
set(LIBRARY_OUTPUT_PATH     "${PROJECT_BINARY_DIR}/lib")


########################################################################################################################
### CMake Modules
########################################################################################################################
include(ExternalProject)
include(FetchContent)
include(GenerateExportHeader)
enable_testing()


########################################################################################################################
### Required / Desired Executables
########################################################################################################################

# Check whether we have access to `lld`, LLVM's linker
find_program(LLD lld NO_CACHE)
if(${LLD})
    set(HAS_LLD TRUE)
else()
    set(HAS_LLD FALSE)
endif()


########################################################################################################################
### mutable build options
########################################################################################################################

# General build options
if(APPLE)
    set(USE_LIBCXX_default TRUE)
else()
    set(USE_LIBCXX_default FALSE)
endif()

option(BUILD_SHARED_LIBS        "Build mutable as a shared library" OFF)
option(ENABLE_SANITIZERS        "Build mutable with address and UB sanitizers" ON)
option(ENABLE_THREAD_SANITIZER  "Build mutable with thread sanitizer" OFF)
option(ENABLE_SANITY_FIELDS     "Build mutable with sanity fields enabled" ON)
option(USE_LIBCXX               "Use libcxx (aka. libc++), LLVM's new implementation of the C++ standard library, instead of GNU's libstdc++" ${USE_LIBCXX_default})
option(USE_LLD                  "Use LLD, a linker from the LLVM project" ${HAS_LLD})

# Enable backends
option(WITH_V8                  "Build with the V8-based WebAssembly execution backend." ON)

if(${BUILD_SHARED_LIBS})
    set(LIB_TYPE SHARED)
else()
    set(LIB_TYPE STATIC)
endif()


########################################################################################################################
### Packages
########################################################################################################################

# Threads
set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)

# Git
find_package(Git REQUIRED)


########################################################################################################################
### Set Compiler and Linker Flags
########################################################################################################################

# Set required C standard
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED)

# Set required CXX standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(is_release_build FALSE)
else()
    set(is_release_build TRUE)
endif()

# Run debug build with sanitizers, but exclude the vptr sanitizer.  We must do this because V8 is built without RTTI,
# see https://groups.google.com/g/v8-users/c/MJztlKiWFUc/m/z3_V-SMvAwAJ.

# Set global compile options
add_compile_options(
    -W
    -Wall
    -pedantic
    -Wno-variadic-macros
    -Wno-gnu-zero-variadic-macro-arguments
    -Wno-gnu-label-as-value
    -Wno-vla-extension
    -Wno-gnu-conditional-omitted-operand
    -Wno-gnu-statement-expression-from-macro-expansion
    -mavx2
    -mbmi2
    -mavx
    -msse4.2
    -mbmi
    -fdiagnostics-color=always
)

# Configure release builds
if(${is_release_build})
    add_compile_definitions(NDEBUG) # set NDEBUG macro
    add_compile_options(-O2)
endif()

# Configure builds with debug information
if(CMAKE_BUILD_TYPE STREQUAL "Debug" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    # Do not omit frame pointers.  They are necessary for correct stack traces when debugging or profiling.
    add_compile_options(-fno-omit-frame-pointer -fno-optimize-sibling-calls)

    # OS dependent debug annotations: lldb is the default debugger on macOS, gdb is the default on *nix
    if(APPLE)
        add_compile_options(-glldb)
    else()
        add_compile_options(-ggdb3)
    endif()
endif()

# Set standard library implementation to use
if(${USE_LIBCXX})
    message("[mutable] Using LLVM's libcxx")
    add_compile_options(-stdlib=libc++)
    add_link_options(-stdlib=libc++ -lc++abi)
else()
    message("[mutable] Using GNU's libstdc++")
    add_compile_options(-stdlib=libstdc++)
    add_link_options(-stdlib=libstdc++)
endif()

# Enable position-independent code (PIC) when building shared libraries
set(CMAKE_POSITION_INDEPENDENT_CODE ${BUILD_SHARED_LIBS})

# Set symbol visibility for dynamic linking
if(${BUILD_SHARED_LIBS})
    set(CMAKE_CXX_VISIBILITY_PRESET hidden)
    set(CMAKE_VISIBILITY_INLINES_HIDDEN 1)
endif()

# Set the linker to use
if(${USE_LLD})
    message("[mutable] Using linker lld (${LLD})")
    add_link_options("-fuse-ld=lld")
endif()

if (${ENABLE_SANITIZERS} AND ${ENABLE_THREAD_SANITIZER})
    message(SEND_ERROR "ENABLE_SANITIZERS and ENABLE_THREAD_SANITIZER are mutually exclusive.  Cannot combine UBSan and ASan with ThreadSan.")
endif()

if(${ENABLE_SANITIZERS})
    message("[mutable] Compiling with address and UB sanitizers")
    add_compile_options(-fsanitize=address -fsanitize=undefined -fno-sanitize=vptr -fsanitize-link-c++-runtime)
    add_link_options(-fsanitize=address -fsanitize=undefined -fno-sanitize=vptr -fsanitize-link-c++-runtime)
elseif(${ENABLE_THREAD_SANITIZER})
    message("[mutable] Compiling with thread sanitizer")
    add_compile_options(-fsanitize=thread -fsanitize-link-c++-runtime)
    add_link_options(-fsanitize=thread -fsanitize-link-c++-runtime)
endif()

if(${ENABLE_SANITY_FIELDS})
    message("[mutable] Compiling mutable with sanity fields")
    add_compile_definitions(M_ENABLE_SANITY_FIELDS)
endif()


########################################################################################################################
### Get Git Version Information
########################################################################################################################

set(GITREV_BARE_FILE    "gitversion.tbl")
set(GITREV_BARE_TMP     "gitversion-tmp.tbl")
set(GITREV_FILE         "${CMAKE_SOURCE_DIR}/include/mutable/${GITREV_BARE_FILE}")
set(GITREV_TMP          "${CMAKE_BINARY_DIR}/${GITREV_BARE_TMP}")

# Generate a table file with the X-macro, that is included by mutable to obtain Git version information
add_custom_command(
    OUTPUT "${GITREV_FILE}"
    COMMAND ${CMAKE_COMMAND} -E echo_append "X(GIT_REV, "                           >  "${GITREV_TMP}"
    COMMAND ${GIT_EXECUTABLE} rev-parse HEAD | tr -d "\\n"                          >> "${GITREV_TMP}"
    COMMAND ${CMAKE_COMMAND} -E echo ")"                                            >> "${GITREV_TMP}"
    COMMAND ${CMAKE_COMMAND} -E echo_append "X(GIT_BRANCH, "                        >> "${GITREV_TMP}"
    COMMAND ${GIT_EXECUTABLE} rev-parse --abbrev-ref HEAD | tr -d "\\n"             >> "${GITREV_TMP}"
    COMMAND ${CMAKE_COMMAND} -E echo ")"                                            >> "${GITREV_TMP}"
    COMMAND ${CMAKE_COMMAND} -E echo_append "X(SEM_VERSION, "                       >> "${GITREV_TMP}"
    COMMAND ${GIT_EXECUTABLE} describe --tags --match "v*" --abbrev=0 | tr -d "\\n" >> "${GITREV_TMP}"
    COMMAND ${CMAKE_COMMAND} -E echo ")"                                            >> "${GITREV_TMP}"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different "${GITREV_TMP}" "${GITREV_FILE}"
    COMMAND ${CMAKE_COMMAND} -E remove "${GITREV_TMP}"
    DEPENDS .git/HEAD .git/index
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
    VERBATIM
)
add_custom_target(gitversion ALL DEPENDS "${GITREV_FILE}")
# Apparently, CMake's dependency scanner does not recognize the dependency to the GITREV_FILE.  We add it manually.
set_source_files_properties(
    "${CMAKE_SOURCE_DIR}/include/mutable/version.hpp"
    PROPERTIES OBJECT_DEPENDS "${GITREV_FILE}"
)


########################################################################################################################
### Third-Parties
########################################################################################################################

# Fetch contents without building.
include("CMakeFiles/Catch2.cmake")
include("CMakeFiles/Eigen.cmake")
include("CMakeFiles/nlohmann_json.cmake")
if (WITH_V8)
    include("CMakeFiles/depot_tools.cmake")
endif()

# Include all required external projects.
include("CMakeFiles/Boost.cmake")
include("CMakeFiles/Replxx.cmake")
if (WITH_V8)
    include("CMakeFiles/Binaryen.cmake")
    include("CMakeFiles/V8.cmake")
endif()


########################################################################################################################
### Project Configuration
########################################################################################################################

# Include directories
include_directories(include src)
if(APPLE)
    include_directories(SYSTEM "/usr/local/include") # Fix macOS system includes
endif()

# Link directories
# Add our LIBRARY_OUTPUT_PATH to the link directories.  This is necessary, as we copy our third-party libraries to that
# path during deployment.
link_directories(${LIBRARY_OUTPUT_PATH})

# Subdirectories
add_subdirectory(src)
add_subdirectory(unittest)
# Generate EXPORT macros to export symbols in dynamic libraries
generate_export_header(
    ${PROJECT_NAME}
    BASE_NAME "M"
    EXPORT_FILE_NAME "${CMAKE_CURRENT_SOURCE_DIR}/include/mutable/mutable-config.hpp"
)


########################################################################################################################
### Testing
########################################################################################################################

# Set environment variables for test runs
if(APPLE)
    set(
        env
        "UBSAN_OPTIONS=print_stacktrace=1"
        "ASAN_OPTIONS=detect_stack_use_after_return=1:detect_container_overflow=0"
        "MallocNanoZone=0"
    )
else()
    set(
        env
        "UBSAN_OPTIONS=print_stacktrace=1"
        "ASAN_OPTIONS=detect_stack_use_after_return=1"
    )
endif()

if(CMAKE_BUILD_TYPE MATCHES Debug)

    add_custom_target(
        pipenv-sync
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
        VERBATIM USES_TERMINAL
        COMMAND pipenv sync
    )

    # Unit test target
    add_custom_target(
        check-unit
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
        DEPENDS unittest pipenv-sync
        VERBATIM USES_TERMINAL
        COMMAND echo "##################"
        COMMAND echo "### Unit Tests ###"
        COMMAND echo "##################"
        COMMAND env ${env} pipenv run "${CMAKE_CURRENT_SOURCE_DIR}/utils/unittest-parallel.py" $<TARGET_FILE:unittest>
    )

    # Integration test target
    add_custom_target(
        check-integration
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
        DEPENDS lex-bin parse-bin check-bin shell pipenv-sync
        VERBATIM USES_TERMINAL
        COMMAND echo "#########################"
        COMMAND echo "### Integration Tests ###"
        COMMAND echo "#########################"
        COMMAND env ${env} pipenv run "${CMAKE_CURRENT_SOURCE_DIR}/test/IntegrationTest.py" -r --builddir "${PROJECT_BINARY_DIR}"
    )

    # Target `check` runs unit and integration tests, in that order
    add_custom_target(
        check
        VERBATIM USES_TERMINAL
        COMMAND ${CMAKE_BUILD_TOOL} check-unit
        COMMAND ${CMAKE_BUILD_TOOL} check-integration
    )
endif()
