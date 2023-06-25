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

option(BUILD_SHARED_LIBS    "Build mutable as a shared library" OFF)
option(ENABLE_SANITIZERS    "Build mutable with address and UB sanitizers" ON)
option(ENABLE_SANITY_FIELDS "Build mutable with sanity fields enabled" ON)
option(USE_LIBCXX           "Use libcxx (aka. libc++), LLVM's new implementation of the C++ standard library, instead of GNU's libstdc++" ${USE_LIBCXX_default})
option(USE_LLD              "Use LLD, a linker from the LLVM project" ${HAS_LLD})

# Enable backends
option(WITH_V8              "Build with the V8-based WebAssembly execution backend." ON)

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

# Boost
if(${BUILD_SHARED_LIBS})
    set(Boost_USE_STATIC_LIBS OFF)
else()
    set(Boost_USE_STATIC_LIBS ON)
endif()
set(Boost_USE_MULTITHREADED ON)
find_package(Boost REQUIRED COMPONENTS system container)

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

if(${ENABLE_SANITIZERS})
    message("[mutable] Compiling with address and UB sanitizers")
    add_compile_options(-fsanitize=address -fsanitize=undefined -fno-sanitize=vptr)
    add_link_options(-fsanitize=address -fsanitize=undefined -fno-sanitize=vptr)
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
    "${CMAKE_SOURCE_DIR}/include/mutable/version.cpp"
    PROPERTIES OBJECT_DEPENDS "${GITREV_FILE}"
)


########################################################################################################################
### Third-Parties
########################################################################################################################

# Catch2 - Unit Testing Framework
FetchContent_Populate(
    Catch2
    URL "https://raw.githubusercontent.com/catchorg/Catch2/v2.13.7/single_include/catch2/catch.hpp"
    URL_HASH MD5=afc1a3a08d753ec1bef9a449ff418e07
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/catch2/include/catch2"
    DOWNLOAD_NO_EXTRACT TRUE
)

# Replxx - Interactive Command Line Tool with History and Completions
if(${BUILD_SHARED_LIBS})
    set(Replxx_LIBRARIES replxx)
else()
    set(Replxx_LIBRARIES replxx-static)
endif()
ExternalProject_Add(
    Replxx
    PREFIX third-party
    DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party"
    GIT_REPOSITORY "https://github.com/AmokHuginnsson/replxx.git"
    GIT_TAG 737c8e8147d44eae530e2b56592ad8788695ef12
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/replxx"
    PATCH_COMMAND sed -i.origin "s/^.*set_property.*DEBUG_POSTFIX.*//" "CMakeLists.txt"
    COMMAND       sed -i.origin "s/^if *( *NOT BUILD_SHARED_LIBS AND MSVC *)/if ( NOT BUILD_SHARED_LIBS )/" "CMakeLists.txt"
    CMAKE_ARGS -DBUILD_SHARED_LIBS=${BUILD_SHARED_LIBS} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    CONFIGURE_HANDLED_BY_BUILD true
    INSTALL_COMMAND ""
)
include_directories(SYSTEM third-party/replxx/include)
link_directories(${CMAKE_CURRENT_BINARY_DIR}/third-party/src/Replxx-build)

# Binaryen - WebAssembly CodeGen Framework with WASM-specific Optimizations
if(${BUILD_SHARED_LIBS})
    set(BINARYEN_build_static OFF)
else()
    set(BINARYEN_build_static ON)
endif()
if(${is_release_build})
    set(BINARYEN_enable_assertions OFF)
else()
    set(BINARYEN_enable_assertions ON)
endif()
ExternalProject_Add(
    Binaryen
    PREFIX third-party
    DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party"
    GIT_REPOSITORY "https://github.com/WebAssembly/binaryen.git"
    GIT_TAG 728b37cbe95ca8ea8cfba9ebc70e3fcb14db273a # version_112
    GIT_SUBMODULES # fetch/update submodules of Binaryen
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/binaryen"
    CMAKE_ARGS -DBUILD_STATIC_LIB=${BINARYEN_build_static} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER} -DBYN_ENABLE_ASSERTIONS=${BINARYEN_enable_assertions} -DBUILD_TESTS=OFF -DBUILD_TOOLS=OFF -DENABLE_WERROR=OFF
    CONFIGURE_HANDLED_BY_BUILD true
    BUILD_BYPRODUCTS "${PROJECT_BINARY_DIR}/third-party/src/Binaryen-build/lib/${CMAKE_STATIC_LIBRARY_PREFIX}binaryen${CMAKE_STATIC_LIBRARY_SUFFIX}"
    BUILD_COMMAND ${CMAKE_BUILD_TOOL} binaryen
    INSTALL_COMMAND ""
)
include_directories(SYSTEM third-party/binaryen/src)
link_directories(${PROJECT_BINARY_DIR}/third-party/src/Binaryen-build/lib)

# nlohmann_json - A Simple, Header-Only JSON Parser
FetchContent_Populate(
    Json
    GIT_REPOSITORY https://github.com/ArthurSonzogni/nlohmann_json_cmake_fetchcontent.git
    GIT_TAG dcfd3ee9de4e50e077c230cbf27c77aa8d760327 # v3.9.0
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/json"
)
include_directories(SYSTEM "${CMAKE_CURRENT_SOURCE_DIR}/third-party/json/include")

# Eigen - Template Library for Linear Algebra: Matrices, Vectors, Numerical Solvers, and related Algorithms
FetchContent_Populate(
    Eigen
    GIT_REPOSITORY "https://gitlab.com/libeigen/eigen.git"
    GIT_TAG 3147391d946bb4b6c68edd901f2add6ac1f31f8c # v3.4.0
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/eigen"
)
include_directories(SYSTEM "${CMAKE_CURRENT_SOURCE_DIR}/third-party/eigen")

# V8 - Google's JavaScript and WebAssembly Engine (used in Chrome, Node.js)
if(${WITH_V8})
    # Determine target CPU architecture (supporting x64 or arm64)
    # more info: "gn help target_cpu"
    execute_process(
        COMMAND uname -m
        OUTPUT_VARIABLE CPU_ARCH
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )

    if(NOT "${CPU_ARCH}" STREQUAL "arm64")
        set(CPU_ARCH "x64")
    endif()

    set(V8_VERSION "11.1.276")
    set(V8_BUILD_ARGS "is_clang=true v8_enable_sandbox=false v8_enable_pointer_compression=false v8_enable_webassembly=true treat_warnings_as_errors=false use_sysroot=false use_glib=false use_custom_libcxx=false v8_use_external_startup_data=false use_locally_built_instrumented_libraries=false clang_use_chrome_plugins=false target_cpu=\"${CPU_ARCH}\"")
    if(APPLE)
        # Compute Clang's base path, so we can propagate it to V8
        find_program(CLANG_PATH ${CMAKE_CXX_COMPILER})
        get_filename_component(CLANG_DIR ${CLANG_PATH} DIRECTORY)
        get_filename_component(CLANG_BASE_PATH "${CLANG_DIR}/.." ABSOLUTE)
        message("[V8] Setting Clang base path to ${CLANG_BASE_PATH}")
        set(V8_BUILD_ARGS "${V8_BUILD_ARGS} clang_base_path=\"${CLANG_BASE_PATH}\"")
    endif()
    if(${BUILD_SHARED_LIBS})
        set(V8_BUILD_ARGS "${V8_BUILD_ARGS} v8_monolithic=false is_component_build=true")
        set(V8_LIBRARIES v8 v8_libplatform)
    else()
        set(V8_BUILD_ARGS "${V8_BUILD_ARGS} v8_monolithic=true is_component_build=false")
        set(V8_LIBRARIES v8_monolith)
    endif()
    if(${USE_LLD})
        set(V8_BUILD_ARGS "${V8_BUILD_ARGS} use_lld=true")
    else()
        set(V8_BUILD_ARGS "${V8_BUILD_ARGS} use_lld=false")
    endif()
    if(${is_release_build})
        message(STATUS "Building V8 in release mode")
        set(V8_BUILD_ARGS "${V8_BUILD_ARGS} is_debug=false symbol_level=0 v8_enable_disassembler=true")
    else()
        message(STATUS "Building V8 in debug mode")
        set(V8_BUILD_ARGS "${V8_BUILD_ARGS} is_debug=true use_debug_fission=true symbol_level=2")
    endif()
    ExternalProject_Add(
        V8
        PREFIX third-party
        DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8"
        SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8/v8"
        DOWNLOAD_COMMAND fetch --force v8 || true
        COMMAND cd "${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8/v8/" && git remote remove mutable || true
        COMMAND cd "${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8/v8/" && git remote add mutable https://gitlab.cs.uni-saarland.de/bigdata/mutable/v8.git || true
        COMMAND cd "${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8/v8/" && git fetch mutable
        COMMAND cd "${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8/v8/" && git checkout adfc01872f43132c74bbd2182b127ad6f462f2c1
        UPDATE_COMMAND cd "${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8/v8/" && gclient sync
        COMMAND        cd "${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8/v8/" && gclient sync -D -f
        CONFIGURE_COMMAND gn gen out --root=${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8/v8 --args=${V8_BUILD_ARGS}
        CONFIGURE_HANDLED_BY_BUILD true
        BUILD_BYPRODUCTS "${PROJECT_BINARY_DIR}/third-party/src/V8-build/out/obj/${CMAKE_STATIC_LIBRARY_PREFIX}v8_monolith${CMAKE_STATIC_LIBRARY_SUFFIX}"
        BUILD_COMMAND ninja -C out ${V8_LIBRARIES}
        INSTALL_COMMAND ""
    )
    include_directories(SYSTEM third-party/v8/v8/include)
    if(${BUILD_SHARED_LIBS})
        link_directories(${PROJECT_BINARY_DIR}/third-party/src/V8-build/out)
    else()
        link_directories(${PROJECT_BINARY_DIR}/third-party/src/V8-build/out/obj)
    endif()
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
        COMMAND env ${env} pipenv run "${CMAKE_CURRENT_SOURCE_DIR}/test/IntegrationTest.py" -r "${PROJECT_BINARY_DIR}"
    )

    # Target `check` runs unit and integration tests, in that order
    add_custom_target(
        check
        VERBATIM USES_TERMINAL
        COMMAND ${CMAKE_BUILD_TOOL} check-unit
        COMMAND ${CMAKE_BUILD_TOOL} check-integration
    )
endif()
