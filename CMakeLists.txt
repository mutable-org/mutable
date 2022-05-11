cmake_minimum_required(VERSION 3.20)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

project("mutable" C CXX)

include(ExternalProject)
include(FetchContent)
include(GenerateExportHeader)
enable_testing()

OPTION(BUILD_SHARED_LIBS "Build mutable as a shared library" OFF)

# Backends
set(WITH_V8 FALSE CACHE BOOL "Enable the V8 platform.")
set(WITH_SPIDERMONKEY FALSE CACHE BOOL "Enable the SpiderMonkey platform.")

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

# Loading whole static library
set(CMAKE_C_LINK_LIBRARY_USING_load_archive_SUPPORTED TRUE)
if(CMAKE_C_COMPILER_ID STREQUAL "AppleClang")
    set(CMAKE_C_LINK_LIBRARY_USING_load_archive "-force_load <LIB_ITEM>")
elseif(CMAKE_C_COMPILER_ID STREQUAL "GNU"
        AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(CMAKE_C_LINK_LIBRARY_USING_load_archive "LINKER:--push-state,--whole-archive"
        "<LINK_ITEM>"
        "LINKER:--pop-state")
elseif(CMAKE_C_COMPILER_ID STREQUAL "MSVC")
    set(CMAKE_C_LINK_LIBRARY_USING_load_archive "/WHOLEARCHIVE:<LIBRARY>")
else()
    # feature not yet supported for the other environments
    set(CMAKE_C_LINK_LIBRARY_USING_load_archive_SUPPORTED FALSE)
endif()

# Git
find_package(Git REQUIRED)

# Build flags
# cmake_policy(SET CMP0063 NEW)
# set(CMAKE_CXX_VISIBILITY_PRESET hidden)
# set(CMAKE_VISIBILITY_INLINES_HIDDEN 1)
set(EXECUTABLE_OUTPUT_PATH      "${PROJECT_BINARY_DIR}/bin")
set(LIBRARY_OUTPUT_PATH         "${PROJECT_BINARY_DIR}/lib")

# Compile flags
if(APPLE)
    execute_process(COMMAND sysctl -n hw.cachelinesize
        OUTPUT_VARIABLE LEVEL1_DCACHE_LINESIZE
        OUTPUT_STRIP_TRAILING_WHITESPACE)
else()
    execute_process(COMMAND getconf LEVEL1_DCACHE_LINESIZE
        OUTPUT_VARIABLE LEVEL1_DCACHE_LINESIZE
        OUTPUT_STRIP_TRAILING_WHITESPACE)
endif()

# Fix macOS system includes
if(APPLE)
    include_directories(SYSTEM "/usr/local/include")
endif()


# Shared library suffix
if(APPLE)
    set(SHARED_LIB_SUFFIX dylib)
elseif(WINDOWS)
    set(SHARED_LIB_SUFFIX dll)
else()
    set(SHARED_LIB_SUFFIX so)
endif()

# Shared or Static library suffix & type
if(${BUILD_SHARED_LIBS})
    set(BUILD_STATIC_LIBS OFF)
    set(LIB_SUFFIX ${SHARED_LIB_SUFFIX})
    set(LIB_TYPE SHARED)
else()
    set(BUILD_STATIC_LIBS ON)
    set(LIB_SUFFIX a)
    set(LIB_TYPE STATIC)
endif()


# Deviant binaries
if(APPLE)
    set(AUTOCONF_BINARY autoconf213)
else()
    set(AUTOCONF_BINARY autoconf-2.13)
endif()

set(CMAKE_COMPILE_FLAGS             "${CMAKE_COMPILE_FLAGS} -W -Wall -pedantic -Wno-variadic-macros -Wno-gnu-zero-variadic-macro-arguments -Wno-gnu-label-as-value -Wno-c++20-designator -Wno-c++20-extensions -Wno-vla-extension -Wno-gnu-conditional-omitted-operand -mavx2 -mbmi2 -mavx -msse4.2 -mbmi")
set(CMAKE_C_FLAGS                   "${CMAKE_C_FLAGS} ${CMAKE_COMPILE_FLAGS}")
set(CMAKE_CXX_FLAGS                 "-std=c++17 ${CMAKE_CXX_FLAGS} ${CMAKE_COMPILE_FLAGS}")
set(CMAKE_CXX_FLAGS_DEBUG           "-fno-omit-frame-pointer -fno-optimize-sibling-calls")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO  "-DNDEBUG -fno-omit-frame-pointer -fno-optimize-sibling-calls -O2")
set(CMAKE_POSITION_INDEPENDENT_CODE ${BUILD_SHARED_LIBS})

# OS dependent debug annotations
if(APPLE)
    set(CMAKE_CXX_FLAGS_DEBUG           "${CMAKE_CXX_FLAGS_DEBUG} -glldb")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO  "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -glldb")
else()
    set(CMAKE_CXX_FLAGS_DEBUG           "${CMAKE_CXX_FLAGS_DEBUG} -ggdb3")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO  "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -ggdb3")
endif()


########################################################################################################################
# Get Git Version Information
########################################################################################################################

SET(GITREV_BARE_FILE    "gitversion.tbl")
SET(GITREV_BARE_TMP     "gitversion-tmp.tbl")
SET(GITREV_FILE         "${CMAKE_SOURCE_DIR}/include/mutable/${GITREV_BARE_FILE}")
SET(GITREV_TMP          "${CMAKE_BINARY_DIR}/${GITREV_BARE_TMP}")

add_custom_command(
    OUTPUT "${GITREV_FILE}"
    COMMAND ${CMAKE_COMMAND} -E echo_append "X(GIT_REV, "               >  "${GITREV_TMP}"
    COMMAND ${GIT_EXECUTABLE} rev-parse HEAD | tr -d "\\n"              >> "${GITREV_TMP}"
    COMMAND ${CMAKE_COMMAND} -E echo ")"                                >> "${GITREV_TMP}"
    COMMAND ${CMAKE_COMMAND} -E echo_append "X(GIT_BRANCH, "            >> "${GITREV_TMP}"
    COMMAND ${GIT_EXECUTABLE} rev-parse --abbrev-ref HEAD | tr -d "\\n" >> "${GITREV_TMP}"
    COMMAND ${CMAKE_COMMAND} -E echo ")"                                >> "${GITREV_TMP}"
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
# Third-Party
########################################################################################################################

# Catch2 - Unit testing
FetchContent_Populate(
    Catch2
    URL "https://raw.githubusercontent.com/catchorg/Catch2/v2.13.7/single_include/catch2/catch.hpp"
    URL_HASH MD5=afc1a3a08d753ec1bef9a449ff418e07
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/catch2/include/catch2"
    DOWNLOAD_NO_EXTRACT TRUE
)

# Replxx - Interactive Command Line Tool with History and Completions
if(${BUILD_SHARED_LIBS})
    set(REPLXX_LIB_SUFFIX .${LIB_SUFFIX})
else()
    set(REPLXX_LIB_SUFFIX -static.a)
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
    CMAKE_ARGS -DBUILD_SHARED_LIBS=${BUILD_SHARED_LIBS} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    CONFIGURE_HANDLED_BY_BUILD true
    BUILD_BYPRODUCTS "${CMAKE_CURRENT_BINARY_DIR}/third-party/src/Replxx-build/libreplxx${REPLXX_LIB_SUFFIX}"
    INSTALL_COMMAND ""
)
include_directories(SYSTEM third-party/replxx/include)
add_library(replxx ${LIB_TYPE} IMPORTED)
set_target_properties(replxx PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/third-party/src/Replxx-build/libreplxx${REPLXX_LIB_SUFFIX}")
add_dependencies(replxx Replxx)

# Binaryen - WebAssembly CodeGen API with WASM-specific Optimizations
ExternalProject_Add(
    Binaryen
    PREFIX third-party
    DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party"
    GIT_REPOSITORY "https://github.com/WebAssembly/binaryen.git"
    GIT_TAG 96f7acf09aae1ec6e8bc573dfa8f309c4f892a40 # version_102
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/binaryen"
    CMAKE_ARGS -DBUILD_STATIC_LIB=${BUILD_STATIC_LIBS} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    CONFIGURE_HANDLED_BY_BUILD true
    BUILD_BYPRODUCTS "${PROJECT_BINARY_DIR}/third-party/src/Binaryen-build/lib/libbinaryen.${LIB_SUFFIX}"
    BUILD_COMMAND ${CMAKE_BUILD_TOOL} binaryen
    INSTALL_COMMAND ""
)
include_directories(SYSTEM third-party/binaryen/src)
add_library(binaryen ${LIB_TYPE} IMPORTED)
set_target_properties(binaryen PROPERTIES IMPORTED_LOCATION "${PROJECT_BINARY_DIR}/third-party/src/Binaryen-build/lib/libbinaryen.${LIB_SUFFIX}")
add_dependencies(binaryen Binaryen)

# nlohmann_json - A simple, header-only JSON parser
FetchContent_Populate(
    Json
    GIT_REPOSITORY https://github.com/ArthurSonzogni/nlohmann_json_cmake_fetchcontent.git
    GIT_TAG dcfd3ee9de4e50e077c230cbf27c77aa8d760327 # v3.9.0
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/json"
)
include_directories(SYSTEM "${CMAKE_CURRENT_SOURCE_DIR}/third-party/json/include")

# Eigen - template library for linear algebra: matrices, vectors, numerical solvers, and related algorithms
FetchContent_Populate(
    Eigen
    GIT_REPOSITORY "https://gitlab.com/libeigen/eigen.git"
    GIT_TAG 0fd6b4f71dd85b2009ee4d1aeb296e2c11fc9d68 # v3.3.9
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/eigen"
)
include_directories(SYSTEM "${CMAKE_CURRENT_SOURCE_DIR}/third-party/eigen")

# V8 - Google's JavaScript and WebAssembly Engine (used in Chrome, Node.js)
if(${WITH_V8})
    set(V8_VERSION "9.8.178")
    set(V8_BUILD_ARGS "is_clang=true v8_enable_pointer_compression=true treat_warnings_as_errors=false v8_use_external_startup_data=false use_sysroot=false use_custom_libcxx=false clang_use_chrome_plugins=false target_cpu=\"x64\"")
    add_definitions("-DV8_COMPRESS_POINTERS")
    if(${BUILD_SHARED_LIBS})
        set(V8_BUILD_ARGS "${V8_BUILD_ARGS} v8_monolithic=false is_component_build=true")
        set(V8_LIBRARY_PATH "${PROJECT_BINARY_DIR}/third-party/src/V8-build/out/libv8.${LIB_SUFFIX}")
        set(V8_PLATFORM_PATH "${PROJECT_BINARY_DIR}/third-party/src/V8-build/out/libv8_libplatform.${LIB_SUFFIX}")
        set(V8_BUILD_TARGETS v8 v8_libplatform)
    else()
        set(V8_BUILD_ARGS "${V8_BUILD_ARGS} v8_monolithic=true is_component_build=false")
        set(V8_LIBRARY_PATH "${PROJECT_BINARY_DIR}/third-party/src/V8-build/out/obj/libv8_monolith.a")
        set(V8_PLATFORM_PATH)
        set(V8_BUILD_TARGETS v8_monolith)
    endif()
    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        message(STATUS "Building V8 in debug mode")
        set(V8_BUILD_ARGS "${V8_BUILD_ARGS} is_debug=true use_debug_fission=true")
    else()
        message(STATUS "Building V8 in release mode")
        set(V8_BUILD_ARGS "${V8_BUILD_ARGS} is_debug=false symbol_level=0 v8_enable_disassembler=true")
    endif()
    ExternalProject_Add(
        V8
        PREFIX third-party
        DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8"
        SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8/v8"
        DOWNLOAD_COMMAND fetch --force v8 || true
        COMMAND cd "${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8/v8/"
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8/v8/third_party/depot_tools/gclient sync
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8/v8/third_party/depot_tools/gclient sync -D
        COMMAND git remote | grep ^mutable$ || git remote add mutable git@gitlab.cs.uni-saarland.de:bigdata/mutable/v8.git
        COMMAND git fetch mutable
        COMMAND git checkout ff478912cd9e0f79dff0c8d930bfaf79c2b788e2
        UPDATE_COMMAND ""
        CONFIGURE_COMMAND gn gen out --root=${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8/v8 --args=${V8_BUILD_ARGS}
        CONFIGURE_HANDLED_BY_BUILD true
        BUILD_COMMAND ninja -C out ${V8_BUILD_TARGETS}
        BUILD_BYPRODUCTS ${V8_LIBRARY_PATH} ${V8_PLATFORM_PATH}
        INSTALL_COMMAND ""
    )
    # add_definitions(-DM_WITH_V8)
    include_directories(SYSTEM third-party/v8/v8/include)
    add_library(v8 ${LIB_TYPE} IMPORTED)
    set_target_properties(v8 PROPERTIES IMPORTED_LOCATION ${V8_LIBRARY_PATH})
    add_dependencies(v8 V8)
    if(${BUILD_SHARED_LIBS})
        add_library(v8_platform ${LIB_TYPE} IMPORTED)
        set_target_properties(v8_platform PROPERTIES IMPORTED_LOCATION ${V8_PLATFORM_PATH})
        add_dependencies(v8_platform V8)
    endif()
endif()

# SpiderMonkey - Mozilla's JavaScript and WebAssembly Engine (used in Firefox)
if(${WITH_SPIDERMONKEY})
    if(APPLE)
        # Apple MacOSX10.11 SDK - Required by SpiderMonkey
        ExternalProject_Add(
            MacOSX10.11.sdk
            PREFIX third-party
            DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party"
            SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/MacOSX10.11.sdk"
            URL "https://github.com/phracker/MacOSX-SDKs/releases/download/10.15/MacOSX10.11.sdk.tar.xz"
            URL_HASH MD5=79940ed366bb2d711ecca85f7acd11b5
            CONFIGURE_COMMAND ""
            BUILD_COMMAND ""
            INSTALL_COMMAND ""
        )
        set(SPIDERMONKEY_DEPENDS MacOSX10.11.sdk)
        set(SPIDERMONKEY_MACOS_SDK_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third-party/MacOSX10.11.sdk)
    endif()
    ExternalProject_Add(
        SpiderMonkey
        PREFIX third-party
        DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party"
        SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/SpiderMonkey"
        URL "https://codeload.github.com/mozilla/gecko-dev/zip/1808f51e9f84c6dc4a24c57a9757a6ee4d69692d"
        URL_HASH MD5=da25d97009f8d390e58fe3acdb2ee0c8
        DOWNLOAD_NAME "SpiderMonkey.zip"
        PATCH_COMMAND patch --quiet --backup ${CMAKE_CURRENT_SOURCE_DIR}/third-party/SpiderMonkey/js/src/devtools/automation/autospider.py ${CMAKE_CURRENT_SOURCE_DIR}/autospider_macos_sdk.patch
        DEPENDS ${SPIDERMONKEY_DEPENDS}
        CONFIGURE_COMMAND ""
        BUILD_COMMAND cd "${CMAKE_CURRENT_SOURCE_DIR}/third-party/SpiderMonkey/js/rust"
        COMMAND MACOS_SDK_DIR=${SPIDERMONKEY_MACOS_SDK_DIR} cargo build --features debugmozjs
        BUILD_BYPRODUCTS "${CMAKE_CURRENT_SOURCE_DIR}/third-party/SpiderMonkey/target/debug/build/mozjs_sys-976838596e5f086e/out/dist/bin/libmozjs-72.${SHARED_LIB_SUFFIX}"
        INSTALL_COMMAND ""
    )
    add_definitions(-DM_WITH_SPIDERMONKEY)
    include_directories(SYSTEM "${CMAKE_CURRENT_SOURCE_DIR}/third-party/SpiderMonkey/target/debug/build/mozjs_sys-976838596e5f086e/out/dist/include/")
    add_library(spidermonkey SHARED IMPORTED)
    set_target_properties(spidermonkey PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_SOURCE_DIR}/third-party/SpiderMonkey/target/debug/build/mozjs_sys-976838596e5f086e/out/dist/bin/libmozjs-72.${SHARED_LIB_SUFFIX}")
endif()

# Directories
include_directories(include src)
add_subdirectory(src)
add_subdirectory(unittest)

# generate export macros
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN 1)
generate_export_header(
    ${PROJECT_NAME}
    BASE_NAME "M"
    EXPORT_FILE_NAME "${CMAKE_CURRENT_SOURCE_DIR}/include/mutable/mutable-config.hpp"
)

# Tests
if(APPLE)
    set(ENV env UBSAN_OPTIONS=print_stacktrace=1 ASAN_OPTIONS=detect_stack_use_after_return=1:detect_container_overflow=0 MallocNanoZone=0)
else()
    set(ENV env UBSAN_OPTIONS=print_stacktrace=1 ASAN_OPTIONS=detect_stack_use_after_return=1)
endif()
add_custom_target(
    check
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    DEPENDS lex-bin parse-bin check-bin shell unittest
    VERBATIM USES_TERMINAL
    COMMAND echo "##################"
    COMMAND echo "### Unit Tests ###"
    COMMAND echo "##################"
    COMMAND ${ENV} ${EXECUTABLE_OUTPUT_PATH}/unittest "\[core\]"
    COMMAND echo "#########################"
    COMMAND echo "### Integration Tests ###"
    COMMAND echo "#########################"
    COMMAND ${ENV} "${CMAKE_CURRENT_SOURCE_DIR}/test/IntegrationTest.py" -r "${PROJECT_BINARY_DIR}"
)
