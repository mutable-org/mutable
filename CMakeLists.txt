cmake_minimum_required(VERSION 3.2.3)

project("mutable" C CXX)

include(ExternalProject)
enable_testing()

OPTION(BUILD_SHARED_LIBS "Build mutable as a shared library" OFF)
set(WITH_V8 FALSE CACHE BOOL "Enable the V8 platform.")
set(WITH_SPIDERMONKEY FALSE CACHE BOOL "Enable the SpiderMonkey platform.")

# Threads
find_package(Threads REQUIRED)

# Boost
if(${BUILD_SHARED_LIBS})
    set(Boost_USE_STATIC_LIBS OFF)
else()
    set(Boost_USE_STATIC_LIBS ON)
endif()
set(Boost_USE_MULTITHREADED ON)
find_package(Boost COMPONENTS system REQUIRED)

# Build flags
set(EXECUTABLE_OUTPUT_PATH      "${PROJECT_BINARY_DIR}/bin")
set(LIBRARY_OUTPUT_PATH         "${PROJECT_BINARY_DIR}/lib")

# Compile flags
if(APPLE)
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

# Deviant binaries
if(APPLE)
    set(AUTOCONF_BINARY autoconf213)
else()
    set(AUTOCONF_BINARY autoconf-2.13)
endif()

set(CMAKE_COMPILE_FLAGS             "-W -Wall -pedantic -DLEVEL1_DCACHE_LINESIZE=${LEVEL1_DCACHE_LINESIZE} -DPAGESIZE=${PAGESIZE} -march=native -Wno-variadic-macros -Wno-gnu-zero-variadic-macro-arguments -Wno-gnu-label-as-value")
set(CMAKE_C_FLAGS                   "${CMAKE_C_FLAGS} ${CMAKE_COMPILE_FLAGS}")
set(CMAKE_CXX_FLAGS                 "-std=c++17 ${CMAKE_CXX_FLAGS} ${CMAKE_COMPILE_FLAGS}")
set(CMAKE_CXX_FLAGS_DEBUG           "${CMAKE_CXX_FLAGS_DEBUG} -ggdb3 -fsanitize=address")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO  "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -ggdb3")
set(CMAKE_POSITION_INDEPENDENT_CODE ${BUILD_SHARED_LIBS})


########################################################################################################################
# Third-Party
########################################################################################################################

# Replxx - Interactive Command Line Tool with History and Completions
ExternalProject_Add(
    Replxx
    PREFIX third-party
    DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party"
    GIT_REPOSITORY "https://github.com/AmokHuginnsson/replxx.git"
    GIT_TAG 737c8e8147d44eae530e2b56592ad8788695ef12
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/replxx"
    PATCH_COMMAND sed -i.origin "s/^.*set_property.*DEBUG_POSTFIX.*//" "CMakeLists.txt"
    COMMAND       sed -i.origin "s/^if *( *NOT BUILD_SHARED_LIBS AND MSVC *)/if ( NOT BUILD_SHARED_LIBS )/" "CMakeLists.txt"
    CMAKE_ARGS -DBUILD_SHARED_LIBS=OFF -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    BUILD_BYPRODUCTS "${CMAKE_CURRENT_BINARY_DIR}/third-party/src/Replxx-build/libreplxx-static.a"
    INSTALL_COMMAND ""
)
include_directories(SYSTEM third-party/replxx/include)
add_library(replxx STATIC IMPORTED)
set_target_properties(replxx PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/third-party/src/Replxx-build/libreplxx-static.a")
add_dependencies(replxx Replxx)

# Binaryen - WebAssembly CodeGen API with WASM-specific Optimizations
ExternalProject_Add(
    Binaryen
    PREFIX third-party
    DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party"
    GIT_REPOSITORY "https://github.com/WebAssembly/binaryen.git"
    GIT_TAG dfb2f27d0123ad62b44914fd0210db1d21cfc91e
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/binaryen"
    CMAKE_ARGS -DBUILD_STATIC_LIB=ON -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    BUILD_BYPRODUCTS "${PROJECT_BINARY_DIR}/third-party/src/Binaryen-build/lib/libbinaryen.a"
    BUILD_COMMAND ${CMAKE_BUILD_TOOL} binaryen
    INSTALL_COMMAND ""
)
include_directories(SYSTEM third-party/binaryen/src)
add_library(binaryen STATIC IMPORTED)
set_target_properties(binaryen PROPERTIES IMPORTED_LOCATION "${PROJECT_BINARY_DIR}/third-party/src/Binaryen-build/lib/libbinaryen.a")
add_dependencies(binaryen Binaryen)

# V8 - Google's JavaScript and WebAssembly Engine (used in Chrome, Node.js)
if(${WITH_V8})
    set(V8_VERSION "8.0.368")
    set(V8_BUILD_ARGS "is_clang=true is_component_build=false v8_monolithic=true v8_enable_pointer_compression=true treat_warnings_as_errors=false v8_use_external_startup_data=false use_sysroot=false use_custom_libcxx=false clang_use_chrome_plugins=false target_cpu=\"x64\"")
    add_definitions("-DV8_COMPRESS_POINTERS")
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
        DOWNLOAD_COMMAND fetch --force v8
        COMMAND cd "${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8/v8/"
        COMMAND git remote add -f mutable git@gitlab.cs.uni-saarland.de:bigdata/mutable/v8.git || true
        COMMAND git fetch mutable
        COMMAND git checkout 6bc75c95d46842a50396f295c185622dc20b05fd
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8/v8/third_party/depot_tools/gclient sync
        UPDATE_COMMAND ""
        CONFIGURE_COMMAND gn gen . --root=${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8/v8 --args=${V8_BUILD_ARGS}
        BUILD_COMMAND ninja
        BUILD_BYPRODUCTS "${PROJECT_BINARY_DIR}/third-party/src/V8-build/obj/libv8_monolith.a"
        INSTALL_COMMAND ""
    )
    add_definitions(-DWITH_V8)
    include_directories(SYSTEM third-party/v8/v8/include)
    add_library(v8 STATIC IMPORTED)
    set_target_properties(v8 PROPERTIES IMPORTED_LOCATION "${PROJECT_BINARY_DIR}/third-party/src/V8-build/obj/libv8_monolith.a")
    add_dependencies(v8 V8)
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
    add_definitions(-DWITH_SPIDERMONKEY)
    include_directories(SYSTEM "${CMAKE_CURRENT_SOURCE_DIR}/third-party/SpiderMonkey/target/debug/build/mozjs_sys-976838596e5f086e/out/dist/include/")
    add_library(spidermonkey SHARED IMPORTED)
    set_target_properties(spidermonkey PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_SOURCE_DIR}/third-party/SpiderMonkey/target/debug/build/mozjs_sys-976838596e5f086e/out/dist/bin/libmozjs-72.${SHARED_LIB_SUFFIX}")
endif()

# Directories
include_directories(src)
add_subdirectory(src)
add_subdirectory(unittest)
