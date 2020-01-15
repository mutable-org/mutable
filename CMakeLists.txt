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

########################################################################################################################
# Third-Party
########################################################################################################################

# Replxx - Interactive Command Line Tool with History and Completions
ExternalProject_Add(
    Replxx
    PREFIX third-party
    DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party"
    GIT_REPOSITORY "https://github.com/AmokHuginnsson/replxx.git"
    GIT_TAG 130c57ed96a0c14c8ce7330c5ec23338c17701d9
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/replxx"
    PATCH_COMMAND sed -i.origin "s/ replxx-d / replxx /g" "CMakeLists.txt"
    CMAKE_ARGS -DBUILD_STATIC_LIBS=ON -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    BUILD_BYPRODUCTS "${CMAKE_CURRENT_BINARY_DIR}/third-party/src/Replxx-build/libreplxx.a"
    INSTALL_COMMAND ":"
    )
include_directories(SYSTEM third-party/replxx/include)
add_library(replxx STATIC IMPORTED)
set_target_properties(replxx PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/third-party/src/Replxx-build/libreplxx.a")
add_dependencies(replxx Replxx)

# Binaryen - WebAssembly CodeGen API with WASM-specific Optimizations
ExternalProject_Add(
    Binaryen
    PREFIX third-party
    DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party"
    GIT_REPOSITORY "https://github.com/WebAssembly/binaryen.git"
    GIT_TAG 132d81eca7f9b07520ce17f69ea348bfb4dbc8e8
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/binaryen"
    CMAKE_ARGS -DBUILD_STATIC_LIB=ON -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    BUILD_BYPRODUCTS "${PROJECT_BINARY_DIR}/third-party/src/Binaryen-build/lib/libbinaryen.a"
    BUILD_COMMAND ${CMAKE_BUILD_TOOL} binaryen
    INSTALL_COMMAND ":"
    )
include_directories(SYSTEM third-party/binaryen/src)
add_library(binaryen STATIC IMPORTED)
set_target_properties(binaryen PROPERTIES IMPORTED_LOCATION "${PROJECT_BINARY_DIR}/third-party/src/Binaryen-build/lib/libbinaryen.a")
add_dependencies(binaryen Binaryen)

# V8 - Google's JavaScript and WebAssembly Engine (used in Chrome, Node.js)
set(V8_VERSION "8.0.368")
set(V8_BUILD_ARGS "is_clang=true is_component_build=false v8_monolithic=true is_debug=true is_official_build=false treat_warnings_as_errors=false v8_use_external_startup_data=false use_sysroot=false use_custom_libcxx=false linux_use_bundled_binutils=false clang_use_chrome_plugins=false use_debug_fission=true")
ExternalProject_Add(
    V8
    PREFIX third-party
    DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8"
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8/v8"
    DOWNLOAD_COMMAND fetch --force v8 && ${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8/v8/third_party/depot_tools/gclient sync --revision ${V8_VERSION}
    UPDATE_COMMAND ":"
    CONFIGURE_COMMAND gn gen . --root=${CMAKE_CURRENT_SOURCE_DIR}/third-party/v8/v8 --args=${V8_BUILD_ARGS}
    BUILD_COMMAND ninja
    BUILD_BYPRODUCTS "${PROJECT_BINARY_DIR}/third-party/src/V8-build/obj/libv8_monolith.a"
    INSTALL_COMMAND ":"
    )
include_directories(SYSTEM third-party/v8/v8/include)
add_library(v8 STATIC IMPORTED)
set_target_properties(v8 PROPERTIES IMPORTED_LOCATION "${PROJECT_BINARY_DIR}/third-party/src/V8-build/obj/libv8_monolith.a")

# SpiderMonkey - Mozilla's JavaScript and WebAssembly Engine (used in Firefox)
if (APPLE)
    set(AUTOCONF_BINARY autoconf213)
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
    set(SM_DEPENDS MacOSX10.11.sdk)
    set(SM_BUILD_ARGS --with-macos-sdk=${CMAKE_CURRENT_SOURCE_DIR}/third-party/MacOSX10.11.sdk)
else()
    set(AUTOCONF_BINARY autoconf-2.13)
endif()
ExternalProject_Add(
    SpiderMonkey
    PREFIX third-party
    DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party"
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/SpiderMonkey"
    URL "https://codeload.github.com/mozilla/gecko-dev/zip/1808f51e9f84c6dc4a24c57a9757a6ee4d69692d"
    URL_HASH MD5=da25d97009f8d390e58fe3acdb2ee0c8
    DOWNLOAD_NAME "SpiderMonkey.zip"
    DEPENDS ${SM_DEPENDS}
    CONFIGURE_COMMAND cd "${CMAKE_CURRENT_SOURCE_DIR}/third-party/SpiderMonkey/js/src"
    COMMAND ${AUTOCONF_BINARY}
    COMMAND cd "${PROJECT_BINARY_DIR}/third-party/src/SpiderMonkey-build/"
    COMMAND "CC=${CMAKE_C_COMPILER}" "CXX=${CMAKE_CXX_COMPILER}" "${CMAKE_CURRENT_SOURCE_DIR}/third-party/SpiderMonkey/js/src/configure" --prefix="${PROJECT_BINARY_DIR}/third-party/src/SpiderMonkey-install" --enable-debug --disable-optimize --disable-tests ${SM_BUILD_ARGS}
    BUILD_COMMAND make -j ${CMAKE_BUILD_PARALLEL_LEVEL}
    BUILD_BYPRODUCTS "${PROJECT_BINARY_DIR}/third-party/src/SpiderMonkey-build/js/src/build/libjs_static.a"
    INSTALL_COMMAND ":"
    )
include_directories(SYSTEM third-party/SpiderMonkey/js/public)
add_library(spidermonkey STATIC IMPORTED)
set_target_properties(spidermonkey PROPERTIES IMPORTED_LOCATION "${PROJECT_BINARY_DIR}/third-party/src/SpiderMonkey-build/js/src/build/libjs_static.a")
add_dependencies(spidermonkey SpiderMonkey)

# Directories
include_directories(src)
add_subdirectory(src)
add_subdirectory(unittest)
