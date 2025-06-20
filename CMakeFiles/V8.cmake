# V8 - Google's JavaScript and WebAssembly Engine (used in Chrome, Node.js)

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

set(V8_BINARY_DIR "${CMAKE_BINARY_DIR}/third-party/src/V8-build")

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
    set(V8_BUILD_BYPRODUCTS "${V8_BINARY_DIR}/obj/${CMAKE_STATIC_LIBRARY_PREFIX}v8_monolith${CMAKE_STATIC_LIBRARY_SUFFIX}")
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
    DOWNLOAD_DIR "${CMAKE_SOURCE_DIR}/third-party/v8"
    SOURCE_DIR "${CMAKE_SOURCE_DIR}/third-party/v8/v8"
    DOWNLOAD_COMMAND env DEPOT_TOOLS_UPDATE=0 "${DEPOT_TOOLS_FETCH}" --force v8 || true
    COMMAND cd "${CMAKE_SOURCE_DIR}/third-party/v8/v8/" && git remote remove mutable || true
    COMMAND cd "${CMAKE_SOURCE_DIR}/third-party/v8/v8/" && git remote add mutable "https://gitlab.cs.uni-saarland.de/bigdata/mutable/v8.git" || true
    COMMAND cd "${CMAKE_SOURCE_DIR}/third-party/v8/v8/" && git fetch mutable
    COMMAND cd "${CMAKE_SOURCE_DIR}/third-party/v8/v8/" && git checkout adfc01872f43132c74bbd2182b127ad6f462f2c1
    UPDATE_COMMAND cd "${CMAKE_SOURCE_DIR}/third-party/v8/v8/" && env DEPOT_TOOLS_UPDATE=0 "${DEPOT_TOOLS_GCLIENT}" sync
    COMMAND        cd "${CMAKE_SOURCE_DIR}/third-party/v8/v8/" && env DEPOT_TOOLS_UPDATE=0 "${DEPOT_TOOLS_GCLIENT}" sync -D -f
    CONFIGURE_COMMAND ${CMAKE_COMMAND} -E make_directory "${V8_BINARY_DIR}" &&
                      cd "${CMAKE_SOURCE_DIR}/third-party/v8/v8/" &&
                      PATH=${DEPOT_TOOLS_PATH}:$ENV{PATH} third_party/depot_tools/gn gen "${V8_BINARY_DIR}" --root=${CMAKE_SOURCE_DIR}/third-party/v8/v8 --args=${V8_BUILD_ARGS}
    CONFIGURE_HANDLED_BY_BUILD true
    BUILD_BYPRODUCTS ${V8_BUILD_BYPRODUCTS}
    BUILD_COMMAND cd "${CMAKE_SOURCE_DIR}/third-party/v8/v8/" &&
                  PATH=${DEPOT_TOOLS_PATH}:$ENV{PATH} ninja -C "${V8_BINARY_DIR}" ${V8_LIBRARIES}
    INSTALL_COMMAND ""
)
include_directories(SYSTEM third-party/v8/v8/include)
if(${BUILD_SHARED_LIBS})
    link_directories(${V8_BINARY_DIR})
else()
    link_directories(${V8_BINARY_DIR}/obj)
endif()
