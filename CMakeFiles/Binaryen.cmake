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
