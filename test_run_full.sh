#!/bin/bash
set -e

# === Improve ccache reliability and speed ===
ccache --set-config sloppiness=time_macros,include_file_ctime,include_file_mtime
ccache --set-config compression=true
ccache --set-config compression_level=6
echo "Using ccache config:"
ccache -p | grep -E 'sloppiness|compression'

LLVM_PATH=$(brew --prefix llvm@17)
BOOST_PATH=$(brew --prefix boost)
BUILD_DIR="build/debug_shared"

EXTRA_CMAKE_ARGS="$@"

# Cleanly specify actual compilers
export CC="${LLVM_PATH}/bin/clang"
export CXX="${LLVM_PATH}/bin/clang++"

# Create build dir
mkdir -p "$BUILD_DIR"

# Run CMake config
cmake -S . -B "$BUILD_DIR" \
    -G Ninja \
    -DCMAKE_C_COMPILER="$CC" \
    -DCMAKE_CXX_COMPILER="$CXX" \
    -DCMAKE_C_COMPILER_LAUNCHER=ccache \
    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -DBUILD_SHARED_LIBS=ON \
    -DWITH_V8=ON \
    -DENABLE_SANITIZERS=OFF \
    -DENABLE_SANITY_FIELDS=OFF \
    -DBOOST_ROOT="$BOOST_PATH" \
    -DUSE_LIBCXX=ON \
    $EXTRA_CMAKE_ARGS

# Build
ninja -C "$BUILD_DIR" -v

# Show ccache stats
echo
ccache --show-stats
