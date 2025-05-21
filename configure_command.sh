#!/bin/bash

# cmake -S . -B build/debug_shared \
#     -G Ninja \
#     -DCMAKE_C_COMPILER=clang \
#     -DCMAKE_CXX_COMPILER=clang++ \
#     -DCMAKE_BUILD_TYPE=Debug \
#     -DBUILD_SHARED_LIBS=ON \
#     -DENABLE_SANITIZERS=ON \
#     -DENABLE_SANITY_FIELDS=ON \
#     -DWITH_V8=ON

cmake -S . -B build/debug_shared \
    --fresh \
    -G Ninja \
    -DCMAKE_C_COMPILER="$CC" \
    -DCMAKE_CXX_COMPILER="$CXX" \
    -DCMAKE_BUILD_TYPE=Debug \
    -DBUILD_SHARED_LIBS=ON \
    -DWITH_V8=ON \
    -DENABLE_SANITIZERS=ON \
    -DENABLE_SANITY_FIELDS=OFF \
    -DBOOST_ROOT=$(brew --prefix boost) \
    -DUSE_LIBCXX=ON \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON