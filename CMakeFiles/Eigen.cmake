# Eigen - Template Library for Linear Algebra: Matrices, Vectors, Numerical Solvers, and related Algorithms
set(EIGEN_VERSION "3.4.0") # 3147391d946bb4b6c68edd901f2add6ac1f31f8c
FetchContent_Populate(
    Eigen
    GIT_REPOSITORY "https://gitlab.com/libeigen/eigen.git"
    GIT_TAG "${EIGEN_VERSION}"
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/eigen-${EIGEN_VERSION}"
    SYSTEM
    EXCLUDE_FROM_ALL
)
include_directories(SYSTEM "${CMAKE_CURRENT_SOURCE_DIR}/third-party/eigen-${EIGEN_VERSION}")
