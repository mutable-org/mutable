# Google depot_tools
set(DEPOT_TOOLS_VERSION 530d86d40b2aab70e0541ea0f296388ec09f0576)
set(DEPOT_TOOLS_PATH "${CMAKE_SOURCE_DIR}/third-party/depot_tools-${DEPOT_TOOLS_VERSION}")
FetchContent_Populate(
    Depot_tools
    PREFIX third-party
    SOURCE_DIR "${CMAKE_SOURCE_DIR}/third-party"
    GIT_REPOSITORY "https://chromium.googlesource.com/chromium/tools/depot_tools.git"
    GIT_TAG "${DEPOT_TOOLS_VERSION}"
    SOURCE_DIR "${DEPOT_TOOLS_PATH}"
    SYSTEM
    EXCLUDE_FROM_ALL
)
find_program(DEPOT_TOOLS_FETCH fetch PATHS "${DEPOT_TOOLS_PATH}" REQUIRED)
find_program(DEPOT_TOOLS_GCLIENT gclient PATHS "${DEPOT_TOOLS_PATH}" REQUIRED)
find_program(DEPOT_TOOLS_GN gn PATHS "${DEPOT_TOOLS_PATH}" REQUIRED)
