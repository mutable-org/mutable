# Google depot_tools
FetchContent_Populate(
    Depot_tools
    PREFIX third-party
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party"
    GIT_REPOSITORY "https://chromium.googlesource.com/chromium/tools/depot_tools.git"
    GIT_TAG 530d86d40b2aab70e0541ea0f296388ec09f0576
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/depot_tools"
    SYSTEM
    EXCLUDE_FROM_ALL
)
find_program(DEPOT_TOOLS_FETCH fetch PATHS "${CMAKE_CURRENT_SOURCE_DIR}/third-party/depot_tools" REQUIRED)
find_program(DEPOT_TOOLS_GCLIENT gclient PATHS "${CMAKE_CURRENT_SOURCE_DIR}/third-party/depot_tools" REQUIRED)
find_program(DEPOT_TOOLS_GN gn PATHS "${CMAKE_CURRENT_SOURCE_DIR}/third-party/depot_tools" REQUIRED)
