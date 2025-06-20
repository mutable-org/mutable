# nlohmann_json - A Simple, Header-Only JSON Parser
set(JSON_VERSION "v3.11.2") # 67e6070f9d9a44b4dec79ebe6b591f39d2285593
FetchContent_Populate(
    Json
    GIT_REPOSITORY https://github.com/ArthurSonzogni/nlohmann_json_cmake_fetchcontent.git
    GIT_TAG ${JSON_VERSION}
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/json-${JSON_VERSION}"
    SYSTEM
    EXCLUDE_FROM_ALL
)
include_directories(SYSTEM "${CMAKE_CURRENT_SOURCE_DIR}/third-party/json-${JSON_VERSION}/single_include")
