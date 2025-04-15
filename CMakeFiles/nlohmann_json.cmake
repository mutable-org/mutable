# nlohmann_json - A Simple, Header-Only JSON Parser
set(JSON_VERSION "v3.9.0") # dcfd3ee9de4e50e077c230cbf27c77aa8d760327
FetchContent_Populate(
    Json
    GIT_REPOSITORY https://github.com/ArthurSonzogni/nlohmann_json_cmake_fetchcontent.git
    GIT_TAG ${JSON_VERSION}
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/json-${JSON_VERSION}"
    SYSTEM
    EXCLUDE_FROM_ALL
)
include_directories(SYSTEM "${CMAKE_CURRENT_SOURCE_DIR}/third-party/json-${JSON_VERSION}/include")
