# nlohmann_json - A Simple, Header-Only JSON Parser
FetchContent_Populate(
    Json
    GIT_REPOSITORY https://github.com/ArthurSonzogni/nlohmann_json_cmake_fetchcontent.git
    GIT_TAG dcfd3ee9de4e50e077c230cbf27c77aa8d760327 # v3.9.0
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/json"
)
include_directories(SYSTEM "${CMAKE_CURRENT_SOURCE_DIR}/third-party/json/include")
