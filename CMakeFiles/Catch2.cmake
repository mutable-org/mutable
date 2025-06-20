# Catch2 - Unit Testing Framework
set(CATCH2_VERSION "v2.13.10") # 182c910b4b63ff587a3440e08f84f70497e49a81
FetchContent_Populate(
    Catch2
    URL "https://raw.githubusercontent.com/catchorg/Catch2/${CATCH2_VERSION}/single_include/catch2/catch.hpp"
    URL_HASH MD5=0e9367cfe53621c8669af73e34a8c556
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/catch2-${CATCH2_VERSION}/include/catch2"
    DOWNLOAD_NO_EXTRACT TRUE
    SYSTEM
    EXCLUDE_FROM_ALL
)
if(CMAKE_BUILD_TYPE MATCHES Debug)
    include_directories(SYSTEM "${CMAKE_CURRENT_SOURCE_DIR}/third-party/catch2-${CATCH2_VERSION}/include")
endif()
