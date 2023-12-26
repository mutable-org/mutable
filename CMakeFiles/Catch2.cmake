# Catch2 - Unit Testing Framework
FetchContent_Populate(
    Catch2
    URL "https://raw.githubusercontent.com/catchorg/Catch2/v2.13.10/single_include/catch2/catch.hpp"
    URL_HASH MD5=0e9367cfe53621c8669af73e34a8c556
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/catch2/include/catch2"
    DOWNLOAD_NO_EXTRACT TRUE
)
