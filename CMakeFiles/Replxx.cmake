# Replxx - Interactive Command Line Tool with History and Completions
set(REPLXX_VERSION "release-0.0.4") # 2b248467112cbbc16f63fde38230fd7c7ffc55f1
if(${BUILD_SHARED_LIBS})
    set(Replxx_LIBRARIES replxx)
else()
    set(Replxx_LIBRARIES replxx-static)
endif()
ExternalProject_Add(
    Replxx
    PREFIX third-party
    DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party"
    GIT_REPOSITORY "https://github.com/AmokHuginnsson/replxx.git"
    GIT_TAG ${REPLXX_VERSION}
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/replxx-${REPLXX_VERSION}"
    PATCH_COMMAND
        sed -i.origin "s/^.*set_property.*DEBUG_POSTFIX.*//" "CMakeLists.txt" &&
        sed -i.origin "s/^.*set_property.*RELWITHDEBINFO_POSTFIX.*//" "CMakeLists.txt" &&
        sed -i.origin "s/^.*set_property.*MINSIZEREL_POSTFIX.*//" "CMakeLists.txt" &&
        sed -i.origin "s/^if *( *NOT BUILD_SHARED_LIBS AND MSVC *)/if ( NOT BUILD_SHARED_LIBS )/" "CMakeLists.txt"
    CMAKE_ARGS -DBUILD_SHARED_LIBS=${BUILD_SHARED_LIBS} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    CONFIGURE_HANDLED_BY_BUILD true
    INSTALL_COMMAND ""
)
include_directories(SYSTEM "third-party/replxx-${REPLXX_VERSION}/include")
link_directories(${CMAKE_CURRENT_BINARY_DIR}/third-party/src/Replxx-build)
