# Replxx - Interactive Command Line Tool with History and Completions
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
    GIT_TAG 737c8e8147d44eae530e2b56592ad8788695ef12
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third-party/replxx"
    PATCH_COMMAND sed -i.origin "s/^.*set_property.*DEBUG_POSTFIX.*//" "CMakeLists.txt"
    COMMAND       sed -i.origin "s/^if *( *NOT BUILD_SHARED_LIBS AND MSVC *)/if ( NOT BUILD_SHARED_LIBS )/" "CMakeLists.txt"
    CMAKE_ARGS -DBUILD_SHARED_LIBS=${BUILD_SHARED_LIBS} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    CONFIGURE_HANDLED_BY_BUILD true
    INSTALL_COMMAND ""
)
include_directories(SYSTEM third-party/replxx/include)
link_directories(${CMAKE_CURRENT_BINARY_DIR}/third-party/src/Replxx-build)
