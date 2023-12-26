set(Boost_LIBRARIES
    system
    container
)
list(TRANSFORM Boost_LIBRARIES PREPEND "Boost::" OUTPUT_VARIABLE BOOST_LINK_LIBRARIES)
set(Boost_BYPRODUCTS)
foreach(lib IN LISTS Boost_LIBRARIES)
    list(APPEND Boost_BYPRODUCTS
        "${CMAKE_BINARY_DIR}/third-party/src/Boost/stage/lib/${CMAKE_STATIC_LIBRARY_PREFIX}boost_${lib}${CMAKE_STATIC_LIBRARY_SUFFIX}"
        "${CMAKE_BINARY_DIR}/third-party/src/Boost/stage/lib/${CMAKE_SHARED_LIBRARY_PREFIX}boost_${lib}${CMAKE_SHARED_LIBRARY_SUFFIX}"
    )
endforeach()

if(is_release_build)
    set(Boost_BUILD_TYPE release)
else()
    set(Boost_BUILD_TYPE debug)
endif()
ExternalProject_Add(
    Boost
    PREFIX third-party
    URL https://github.com/boostorg/boost/releases/download/boost-1.84.0/boost-1.84.0.tar.xz
    URL_MD5 893b5203b862eb9bbd08553e24ff146a
    DOWNLOAD_EXTRACT_TIMESTAMP ON
    CONFIGURE_COMMAND ./bootstrap.sh --with-toolset=clang --libdir="${CMAKE_BINARY_DIR}/third-party/src/Boost-build" --with-libraries=all
    CONFIGURE_HANDLED_BY_BUILD true
    BUILD_IN_SOURCE ON
    BUILD_COMMAND ./b2 ${Boost_BUILD_TYPE}
    BUILD_BYPRODUCTS ${Boost_BYPRODUCTS}
    INSTALL_COMMAND ""
)
ExternalProject_Get_Property(Boost BINARY_DIR)
set(BOOST_DIR ${BINARY_DIR})
set(BOOST_LIBRARY_DIR "${BOOST_DIR}/stage/lib")

include_directories(SYSTEM "${BOOST_DIR}")
link_directories("${BOOST_LIBRARY_DIR}")

set(BOOST_STATIC_LIBRARY_PATHS)
set(BOOST_SHARED_LIBRARY_PATHS)
foreach(lib IN LISTS Boost_LIBRARIES)
    add_library("Boost::${lib}" ${LIB_TYPE} IMPORTED GLOBAL)
    set(static_library_path "${BOOST_LIBRARY_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_${lib}${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(shared_library_path "${BOOST_LIBRARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}boost_${lib}${CMAKE_SHARED_LIBRARY_SUFFIX}")
    list(APPEND BOOST_STATIC_LIBRARY_PATHS "${static_library_path}")
    list(APPEND BOOST_SHARED_LIBRARY_PATHS "${shared_library_path}")
    if (BUILD_SHARED_LIBS)
        set_target_properties("Boost::${lib}" PROPERTIES IMPORTED_LOCATION "${shared_library_path}")
    else()
        set_target_properties("Boost::${lib}" PROPERTIES IMPORTED_LOCATION "${static_library_path}")
    endif()
endforeach()
