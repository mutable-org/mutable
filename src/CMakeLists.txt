add_subdirectory(util)
add_subdirectory(catalog)
add_subdirectory(lex)
add_subdirectory(parse)
add_subdirectory(storage)
add_subdirectory(io)
add_subdirectory(IR)
add_subdirectory(backend)


########################################################################################################################
# libmutable
########################################################################################################################

set(
    MUTABLE_SOURCES
    mutable.cpp
    Options.cpp
    version.cpp
    $<TARGET_OBJECTS:backend>
    $<TARGET_OBJECTS:IR>
    $<TARGET_OBJECTS:io>
    $<TARGET_OBJECTS:storage>
    $<TARGET_OBJECTS:parse>
    $<TARGET_OBJECTS:lex>
    $<TARGET_OBJECTS:catalog>
    $<TARGET_OBJECTS:util>
)
add_library(${PROJECT_NAME} ${MUTABLE_SOURCES}) # STATIC or DYNAMIC, depends on BUILD_SHARED_LIBS

# V8
if(${WITH_V8})
    add_dependencies(${PROJECT_NAME} V8)
    target_link_libraries(${PROJECT_NAME} PUBLIC ${V8_LIBRARIES})
endif()

# Binaryen
if(${WITH_V8})
    add_dependencies(${PROJECT_NAME} Binaryen)
    target_link_libraries(${PROJECT_NAME} PUBLIC binaryen)
endif()

# others
target_link_libraries(${PROJECT_NAME} PUBLIC ${Boost_LIBRARIES} dl)

if(${BUILD_SHARED_LIBS})
    # When creating a SHARED library ${PROJECT_NAME} (libmutable.so), it will be stripped of unused and non-exported
    # symbols.  Then ${PROJECT_NAME} (libmutable.so) cannot be linked to our internal executables that directly access
    # non-API symbols. We therefore introduce an INTERFACE library that combines all object files under a common library
    # name ${PROJECT_NAME}_complete.  Executables must then be linked to ${PROJECT_NAME}_complete.  ${PROJECT_NAME}
    # (libmutable.so) is mainly intended for distribution to clients, where we want it to be stripped of non-exported
    # symbols.
    add_library(${PROJECT_NAME}_complete INTERFACE)
    target_sources(${PROJECT_NAME}_complete INTERFACE ${MUTABLE_SOURCES})
    # Link to the same libraries as ${PROJECT_NAME}
    target_link_libraries(${PROJECT_NAME}_complete INTERFACE $<TARGET_PROPERTY:${PROJECT_NAME},INTERFACE_LINK_LIBRARIES>)
else()
    # Specify the full name of our bundled static library.  We will need this multiple times.
    set(
        BUNDLED_FULL_NAME
        "${LIBRARY_OUTPUT_PATH}/${CMAKE_STATIC_LIBRARY_PREFIX}${PROJECT_NAME}_bundled${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )

    # Collect all static libraries to incorporate into our bundled library.
    set(static_libs)
    list(
        APPEND static_libs
        "${LIBRARY_OUTPUT_PATH}/${CMAKE_STATIC_LIBRARY_PREFIX}${PROJECT_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
        "${PROJECT_BINARY_DIR}/third-party/src/V8-build/out/obj/${CMAKE_STATIC_LIBRARY_PREFIX}v8_monolith${CMAKE_STATIC_LIBRARY_SUFFIX}"
        "${PROJECT_BINARY_DIR}/third-party/src/Binaryen-build/lib/${CMAKE_STATIC_LIBRARY_PREFIX}binaryen${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    foreach(lib_target ${Boost_LIBRARIES})
        get_target_property(lib_location ${lib_target} LOCATION)
        list(APPEND static_libs ${lib_location})
    endforeach()

    # Detect the toolchain to use for bundling.
    if (CMAKE_CXX_COMPILER_ID MATCHES "^(Clang)$")
        set(ar_tool ${CMAKE_CXX_COMPILER_AR})
        file(
            WRITE ${CMAKE_BINARY_DIR}/${PROJECT_NAME}_bundled.ar.in
            "CREATE \"${BUNDLED_FULL_NAME}\"\n"
        )
        foreach(lib ${static_libs})
            file(APPEND ${CMAKE_BINARY_DIR}/${PROJECT_NAME}_bundled.ar.in "ADDLIB \"${lib}\"\n")
        endforeach()
        file(APPEND ${CMAKE_BINARY_DIR}/${PROJECT_NAME}_bundled.ar.in "SAVE\nEND\n")

        file(GENERATE
            OUTPUT ${CMAKE_BINARY_DIR}/${PROJECT_NAME}_bundled.ar
            INPUT ${CMAKE_BINARY_DIR}/${PROJECT_NAME}_bundled.ar.in
        )

        # Invoke the archiver tool to bundle the static libraries.
        add_custom_command(
            OUTPUT "${BUNDLED_FULL_NAME}"
            COMMAND ${ar_tool} -M < "${CMAKE_BINARY_DIR}/${PROJECT_NAME}_bundled.ar"
            DEPENDS ${static_libs}
            COMMENT "Bundling ${PROJECT_NAME}"
            VERBATIM
        )
    else()
        message(FATAL_ERROR "Unknown bundle scenario!")
    endif()

    # Create a custom target to reqire building our bundled static library as a dependency.
    add_custom_target(${PROJECT_NAME}_bundled ALL DEPENDS "${BUNDLED_FULL_NAME}")

    # Ensure that we do not strip our bundled library from unused symbols.  We therefore create an INTERFACE library
    # that simply augments linking to our static bundled library by additional linker flags.
    # NOTE: Our bundled library still includes all symbols.  Symbols are only stripped when linking static libraries to
    # executables or other static libraries.
    add_library(${PROJECT_NAME}_complete INTERFACE)
    add_dependencies(${PROJECT_NAME}_complete ${PROJECT_NAME}_bundled)
    if(APPLE)
        target_link_libraries(
            ${PROJECT_NAME}_complete
            INTERFACE
            -Wl,-force_load
            "${BUNDLED_FULL_NAME}"
        )
    else()
        target_link_libraries(
            ${PROJECT_NAME}_complete
            INTERFACE
            -Wl,--whole-archive
            "${BUNDLED_FULL_NAME}"
            -Wl,--no-whole-archive
        )
    endif()
endif()


########################################################################################################################
# executables
########################################################################################################################

add_executable(lex-bin lex.cpp)
target_link_libraries(lex-bin ${PROJECT_NAME})
set_target_properties(lex-bin PROPERTIES OUTPUT_NAME lex)

add_executable(parse-bin parse.cpp)
target_link_libraries(parse-bin PRIVATE ${PROJECT_NAME})
set_target_properties(parse-bin PROPERTIES OUTPUT_NAME parse)

add_executable(check-bin check.cpp)
target_link_libraries(check-bin PRIVATE ${PROJECT_NAME}_complete)
set_target_properties(check-bin PROPERTIES OUTPUT_NAME check)

add_executable(shell shell.cpp)
add_dependencies(shell Replxx)
target_link_libraries(shell PRIVATE ${Replxx_LIBRARIES} Threads::Threads)
target_link_libraries(shell PUBLIC ${PROJECT_NAME}_complete)
set_target_properties(shell PROPERTIES ENABLE_EXPORTS on)

add_executable(train-operator-model train-operator-model.cpp)
target_link_libraries(train-operator-model ${PROJECT_NAME} Threads::Threads)
set_target_properties(train-operator-model PROPERTIES EXCLUDE_FROM_ALL ON)

add_executable(allocator_benchmark allocator_benchmark.cpp)
target_link_libraries(allocator_benchmark $<TARGET_OBJECTS:util> dl Threads::Threads)
set_target_properties(allocator_benchmark PROPERTIES EXCLUDE_FROM_ALL ON)

add_executable(cardinality_gen cardinality_gen.cpp)
target_link_libraries(cardinality_gen PUBLIC ${PROJECT_NAME}_complete)
set_target_properties(cardinality_gen PROPERTIES EXCLUDE_FROM_ALL ON)

add_executable(query_slicer query_slicer.cpp)
target_link_libraries(query_slicer ${PROJECT_NAME})
set_target_properties(query_slicer PROPERTIES EXCLUDE_FROM_ALL ON)


########################################################################################################################
### Copy used system libraries to project, to be self-contained
########################################################################################################################

# Copy required Boost libraries
if(${BUILD_SHARED_LIBS})
    foreach(lib_target ${Boost_LIBRARIES})
        get_target_property(lib_location ${lib_target} LOCATION)
        add_custom_command(
            TARGET ${PROJECT_NAME} POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy_if_different ${lib_location} "${PROJECT_BINARY_DIR}/lib"
        )
    endforeach()
endif()
