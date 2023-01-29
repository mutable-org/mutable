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

add_library(
    ${PROJECT_NAME}
    $<TARGET_OBJECTS:backend>
    $<TARGET_OBJECTS:IR>
    $<TARGET_OBJECTS:io>
    $<TARGET_OBJECTS:storage>
    $<TARGET_OBJECTS:parse>
    $<TARGET_OBJECTS:lex>
    $<TARGET_OBJECTS:catalog>
    $<TARGET_OBJECTS:util>
    mutable.cpp
    Options.cpp
    version.cpp
)

# When creating a STATIC library, ensure that we don't strip unused symbols.  We need them because plugins register
# themselves with the `Catalog` via `__attribute__((constructor))` et al
if(NOT ${BUILD_SHARED_LIBS})
    if(APPLE)
        target_link_options(
            ${PROJECT_NAME}
            INTERFACE
            -Wl,-force_load $<TARGET_LINKER_FILE:${PROJECT_NAME}> -Wl,-noall_load
        )
    else()
        target_link_options(
            ${PROJECT_NAME}
            INTERFACE
            -Wl,--whole-archive $<TARGET_LINKER_FILE:${PROJECT_NAME}> -Wl,--no-whole-archive
        )
    endif()
endif()

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


########################################################################################################################
# executables
########################################################################################################################

add_executable(lex-bin lex.cpp)
target_link_libraries(lex-bin ${PROJECT_NAME})
set_target_properties(lex-bin PROPERTIES OUTPUT_NAME lex)

add_executable(parse-bin parse.cpp)
target_link_libraries(parse-bin ${PROJECT_NAME})
set_target_properties(parse-bin PROPERTIES OUTPUT_NAME parse)

add_executable(check-bin check.cpp)
target_link_libraries(check-bin ${PROJECT_NAME})
set_target_properties(check-bin PROPERTIES OUTPUT_NAME check)

add_executable(shell shell.cpp)
add_dependencies(shell Replxx)
target_link_libraries(shell ${Replxx_LIBRARIES} ${PROJECT_NAME} Threads::Threads)

add_executable(train-operator-model train-operator-model.cpp)
target_link_libraries(train-operator-model ${PROJECT_NAME} Threads::Threads)

add_executable(allocator_benchmark allocator_benchmark.cpp)
target_link_libraries(allocator_benchmark $<TARGET_OBJECTS:util> dl Threads::Threads)

add_executable(cardinality_gen cardinality_gen.cpp)
target_link_libraries(cardinality_gen ${PROJECT_NAME})

add_executable(query_slicer query_slicer.cpp)
target_link_libraries(query_slicer ${PROJECT_NAME})


########################################################################################################################
### Copy used system libraries to project, to be self-contained
########################################################################################################################

# Copy required Boost libraries
if(${BUILD_SHARED_LIBS})
    foreach(lib_target ${Boost_LIBRARIES})
        get_target_property(lib_location ${lib_target} LOCATION)
        add_custom_command(
            TARGET ${PROJECT_NAME} POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy_if_different ${lib_location} "${PROJECT_BINARY_DIR}/third-party"
        )
    endforeach()
endif()
