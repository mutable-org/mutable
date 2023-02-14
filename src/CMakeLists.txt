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
    # When creating a SHARED library ${PROJECT_NAME} (libmutable.so), it will be stripped of unused symbols.  Then
    # ${PROJECT_NAME} (libmutable.so) cannot be linked to our internal executables that directly access non-API symbols.
    # We therefore introduce an INTERFACE library that combines all object files under a common library name
    # ${PROJECT_NAME}_complete.  Executables must then be linked to ${PROJECT_NAME}_complete.  ${PROJECT_NAME}
    # (libmutable.so) is mainly intended for distribution to clients, where we want it to be stripped of non-exported
    # symbols.
    add_library(${PROJECT_NAME}_complete INTERFACE)
    target_sources(${PROJECT_NAME}_complete INTERFACE ${MUTABLE_SOURCES})
    # Link to the same libraries as ${PROJECT_NAME}
    target_link_libraries(${PROJECT_NAME}_complete INTERFACE $<TARGET_PROPERTY:${PROJECT_NAME},INTERFACE_LINK_LIBRARIES>)
else()
    # When creating a STATIC library, ensure that we don't strip unused symbols when linking ${PROJECT_NAME}
    # (libmutable.a) to executables.  We need all symbols because plugins register themselves with the `Catalog` via
    # `__attribute__((constructor))` et al.  To ensure that no symbols are stripped, we create an INTERFACE library
    # ${PROJECT_NAME}_complete that wraps platform-dependent linker arguments *around* ${PROJECT_NAME} (libmutable.a)
    # and its dependencies.  Link with ${PROJECT_NAME}_complete to get all symbols.  ${PROJECT_NAME} (libmutable.a) is
    # mainly intended for distribution to clients.
    # NOTE: ${PROJECT_NAME} (libmutable.a) still includes all symbols.  Symbols are only stripped when linking static
    # libraries to executables or other static libraries.
    add_library(${PROJECT_NAME}_complete INTERFACE)
    if(APPLE)
        target_link_libraries(
            ${PROJECT_NAME}_complete
            INTERFACE
            -Wl,-force_load
            $<TARGET_LINKER_FILE:${PROJECT_NAME}>
            $<TARGET_PROPERTY:${PROJECT_NAME},INTERFACE_LINK_LIBRARIES>
            -Wl,-noall_load
        )
    else()
        target_link_libraries(
            ${PROJECT_NAME}_complete
            INTERFACE
            -Wl,--whole-archive
            -Wl,--gc-sections
            -Wl,--gc-keep-exported
            $<TARGET_LINKER_FILE:${PROJECT_NAME}>
            $<TARGET_PROPERTY:${PROJECT_NAME},INTERFACE_LINK_LIBRARIES>
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
target_link_libraries(check-bin PRIVATE ${PROJECT_NAME})
set_target_properties(check-bin PROPERTIES OUTPUT_NAME check)

add_executable(shell shell.cpp)
add_dependencies(shell Replxx)
target_link_libraries(shell PRIVATE ${Replxx_LIBRARIES} Threads::Threads)
target_link_libraries(shell PUBLIC ${PROJECT_NAME})
set_target_properties(shell PROPERTIES ENABLE_EXPORTS on)

add_executable(train-operator-model train-operator-model.cpp)
target_link_libraries(train-operator-model ${PROJECT_NAME}_complete Threads::Threads)
set_target_properties(train-operator-model PROPERTIES EXCLUDE_FROM_ALL ON)

add_executable(allocator_benchmark allocator_benchmark.cpp)
target_link_libraries(allocator_benchmark $<TARGET_OBJECTS:util> dl Threads::Threads)
set_target_properties(allocator_benchmark PROPERTIES EXCLUDE_FROM_ALL ON)

add_executable(cardinality_gen cardinality_gen.cpp)
target_link_libraries(cardinality_gen ${PROJECT_NAME}_complete)
set_target_properties(cardinality_gen PROPERTIES EXCLUDE_FROM_ALL ON)

add_executable(query_slicer query_slicer.cpp)
target_link_libraries(query_slicer ${PROJECT_NAME}_complete)
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
