add_subdirectory(util)
add_subdirectory(catalog)
add_subdirectory(lex)
add_subdirectory(parse)
add_subdirectory(storage)
add_subdirectory(io)
add_subdirectory(IR)
add_subdirectory(backend)

add_library(
    ${PROJECT_NAME}
    globals.cpp
    $<TARGET_OBJECTS:backend>
    $<TARGET_OBJECTS:IR>
    $<TARGET_OBJECTS:io>
    $<TARGET_OBJECTS:storage>
    $<TARGET_OBJECTS:parse>
    $<TARGET_OBJECTS:lex>
    $<TARGET_OBJECTS:catalog>
    $<TARGET_OBJECTS:util>
)
target_link_libraries(${PROJECT_NAME} PRIVATE binaryen ${Boost_LIBRARIES} dl)
if (${WITH_V8})
    target_link_libraries(${PROJECT_NAME} PRIVATE v8)
endif()
if(${WITH_SPIDERMONKEY})
    target_link_libraries(${PROJECT_NAME} PRIVATE spidermonkey)
endif()

add_executable(lex-bin lex.cpp globals.cpp)
target_link_libraries(lex-bin ${PROJECT_NAME})
set_target_properties(lex-bin PROPERTIES OUTPUT_NAME lex)

add_executable(parse-bin parse.cpp globals.cpp)
target_link_libraries(parse-bin ${PROJECT_NAME})
set_target_properties(parse-bin PROPERTIES OUTPUT_NAME parse)

add_executable(check-bin check.cpp globals.cpp)
target_link_libraries(check-bin ${PROJECT_NAME})
set_target_properties(check-bin PROPERTIES OUTPUT_NAME check)

add_executable(shell shell.cpp)
add_dependencies(shell Replxx)
target_link_libraries(shell ${CMAKE_THREAD_LIBS_INIT} replxx ${PROJECT_NAME})
