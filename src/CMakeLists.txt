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
    mutable.cpp
    globals.cpp
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
add_dependencies(${PROJECT_NAME} gitversion)
target_link_libraries(${PROJECT_NAME} PRIVATE binaryen ${Boost_LIBRARIES} dl)
if(${WITH_V8})
    target_link_libraries(${PROJECT_NAME} PRIVATE v8)
endif()
if(${WITH_SPIDERMONKEY})
    target_link_libraries(${PROJECT_NAME} PRIVATE spidermonkey)
endif()

add_library(
    frontend
    globals.cpp
    $<TARGET_OBJECTS:backend>
    $<TARGET_OBJECTS:IR>
    $<TARGET_OBJECTS:storage>
    $<TARGET_OBJECTS:parse>
    $<TARGET_OBJECTS:lex>
    $<TARGET_OBJECTS:catalog>
    $<TARGET_OBJECTS:util>
)

add_executable(lex-bin lex.cpp)
target_link_libraries(lex-bin frontend)
set_target_properties(lex-bin PROPERTIES OUTPUT_NAME lex)

add_executable(parse-bin parse.cpp)
target_link_libraries(parse-bin frontend)
set_target_properties(parse-bin PROPERTIES OUTPUT_NAME parse)

add_executable(check-bin check.cpp)
target_link_libraries(check-bin frontend)
set_target_properties(check-bin PROPERTIES OUTPUT_NAME check)

add_executable(shell shell.cpp)
add_dependencies(shell Replxx)
target_link_libraries(shell replxx ${PROJECT_NAME} Threads::Threads)

add_executable(train-operator-model train-operator-model.cpp)
target_link_libraries(train-operator-model ${PROJECT_NAME} Threads::Threads)
