add_subdirectory(util)
add_subdirectory(catalog)
add_subdirectory(lex)
add_subdirectory(parse)
add_subdirectory(storage)
add_subdirectory(io)
add_subdirectory(IR)
add_subdirectory(backend)

add_library(db INTERFACE)
target_link_libraries(db INTERFACE backend IR io storage parse lex catalog util)

add_executable(lex-bin lex.cpp)
target_link_libraries(lex-bin lex util)
set_target_properties(lex-bin PROPERTIES OUTPUT_NAME lex)

add_executable(parse-bin parse.cpp)
target_link_libraries(parse-bin parse lex catalog util)
set_target_properties(parse-bin PROPERTIES OUTPUT_NAME parse)

add_executable(check-bin check.cpp)
target_link_libraries(check-bin parse lex catalog util)
set_target_properties(check-bin PROPERTIES OUTPUT_NAME check)

add_executable(shell shell.cpp)
target_link_libraries(shell ${CMAKE_THREAD_LIBS_INIT} replxx db)

add_executable(wasm wasm.cpp)
target_link_libraries(wasm ${CMAKE_THREAD_LIBS_INIT} db)
