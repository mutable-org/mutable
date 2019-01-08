add_subdirectory(util)
add_subdirectory(lex)
add_subdirectory(parse)
add_subdirectory(catalog)

add_library(db INTERFACE)
target_link_libraries(db INTERFACE parse lex catalog util)

add_executable(lex-bin lex.cpp)
target_link_libraries(lex-bin lex util)
set_target_properties(lex-bin PROPERTIES OUTPUT_NAME lex)

add_executable(parse-bin parse.cpp)
target_link_libraries(parse-bin parse lex catalog util)
set_target_properties(parse-bin PROPERTIES OUTPUT_NAME parse)

add_executable(check-bin check.cpp)
target_link_libraries(check-bin parse lex catalog util)
set_target_properties(check-bin PROPERTIES OUTPUT_NAME check)
