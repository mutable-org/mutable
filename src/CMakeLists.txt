add_subdirectory(util)
add_subdirectory(lex)
add_subdirectory(parse)

add_library(db INTERFACE)
target_link_libraries(db INTERFACE parse lex util)

add_executable(lex-bin lex.cpp)
target_link_libraries(lex-bin lex util)
set_target_properties(lex-bin PROPERTIES OUTPUT_NAME lex)
