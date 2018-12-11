add_subdirectory(util)
add_subdirectory(lex)
add_subdirectory(parse)

add_executable(
    demo
    demo.cpp
    )
target_link_libraries(demo lex util)

add_executable(
    lex-bin
    lex.cpp
    )
target_link_libraries(lex-bin lex util)
set_target_properties(lex-bin PROPERTIES OUTPUT_NAME lex)
