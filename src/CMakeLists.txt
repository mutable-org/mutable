add_subdirectory(util)
add_subdirectory(lex)
#add_subdirectory(parse)

add_executable(
    demo
    demo.cpp
    )
target_link_libraries(demo lex util)
