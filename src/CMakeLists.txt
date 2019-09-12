add_subdirectory(util)
add_subdirectory(lex)
add_subdirectory(parse)
add_subdirectory(IR)
add_subdirectory(catalog)
add_subdirectory(storage)

add_library(db INTERFACE)
target_link_libraries(db INTERFACE storage IR parse lex catalog util)

add_executable(lex-bin lex.cpp)
target_link_libraries(lex-bin lex util)
set_target_properties(lex-bin PROPERTIES OUTPUT_NAME lex)

add_executable(parse-bin parse.cpp)
target_link_libraries(parse-bin parse lex catalog util)
set_target_properties(parse-bin PROPERTIES OUTPUT_NAME parse)

add_executable(astdot astdot.cpp)
target_link_libraries(astdot parse lex catalog util)

add_executable(check-bin check.cpp)
target_link_libraries(check-bin parse lex catalog util)
set_target_properties(check-bin PROPERTIES OUTPUT_NAME check)

add_executable(cnf cnf.cpp)
target_link_libraries(cnf IR parse lex catalog util)

add_executable(ir ir.cpp)
target_link_libraries(ir IR parse lex catalog util)

add_executable(store store.cpp)
target_link_libraries(store storage IR parse lex catalog util)

add_executable(operator operator.cpp)
target_link_libraries(operator storage IR parse lex catalog util)

add_executable(sorting sorting.cpp)
target_link_libraries(sorting storage IR parse lex catalog util)
