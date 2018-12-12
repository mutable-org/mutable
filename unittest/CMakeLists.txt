include_directories(.)

add_executable(
    unittest
    main.cpp

    # util
    util/FnTest.cpp
    util/PositionTest.cpp
    util/StringPoolTest.cpp

    # lex
    lex/TokenTest.cpp

    # parse
    parse/ParserTest.cpp
    )
target_link_libraries(unittest lex util)
