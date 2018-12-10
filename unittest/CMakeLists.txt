include_directories(.)

add_executable(
    unittest
    main.cpp

    # Util
    util/FnTest.cpp
    util/PositionTest.cpp
    util/StringPoolTest.cpp
    )
target_link_libraries(unittest lex util)
