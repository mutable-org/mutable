include_directories(.)

add_executable(
    unittest
    main.cpp

    # util
    util/FnTest.cpp
    util/PoolTest.cpp
    util/PositionTest.cpp
    util/StringPoolTest.cpp

    # lex
    lex/TokenTest.cpp

    # parse
    parse/ParserTest.cpp
    parse/SemaTest.cpp

    # IR
    IR/CNFTest.cpp

    # catalog
    catalog/SchemaTest.cpp
    catalog/TypeTest.cpp

    # storage
    storage/RowStoreTest.cpp
    )
target_link_libraries(unittest db)
