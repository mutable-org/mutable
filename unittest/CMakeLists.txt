include_directories(.)

add_executable(
    unittest
    main.cpp

    # util
    util/FnTest.cpp
    util/MemoryTest.cpp
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
    IR/AdjacencyMatrixTest.cpp

    # catalog
    catalog/SchemaTest.cpp
    catalog/TypeTest.cpp

    # storage
    storage/ColumnStoreTest.cpp
    storage/RowStoreTest.cpp

    # backend
    backend/StackMachineTest.cpp
    )
target_link_libraries(unittest db)
