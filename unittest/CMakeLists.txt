if (CMAKE_BUILD_TYPE MATCHES Debug)
include_directories(.)

add_executable(
    unittest
    main.cpp

    # src
    GlobalsTest.cpp

    # util
    util/ADTTest.cpp
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
    IR/QueryGraphTest.cpp
    IR/CNFTest.cpp
    IR/PlanEnumeratorTest.cpp
    IR/TupleTest.cpp

    # catalog
    catalog/SchemaTest.cpp
    catalog/TypeTest.cpp

    # storage
    storage/ColumnStoreTest.cpp
    storage/RowStoreTest.cpp

    # backend
    backend/StackMachineTest.cpp

    # globals
    ../src/globals.cpp
    )
target_link_libraries(unittest db)
endif()
