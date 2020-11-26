if (CMAKE_BUILD_TYPE MATCHES Debug)
    include_directories(.)

    add_executable(
        unittest
        main.cpp

        # src
        GlobalsTest.cpp

        # util
        util/ADTTest.cpp
        util/AlgorithmsTest.cpp
        util/FnTest.cpp
        util/MemoryTest.cpp
        util/PoolTest.cpp
        util/PositionTest.cpp
        util/StringPoolTest.cpp

        # util/container
        util/container/RefCountingHashMapTest.cpp

        # lex
        lex/LexerTest.cpp
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
        backend/InterpreterTest.cpp
        backend/StackMachineTest.cpp
    )
    target_link_libraries(unittest ${PROJECT_NAME})
endif()
