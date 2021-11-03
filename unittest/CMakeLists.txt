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
        util/allocatorTest.cpp
        util/FnTest.cpp
        util/LinearModelTest.cpp
        util/MemoryTest.cpp
        util/PoolTest.cpp
        util/PositionTest.cpp
        util/StringPoolTest.cpp

        # util/container
        util/container/RefCountingHashMapTest.cpp
        util/GridSearchTest.cpp

        # lex
        lex/LexerTest.cpp
        lex/TokenTest.cpp

        # parse
        parse/ParserTest.cpp
        parse/SemaTest.cpp

        # IR
        IR/CNFTest.cpp
        IR/PlanEnumeratorTest.cpp
        IR/QueryGraphTest.cpp
        IR/TupleTest.cpp

        # catalog
        catalog/CardinalityEstimatorTest.cpp
        catalog/SchemaTest.cpp
        catalog/TypeTest.cpp

        # storage
        storage/ColumnStoreTest.cpp
        storage/PaxStoreTest.cpp
        storage/RowStoreTest.cpp

        # backend
        backend/InterpreterTest.cpp
        backend/StackMachineTest.cpp
    )
    target_link_libraries(unittest ${PROJECT_NAME} Threads::Threads)
endif()
