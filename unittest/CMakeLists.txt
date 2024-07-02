set(
    UNITTEST_SOURCES
    main.cpp

    # src
    OptionsTest.cpp
    sanity.cpp

    # util
    util/AdjacencyMatrixTest.cpp
    util/ADTTest.cpp
    util/AlgorithmsTest.cpp
    util/AllocatorTest.cpp
    util/ArgParserTest.cpp
    util/FnTest.cpp
    util/GridSearchTest.cpp
    util/KmeansTest.cpp
    util/LinearModelTest.cpp
    util/MemoryTest.cpp
    util/PoolTest.cpp
    util/PositionTest.cpp
    util/reader_writer_lock_test.cpp
    util/SpnTest.cpp
    util/TimerTest.cpp
    util/unsharable_shared_ptr_test.cpp

    # util/container
    util/container/RefCountingHashMapTest.cpp

    # lex
    lex/LexerTest.cpp
    lex/TokenTest.cpp

    # parse
    parse/ParserTest.cpp
    parse/SemaTest.cpp

    # IR
    IR/CNFTest.cpp
    IR/HeuristicSearchPlanEnumeratorTest.cpp
    IR/PartialPlanGeneratorTest.cpp
    IR/PlanEnumeratorTest.cpp
    IR/QueryGraphTest.cpp
    IR/TupleTest.cpp
    IR/OptimizerTest.cpp

    # catalog
    catalog/CardinalityEstimatorTest.cpp
    catalog/DatabaseCommandTest.cpp
    catalog/SchemaTest.cpp
    catalog/TableFactoryTest.cpp
    catalog/TypeTest.cpp
    catalog/YannakakisHeuristicTest.cpp

    # storage
    storage/ColumnStoreTest.cpp
    storage/IndexTest.cpp
    storage/PaxStoreTest.cpp
    storage/RowStoreTest.cpp
    storage/StoreTest.cpp
    storage/store_manipTest.cpp

    # backend
    backend/InterpreterTest.cpp
    backend/StackMachineTest.cpp

    # io
    io/DSVReaderTest.cpp

)

if(${WITH_V8})
    list(APPEND UNITTEST_SOURCES backend/WasmTestInterpreter.cpp backend/WasmTestV8.cpp)
endif()

if(CMAKE_BUILD_TYPE MATCHES Debug)
    include_directories(${PROJECT_SOURCE_DIR}/third-party/catch2/include .)
    add_executable(unittest ${UNITTEST_SOURCES})
    target_link_libraries(unittest PRIVATE ${PROJECT_NAME}_complete Threads::Threads)
endif()
