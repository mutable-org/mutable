SET(
    BACKEND_SOURCES
    Backend.cpp
    Interpreter.cpp
    StackMachine.cpp
    WasmAlgo.cpp
    WasmUtil.cpp
    WebAssembly.cpp
    )

SET(BACKEND_LIBRARIES binaryen)

if(${WITH_V8})
    SET(BACKEND_SOURCES ${BACKEND_SOURCES} V8Platform.cpp)
    SET(BACKEND_LIBRARIES ${BACKEND_LIBRARIES} v8 sockpp)
endif()

if(${WITH_SPIDERMONKEY})
    SET(BACKEND_SOURCES ${BACKEND_SOURCES} SpiderMonkeyPlatform.cpp)
    SET(BACKEND_LIBRARIES ${BACKEND_LIBRARIES} spidermonkey)
endif()

add_library(backend STATIC ${BACKEND_SOURCES})
target_link_libraries(backend ${BACKEND_LIBRARIES})
