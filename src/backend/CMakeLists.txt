set(
    BACKEND_SOURCES
    Backend.cpp
    Interpreter.cpp
    StackMachine.cpp
    WasmAlgo.cpp
    WasmUtil.cpp
    WebAssembly.cpp
)

if(${WITH_V8})
    set(BACKEND_SOURCES ${BACKEND_SOURCES} V8Platform.cpp)
endif()
if(${WITH_SPIDERMONKEY})
    set(BACKEND_SOURCES ${BACKEND_SOURCES} SpiderMonkeyPlatform.cpp)
endif()

add_library(backend OBJECT ${BACKEND_SOURCES})
add_dependencies(backend Binaryen)
if(${WITH_V8})
    add_dependencies(backend V8)
endif()
if(${WITH_SPIDERMONKEY})
    add_dependencies(backend SpiderMonkey)
endif()
