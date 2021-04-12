set(
    BACKEND_SOURCES
    Backend.cpp
    Interpreter.cpp
    RuntimeStruct.cpp
    StackMachine.cpp
)

if (${WITH_V8} OR ${WITH_SPIDERMONKEY})
    set(BACKEND_SOURCES ${BACKEND_SOURCES}
        WasmAlgo.cpp
        WasmUtil.cpp
        WebAssembly.cpp
    )
endif()

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
