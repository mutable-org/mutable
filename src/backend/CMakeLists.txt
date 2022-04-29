set(
    BACKEND_SOURCES
    Interpreter.cpp
    RuntimeStruct.cpp
    StackMachine.cpp
)

if(${WITH_V8})
    list(APPEND BACKEND_SOURCES WasmAlgo.cpp WasmUtil.cpp WebAssembly.cpp V8Platform.cpp)
endif()

add_library(
    backend
    OBJECT
    ${BACKEND_SOURCES}
)

if(${WITH_V8})
    add_dependencies(backend Binaryen V8)
endif()
