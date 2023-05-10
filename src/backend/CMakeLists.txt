set(
    BACKEND_SOURCES
    Interpreter.cpp
    StackMachine.cpp
)

if(${WITH_V8})
    list(APPEND BACKEND_SOURCES
        PhysicalOperator.cpp
        V8Engine.cpp
        WasmDSL.cpp
        WasmAlgo.cpp
        WasmOperator.cpp
        WasmUtil.cpp
        WebAssembly.cpp
    )
endif()

add_library(
    backend
    OBJECT
    ${BACKEND_SOURCES}
)

if(${WITH_V8})
    add_dependencies(backend Binaryen V8)
endif()
