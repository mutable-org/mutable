add_library(
    backend
    STATIC
    Interpreter.cpp
    V8Backend.cpp
    WebAssembly.cpp
    )
target_link_libraries(backend v8 binaryen)
