add_executable(benchmark_v8 v8.cpp v8_impl.cpp)
target_link_libraries(benchmark_v8 db ${CMAKE_THREAD_LIBS_INIT})
