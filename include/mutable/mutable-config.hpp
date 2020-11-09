#pragma once

#if defined(_WIN32) || defined(__CYGWIN__)
  #if defined(BUILD_SHARED_LIBS)
    #if defined(MUTABLE_EXPORTS)
      #define MUTABLE_API __declspec(dllexport)
    #else
      #define MUTABLE_API __declspec(dllimport)
    #endif
  #endif
#elif defined(__linux__) || defined(__APPLE__)
  #define MUTABLE_API __attribute__((visibility("default")))
#else
#error "I don't know how to export symbols for your platform..."
#endif
