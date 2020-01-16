#pragma once

#include <cstdint>


inline int inlined_sum(int a, int b) { return a + b; }

int c_sum(int a, int b);

struct virtual_sum
{
    virtual ~virtual_sum() { }

    virtual int operator()(int a, int b) = 0;

    static virtual_sum * Create();
};

extern uint8_t wasm_module[];
extern std::size_t wasm_module_size;
