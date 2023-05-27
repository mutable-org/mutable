#pragma once


/*======================================================================================================================
 * Convenience macros
 *====================================================================================================================*/

#define BLOCK_OPEN(BLK) if (m::wasm::BlockUser ThisBlockUser((BLK)); (void)(ThisBlockUser), false) { } else

#define M_WASM_BLOCK_NAMED_(NAME) \
    if (m::wasm::Block NAME(#NAME, true); (void)(NAME), false) { } else BLOCK_OPEN(NAME)
#define M_WASM_BLOCK_ANON_() \
    if (m::wasm::Block ThisBlock(true); (void)(ThisBlock), false) { } else BLOCK_OPEN(ThisBlock)
#define M_GET_WASM_BLOCK_(XXX, _1, NAME, ...) NAME
#define BLOCK(...) M_GET_WASM_BLOCK_(XXX, ##__VA_ARGS__, M_WASM_BLOCK_NAMED_, M_WASM_BLOCK_ANON_)(__VA_ARGS__)

#define FUNCTION(NAME, TYPE) \
    m::wasm::FunctionProxy<TYPE> NAME(#NAME); \
    if (auto ThisFunction = NAME.make_function(); (void)(ThisFunction), false) { } else BLOCK_OPEN(ThisFunction.body())
#define PARAMETER(IDX) ThisFunction.template parameter<IDX>()
#define RETURN(RES) ThisFunction.emit_return(RES)

#define IF(COND) if (m::wasm::If ThisIf((COND)); false) { } else ThisIf.Then = [&]
#define ELSE , ThisIf.Else = [&]

#define M_WASM_LOOP_NAMED_(NAME) \
    if (m::wasm::Loop ThisLoop((NAME)); (void)(ThisLoop), false) { } else BLOCK_OPEN(ThisLoop.body())
#define M_WASM_LOOP_ANON_() M_WASM_LOOP_NAMED_("loop")
#define M_GET_WASM_LOOP_(XXX, _1, NAME, ...) NAME
#define LOOP(...) M_GET_WASM_LOOP_(XXX, ##__VA_ARGS__, M_WASM_LOOP_NAMED_, M_WASM_LOOP_ANON_)(__VA_ARGS__)

#define M_WASM_DO_WHILE_NAMED_(NAME, COND) \
    if (m::wasm::DoWhile ThisDoWhile((NAME), (COND)); (void)(ThisDoWhile), false) { } else \
        BLOCK_OPEN(ThisDoWhile.body())
#define M_WASM_DO_WHILE_ANON_(COND) M_WASM_DO_WHILE_NAMED_("do-while", COND)
#define M_GET_WASM_DO_WHILE_(XXX, _1, _2, NAME, ...) NAME
#define DO_WHILE(...) M_GET_WASM_DO_WHILE_(XXX, ##__VA_ARGS__, M_WASM_DO_WHILE_NAMED_, M_WASM_DO_WHILE_ANON_)(__VA_ARGS__)

#define M_WASM_WHILE_NAMED_(NAME, COND) \
    if (m::wasm::While ThisWhile((NAME), (COND)); (void)(ThisWhile), false) { } else BLOCK_OPEN(ThisWhile.body())
#define M_WASM_WHILE_ANON_(COND) M_WASM_WHILE_NAMED_("while", COND)
#define M_GET_WASM_WHILE_(XXX, _1, _2, NAME, ...) NAME
#define WHILE(...) M_GET_WASM_WHILE_(XXX, ##__VA_ARGS__, M_WASM_WHILE_NAMED_, M_WASM_WHILE_ANON_)(__VA_ARGS__)

#define M_WASM_THROW2_(TYPE, MSG) m::wasm::Module::Get().emit_throw(TYPE, __FILE__, __LINE__, MSG)
#define M_WASM_THROW1_(TYPE) M_WASM_THROW2_(TYPE, nullptr)
#define M_GET_WASM_THROW_(XXX, _1, _2, NAME, ...) NAME
#define Throw(...) M_GET_WASM_THROW_(XXX, ##__VA_ARGS__, M_WASM_THROW2_, M_WASM_THROW1_)(__VA_ARGS__)
