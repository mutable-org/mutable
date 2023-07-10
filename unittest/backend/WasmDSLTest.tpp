/* vim: set filetype=cpp: */
#include "backend/WasmDSL.hpp"
#include "backend/WasmMacro.hpp"
#include "testutil.hpp"

#ifndef BACKEND_NAME
#error "must define BACKEND_NAME before including this file"
#endif

using namespace m::wasm;


TEST_CASE("Wasm/" BACKEND_NAME "/Module init & dispose", "[core][wasm]")
{
    Module::Init();
    REQUIRE(Module::Validate());
    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/ModuleInstance create from module", "[core][wasm]")
{
    Module::Init();
    auto I = Module::Get().instantiate();
    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/wasm_type", "[core][wasm]")
{
    /*----- Void -----*/
    REQUIRE(wasm_type<void, 1>() == ::wasm::Type::none);

    /*----- Integers -----*/
    REQUIRE(wasm_type<signed char, 1>() == ::wasm::Type::i32);
    REQUIRE(wasm_type<signed short, 1>() == ::wasm::Type::i32);
    REQUIRE(wasm_type<signed int, 1>() == ::wasm::Type::i32);
    REQUIRE(wasm_type<int64_t, 1>() == ::wasm::Type::i64);
    REQUIRE(wasm_type<unsigned char, 1>() == ::wasm::Type::i32);
    REQUIRE(wasm_type<unsigned short, 1>() == ::wasm::Type::i32);
    REQUIRE(wasm_type<unsigned int, 1>() == ::wasm::Type::i32);
    REQUIRE(wasm_type<uint64_t, 1>() == ::wasm::Type::i64);

    /*----- Floating points -----*/
    REQUIRE(wasm_type<float, 1>() == ::wasm::Type::f32);
    REQUIRE(wasm_type<double, 1>() == ::wasm::Type::f64);

    /*----- Pointers -----*/
    REQUIRE(wasm_type<void*, 1>() == ::wasm::Type::i32);
    REQUIRE(wasm_type<float*, 10>() == ::wasm::Type::i32);

    /*----- Functions -----*/
    REQUIRE(wasm_type<void(signed int), 1>() == ::wasm::Signature({ ::wasm::Type::i32 }, ::wasm::Type::none));

    /*----- Vectorial types -----*/
    REQUIRE(wasm_type<unsigned int, 2>() == ::wasm::Type::v128);

    /*----- PrimitiveExprs -----*/
    REQUIRE(wasm_type<I8x1, 1>() == ::wasm::Type::i32);
}

TEST_CASE("Wasm/" BACKEND_NAME "/Block", "[core][wasm]")
{
    Module::Init();

    CHECK_RESULT_INLINE(42, int(), {
        Block the_block(true);
        BLOCK_OPEN(the_block) {
            RETURN(42);
        }
    });

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Function", "[core][wasm]")
{
    Module::Init();

    /*----- T -> void -----*/
    INVOKE_INLINE(void(void), {});
    INVOKE_INLINE(void(), {});
    INVOKE_INLINE(void(void), { RETURN(); });

    INVOKE_INLINE(void(signed short), { }, 42);
    INVOKE_INLINE(void(signed int), { }, 42);
    INVOKE_INLINE(void(int64_t), { }, 42);
    INVOKE_INLINE(void(unsigned short), { }, 42);
    INVOKE_INLINE(void(unsigned int), { }, 42);
    INVOKE_INLINE(void(uint64_t), { }, 42);

    INVOKE_INLINE(void(float), { }, 42);
    INVOKE_INLINE(void(double), { }, 42);

    INVOKE_INLINE(void(I32x4), { }, 42);
    INVOKE_INLINE(void(I32x4), { }, I32x4(42, 17, 314, -1234));

    /*----- Ts... -> void -----*/
    INVOKE_INLINE(void(int, float), { }, 42, 3.14f);
    INVOKE_INLINE(void(char, unsigned, double, float, int), { }, 42, 2U, 2.72, 3.14f, 1337);
    INVOKE_INLINE(void(I32x4, Doublex2), { }, 42, 3.14);

    /*----- void -> T -----*/
    CHECK_RESULT_INLINE(-12, signed char(), {
        RETURN(Expr<signed char>(-12));
    });
    CHECK_RESULT_INLINE(-1234, signed short(), {
        RETURN(Expr<signed short>(-1234));
    });
    CHECK_RESULT_INLINE(-123456, signed int(), {
        RETURN(Expr<signed int>(-123456));
    });
    CHECK_RESULT_INLINE(-123456789012, int64_t(), {
        RETURN(_I64x1(-123456789012));
    });
    CHECK_RESULT_INLINE(12, unsigned char(), {
        RETURN(Expr<unsigned char>(12));
    });
    CHECK_RESULT_INLINE(1234, unsigned short(), {
        RETURN(Expr<unsigned short>(1234));
    });
    CHECK_RESULT_INLINE(123456, unsigned int(), {
        RETURN(Expr<unsigned int>(123456));
    });
    CHECK_RESULT_INLINE(123456789012, uint64_t(), {
        RETURN(_U64x1(123456789012));
    });
    CHECK_RESULT_INLINE(3.14f, float(), {
        RETURN(_Floatx1(3.14f));
    });
    CHECK_RESULT_INLINE(3.14, double(), {
        RETURN(_Doublex1(3.14));
    });
    CHECK_RESULT_INLINE(std::to_array({ 42, 42, 42, 42 }), I32x4(), {
        RETURN(I32x4(42));
    });
    CHECK_RESULT_INLINE(std::to_array({ 42, 17, 314, -1234 }), I32x4(), {
        RETURN(I32x4(42, 17, 314, -1234));
    });

    /*----- T -> T -----*/
    CHECK_RESULT_INLINE(-12, signed char(signed char), {
        RETURN(PARAMETER(0));
    }, -12);
    CHECK_RESULT_INLINE(-1234, signed short(signed short), {
        RETURN(PARAMETER(0));
    }, -1234);
    CHECK_RESULT_INLINE(-123456, signed int(signed int), {
        RETURN(PARAMETER(0));
    }, -123456);
    CHECK_RESULT_INLINE(-123456789012, int64_t(int64_t), {
        RETURN(PARAMETER(0));
    }, -123456789012);
    CHECK_RESULT_INLINE(12, unsigned char(unsigned char), {
        RETURN(PARAMETER(0));
    }, 12);
    CHECK_RESULT_INLINE(1234, unsigned short(unsigned short), {
        RETURN(PARAMETER(0));
    }, 1234);
    CHECK_RESULT_INLINE(123456, unsigned int(unsigned int), {
        RETURN(PARAMETER(0));
    }, 123456);
    CHECK_RESULT_INLINE(123456789012, uint64_t(uint64_t), {
        RETURN(PARAMETER(0));
    }, 123456789012);
    CHECK_RESULT_INLINE(3.14f, float(float), {
        RETURN(PARAMETER(0));
    }, 3.14f);
    CHECK_RESULT_INLINE(3.14, double(double), {
        RETURN(PARAMETER(0));
    }, 3.14);
    CHECK_RESULT_INLINE(std::to_array({ 42, 17, 314, -1234 }), I32x4(I32x4), {
        RETURN(PARAMETER(0));
    }, I32x4(42, 17, 314, -1234));

    /*----- Ts -> T -----*/
    CHECK_RESULT_INLINE(42, int(int, float), {
        RETURN(PARAMETER(0));
    }, 42, 3.14f);
    CHECK_RESULT_INLINE(2.72, double(char, unsigned, double, float, int), {
        RETURN(PARAMETER(2));
    }, 42, 2U, 2.72, 3.14f, 1337);
    CHECK_RESULT_INLINE(std::to_array({ 3.14, 3.14 }), Doublex2(I32x4, Doublex2), {
        RETURN(PARAMETER(1));
    }, 42, 3.14);

    // TODO cover all types as parameter and return types
    // TODO hypothetically, Wasm allows for functions returning multiple values.  We do not yet have a simple interface
    // for that (but we can still define and use them).  Should we test that?

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Function/call", "[core][wasm]")
{
    Module::Init();

    /*----- void -> T -----*/
    CHECK_RESULT_INLINE(17, int(void), {
        FUNCTION(f, int(void))
        {
            RETURN(17);
        }
        RETURN(f());
    });
    CHECK_RESULT_INLINE(3.14, double(void), {
        FUNCTION(f, double(void))
        {
            RETURN(3.14);
        }
        RETURN(f());
    });
    CHECK_RESULT_INLINE(std::to_array({ 17, -4321, 1, 42 }), I32x4(void), {
        FUNCTION(f, I32x4(void))
        {
            RETURN(I32x4(17, -4321, 1, 42));
        }
        RETURN(f());
    });

    /*----- T -> T -----*/
    CHECK_RESULT_INLINE(17, int(void), {
        FUNCTION(f, int(int))
        {
            RETURN(PARAMETER(0));
        }
        RETURN(f(17));
    });
    CHECK_RESULT_INLINE(3.14, double(void), {
        FUNCTION(f, double(double))
        {
            RETURN(PARAMETER(0));
        }
        RETURN(f(3.14));
    });
    CHECK_RESULT_INLINE(std::to_array({ 17, -4321, 1, 42 }), I32x4(void), {
        FUNCTION(f, I32x4(I32x4))
        {
            RETURN(PARAMETER(0));
        }
        RETURN(f(I32x4(17, -4321, 1, 42)));
    });

    /*----- Ts -> T -----*/
    CHECK_RESULT_INLINE(0x2a, char(void), {
        FUNCTION(f, char(int, double, char))
        {
            RETURN(PARAMETER(2));
        }
        RETURN(f(17, 3.14, '\x2a'));
    });
    CHECK_RESULT_INLINE(1.234f, float(void), {
        FUNCTION(f, float(float, int, double, char))
        {
            RETURN(PARAMETER(0));
        }
        RETURN(f(1.234f, 17, 3.14, '\x2a'));
    });
    CHECK_RESULT_INLINE(std::to_array({ 3.14, -1.23 }), Doublex2(void), {
        FUNCTION(f, Doublex2(I32x4, Doublex2))
        {
            RETURN(PARAMETER(1));
        }
        RETURN(f(I32x4(17, -4321, 1, 42), Doublex2(3.14, -1.23)));
    });

    /* implicit parameter conversion */
    CHECK_RESULT_INLINE(0, int8_t(void), {
        FUNCTION(f, int8_t(int8_t))
        {
            RETURN(PARAMETER(0));
        }
        RETURN(f(0x100)); // expect truncation
    });
    /* recursion */
    CHECK_RESULT_INLINE(24, unsigned(void), {
        FUNCTION(fac, unsigned(unsigned))
        {
            auto param = PARAMETER(0);
            IF (param == 0U) {
                RETURN(1U);
            } ELSE {
                RETURN(param * fac(param - 1U));
            };
        }
        RETURN(fac(4U));
    });

    /* side-effects */
    CHECK_RESULT_INLINE(1, int(void), {
        Global<I32x1> global(0);
        FUNCTION(f, int(void))
        {
            global = 1;
            RETURN(0);
        }
        f().discard();
        RETURN(global);
    });

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/GlobalVariable", "[core][wasm]")
{
    Module::Init();

    CHECK_RESULT_INLINE(1, int(void), {
        Global<I32x1> global(0);
        FUNCTION(f, void(void))
        {
            global = 1;
        }
        f();
        RETURN(global);
    });

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Expr/vectorial/c'tor", "[core][wasm]")
{
    Module::Init();

    CHECK_RESULT_INLINE(std::to_array({ 42,  42 }),           I32x2(), { _I32x2 a(42);                RETURN(a); });
    CHECK_RESULT_INLINE(std::to_array({ 42,  42,  42,  42 }), I32x4(), { _I32x4 a(42);                RETURN(a); });
    CHECK_RESULT_INLINE(std::to_array({ 42, -17 }),           I32x2(), { _I32x2 a(42, -17);           RETURN(a); });
    CHECK_RESULT_INLINE(std::to_array({ 42, -17, 123, 321 }), I32x4(), { _I32x4 a(42, -17, 123, 321); RETURN(a); });

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Expr/scalar/operations/unary", "[core][wasm]")
{
    Module::Init();

    CHECK_RESULT_INLINE( 42, int(), { _I32x1 a(42); RETURN(+a); });
    CHECK_RESULT_INLINE(-42, int(), { _I32x1 a(42); RETURN(-a); });
    // abs() not supported for scalar integral types
    // ceil() not supported for integral types
    // floor() not supported for integral types
    // trunc() not supported for scalar types
    // nearest() not supported for scalar types
    // sqrt() not supported for integral types
    // add_pairwise() not supported for scalar types
    CHECK_RESULT_INLINE(  ~42, int32_t(),  { _I32x1 a(42); RETURN(~a); });
    CHECK_RESULT_INLINE(   28, uint32_t(), { _U32x1 a(0b1010);  RETURN(a.clz()); });
    CHECK_RESULT_INLINE(   12, uint16_t(), { _U16x1 a(0b1010);  RETURN(a.clz()); });
    CHECK_RESULT_INLINE(    4, uint8_t(),  { _U8x1  a(0b1010);  RETURN(a.clz()); });
    CHECK_RESULT_INLINE(    1, uint32_t(), { _U32x1 a(0b1010);  RETURN(a.ctz()); });
    CHECK_RESULT_INLINE(    1, uint16_t(), { _U16x1 a(0b1010);  RETURN(a.ctz()); });
    CHECK_RESULT_INLINE(    1, uint8_t(),  { _U8x1  a(0b1010);  RETURN(a.ctz()); });
    CHECK_RESULT_INLINE(    2, uint32_t(), { _U32x1 a(0b1010);  RETURN(a.popcnt()); });
    CHECK_RESULT_INLINE(    2, uint16_t(), { _U16x1 a(0b1010);  RETURN(a.popcnt()); });
    CHECK_RESULT_INLINE(    2, uint8_t(),  { _U8x1  a(0b1010);  RETURN(a.popcnt()); });
    // bitmask() not supported for scalar types
    CHECK_RESULT_INLINE(false, bool(), { _I32x1 a(42); RETURN(a.eqz()); });
    CHECK_RESULT_INLINE( true, bool(), { _I32x1 a(0);  RETURN(a.eqz()); });
    CHECK_RESULT_INLINE(false, bool(), { _Boolx1 a(true);  RETURN(not a); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(false); RETURN(not a); });
    CHECK_RESULT_INLINE( true, bool(), { _I32x1 a(_I32x1::Null()); RETURN(a.is_null()); });
    CHECK_RESULT_INLINE(false, bool(), { _I32x1 a(42);           RETURN(a.is_null()); });
    CHECK_RESULT_INLINE(false, bool(), { _I32x1 a(_I32x1::Null()); RETURN(a.not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _I32x1 a(42);           RETURN(a.not_null()); });
    CHECK_RESULT_INLINE(false, bool(), { _Boolx1 a(_Boolx1::Null()); RETURN(a.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(true);          RETURN(a.is_true_and_not_null()); });
    CHECK_RESULT_INLINE(false, bool(), { _Boolx1 a(false);         RETURN(a.is_true_and_not_null()); });
    CHECK_RESULT_INLINE(false, bool(), { _Boolx1 a(_Boolx1::Null()); RETURN(a.is_false_and_not_null()); });
    CHECK_RESULT_INLINE(false, bool(), { _Boolx1 a(true);          RETURN(a.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(false);         RETURN(a.is_false_and_not_null()); });

    CHECK_RESULT_INLINE( 3.14, double(), { _Doublex1 a( 3.14); RETURN(+a); });
    CHECK_RESULT_INLINE(-3.14, double(), { _Doublex1 a( 3.14); RETURN(-a); });
    CHECK_RESULT_INLINE( 3.14, double(), { _Doublex1 a( 3.14); RETURN(a.abs()); });
    CHECK_RESULT_INLINE( 3.14, double(), { _Doublex1 a(-3.14); RETURN(a.abs()); });
    CHECK_RESULT_INLINE( 4.0,  double(), { _Doublex1 a( 3.14); RETURN(a.ceil()); });
    CHECK_RESULT_INLINE( 3.0,  double(), { _Doublex1 a( 3.14); RETURN(a.floor()); });
    // trunc() not supported for scalar types
    // nearest() not supported for scalar types
    CHECK_RESULT_INLINE( 3.0,  double(), { _Doublex1 a( 9.0);  RETURN(a.sqrt()); });
    // add_pairwise() not supported for scalar types
    // operator~ not supported for floating-point types
    // clz() not supported for floating-point types
    // ctz() not supported for floating-point types
    // popcnt() not supported for floating-point types
    // bitmask() not supported for scalar types
    // eqz() not supported for floating-point types
    // operator! not supported for floating-point types
    // any_true() not supported for floating-point types
    // all_true() not supported for floating-point types
    CHECK_RESULT_INLINE( true, bool(), { _Doublex1 a(_Doublex1::Null()); RETURN(a.is_null()); });
    CHECK_RESULT_INLINE(false, bool(), { _Doublex1 a(3.14);            RETURN(a.is_null()); });
    CHECK_RESULT_INLINE(false, bool(), { _Doublex1 a(_Doublex1::Null()); RETURN(a.not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Doublex1 a(3.14);            RETURN(a.not_null()); });
    // is_true_and_not_null() not supported for floating-point types
    // is_false_and_not_null() not supported for floating-point types

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Expr/vectorial/operations/unary", "[core][wasm]")
{
    Module::Init();

    CHECK_RESULT_INLINE(std::to_array({  42, -17 }), I32x2(), { _I32x2 a(42, -17); RETURN(+a); });
    CHECK_RESULT_INLINE(std::to_array({ -42,  17 }), I32x2(), { _I32x2 a(42, -17); RETURN(-a); });
    CHECK_RESULT_INLINE(std::to_array({  42,  17 }), I32x2(), { _I32x2 a(42, -17); RETURN(a.abs()); });
    // ceil() not supported for integral types
    // floor() not supported for integral types
    // trunc() not supported for integral types
    // nearest() not supported for integral types
    // sqrt() not supported for integral types
    CHECK_RESULT_INLINE(25, int16_t(), { _I8x2  a(42, -17); RETURN(a.add_pairwise()); });
    CHECK_RESULT_INLINE(25, int32_t(), { _I16x2 a(42, -17); RETURN(a.add_pairwise()); });
    CHECK_RESULT_INLINE(std::to_array<int16_t>({ 25, 154 }), I16x2(), { _I8x4  a(42, -17, 123, 31); RETURN(a.add_pairwise()); });
    CHECK_RESULT_INLINE(std::to_array<int32_t>({ 25, 154 }), I32x2(), { _I16x4 a(42, -17, 123, 31); RETURN(a.add_pairwise()); });
    CHECK_RESULT_INLINE(std::to_array({ ~42, ~(-17) }), I32x2(), { _I32x2 a(42, -17); RETURN(~a); });
    // clz() not supported for vectorial types
    // ctz() not supported for vectorial types
    CHECK_RESULT_INLINE(std::to_array({ 2U, 13U }),        U32x2(), { _U32x2 a(0b1010, 0x12345678);  RETURN(a.popcnt()); });
    CHECK_RESULT_INLINE(std::to_array<uint16_t>({ 2, 5 }), U16x2(), { _U16x2 a(0b1010, 0x1234);      RETURN(a.popcnt()); });
    CHECK_RESULT_INLINE(std::to_array<uint8_t>({ 2, 3 }),  U8x2(),  { _U8x2  a(0b1010, 0x34);        RETURN(a.popcnt()); });
    CHECK_RESULT_INLINE(0b1101U, uint32_t(), { _U8x4 a(0x80, 0x70, 0xff, 0xa1); RETURN(a.bitmask()); });
    // eqz() not supported for vectorial types
    CHECK_RESULT_INLINE(std::to_array({ false, true }), Boolx2(), { _Boolx2 a(true, false); RETURN(not a); });
    CHECK_RESULT_INLINE(false, bool(), { _Boolx2 a(false, false); RETURN(a.any_true()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx2 a(true,  false); RETURN(a.any_true()); });
    CHECK_RESULT_INLINE(false, bool(), { _Boolx2 a(true,  false); RETURN(a.all_true()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx2 a(true,  true);  RETURN(a.all_true()); });
    CHECK_RESULT_INLINE(std::to_array({ false, true }), Boolx2(), {
        _I32x2  a(I32x2(42, 0), Boolx2(false, true)); RETURN(a.is_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, false }), Boolx2(), {
        _I32x2  a(I32x2(0, 42), Boolx2(true, false)); RETURN(a.is_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ false, true, false, false }), Boolx4(), {
        _Boolx4 a(Boolx4(false, true, false, true), Boolx4(false, false, true, true)); RETURN(a.is_true_and_not_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, false, false, false }), Boolx4(), {
        _Boolx4 a(Boolx4(false, true, false, true), Boolx4(false, false, true, true)); RETURN(a.is_false_and_not_null());
    });

    CHECK_RESULT_INLINE(std::to_array({  3.14, -1.23 }), Doublex2(), { _Doublex2 a(3.14, -1.23); RETURN(+a); });
    CHECK_RESULT_INLINE(std::to_array({ -3.14,  1.23 }), Doublex2(), { _Doublex2 a(3.14, -1.23); RETURN(-a); });
    CHECK_RESULT_INLINE(std::to_array({  3.14,  1.23 }), Doublex2(), { _Doublex2 a(3.14, -1.23); RETURN(a.abs()); });
    CHECK_RESULT_INLINE(std::to_array({  4.0,  -1.0  }), Doublex2(), { _Doublex2 a(3.14, -1.23); RETURN(a.ceil()); });
    CHECK_RESULT_INLINE(std::to_array({  3.0,  -2.0  }), Doublex2(), { _Doublex2 a(3.14, -1.23); RETURN(a.floor()); });
    CHECK_RESULT_INLINE(std::to_array({  3.0,  -1.0  }), Doublex2(), { _Doublex2 a(3.14, -1.23); RETURN(a.trunc()); });
    CHECK_RESULT_INLINE(std::to_array({  3.0,  -2.0  }), Doublex2(), { _Doublex2 a(3.14, -1.5);  RETURN(a.nearest()); });
    CHECK_RESULT_INLINE(std::to_array({  3.0,   0.0  }), Doublex2(), { _Doublex2 a(9.0, 0.0);    RETURN(a.sqrt()); });
    // add_pairwise() not supported for floating-point types
    // operator~ not supported for floating-point types
    // clz() not supported for floating-point types
    // ctz() not supported for floating-point types
    // popcnt() not supported for floating-point types
    // bitmask() not supported for floating-point types
    // eqz() not supported for vectorial types
    // operator! not supported for floating-point types
    // any_true() not supported for floating-point types
    // all_true() not supported for floating-point types
    CHECK_RESULT_INLINE(std::to_array({ false, true }), Boolx2(), {
        _Doublex2  a(Doublex2(3.14, 0.0), Boolx2(false, true)); RETURN(a.is_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, false }), Boolx2(), {
        _Doublex2  a(Doublex2(0.0, 3.14), Boolx2(true, false)); RETURN(a.is_null());
    });
    // is_true_and_not_null() not supported for floating-point types
    // is_false_and_not_null() not supported for floating-point types

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Expr/scalar/operations/binary", "[core][wasm]")
{
    Module::Init();

    CHECK_RESULT_INLINE( 59, int(), { _I32x1 a(42); _I32x1 b(17); RETURN(a + b); });
    CHECK_RESULT_INLINE( 25, int(), { _I32x1 a(42); _I32x1 b(17); RETURN(a - b); });
    CHECK_RESULT_INLINE(714, int(), { _I32x1 a(42); _I32x1 b(17); RETURN(a * b); });
    CHECK_RESULT_INLINE(  2, int(), { _I32x1 a(42); _I32x1 b(17); RETURN(a / b); });
    CHECK_RESULT_INLINE(  8, int(), { _I32x1 a(42); _I32x1 b(17); RETURN(a % b); });
    // copy_sign() not supported for integral types
    // min() not supported for integral types
    // max() not supported for integral types
    // avg() not supported for scalar types
    CHECK_RESULT_INLINE(0b00001000, int(), { _I8x1 a(0b00101010);  _I8x1 b(0b00001001); RETURN(a bitand b); });
    CHECK_RESULT_INLINE(0b00101011, int(), { _I8x1 a(0b00101010);  _I8x1 b(0b00001001); RETURN(a bitor b); });
    CHECK_RESULT_INLINE(0b00100011, int(), { _I8x1 a(0b00101010);  _I8x1 b(0b00001001); RETURN(a xor b); });
    CHECK_RESULT_INLINE(0b01000000, int8_t(), { _I8x1 a(0b00101010);  _I8x1 b(5);  RETURN(a << b); });
    CHECK_RESULT_INLINE(0b00001010, int8_t(), { _I8x1 a(0b00101010);  _I8x1 b(2);  RETURN(a >> b); });
    CHECK_RESULT_INLINE(0x01010001, int(), { _I32x1 a(0x00101010); _I32x1 b(12); RETURN(rotl(a, b)); });
    CHECK_RESULT_INLINE(0x01000101, int(), { _I32x1 a(0x00101010); _I32x1 b(12); RETURN(rotr(a, b)); });
    CHECK_RESULT_INLINE( true, bool(), { _I32x1 a(42); _I32x1 b(42); RETURN(a == b); });
    CHECK_RESULT_INLINE(false, bool(), { _I32x1 a(42); _I32x1 b(17); RETURN(a == b); });
    CHECK_RESULT_INLINE(false, bool(), { _I32x1 a(42); _I32x1 b(42); RETURN(a != b); });
    CHECK_RESULT_INLINE( true, bool(), { _I32x1 a(42); _I32x1 b(17); RETURN(a != b); });
    CHECK_RESULT_INLINE(false, bool(), { _I32x1 a(42); _I32x1 b(42); RETURN(a < b); });
    CHECK_RESULT_INLINE(false, bool(), { _I32x1 a(42); _I32x1 b(17); RETURN(a < b); });
    CHECK_RESULT_INLINE( true, bool(), { _I32x1 a(17); _I32x1 b(42); RETURN(a < b); });
    CHECK_RESULT_INLINE( true, bool(), { _I32x1 a(42); _I32x1 b(42); RETURN(a <= b); });
    CHECK_RESULT_INLINE(false, bool(), { _I32x1 a(42); _I32x1 b(17); RETURN(a <= b); });
    CHECK_RESULT_INLINE( true, bool(), { _I32x1 a(17); _I32x1 b(42); RETURN(a <= b); });
    CHECK_RESULT_INLINE(false, bool(), { _I32x1 a(42); _I32x1 b(42); RETURN(a > b); });
    CHECK_RESULT_INLINE( true, bool(), { _I32x1 a(42); _I32x1 b(17); RETURN(a > b); });
    CHECK_RESULT_INLINE(false, bool(), { _I32x1 a(17); _I32x1 b(42); RETURN(a > b); });
    CHECK_RESULT_INLINE( true, bool(), { _I32x1 a(42); _I32x1 b(42); RETURN(a >= b); });
    CHECK_RESULT_INLINE( true, bool(), { _I32x1 a(42); _I32x1 b(17); RETURN(a >= b); });
    CHECK_RESULT_INLINE(false, bool(), { _I32x1 a(17); _I32x1 b(42); RETURN(a >= b); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(true);  _Boolx1 b(true);  RETURN(a and b); });
    CHECK_RESULT_INLINE(false, bool(), { _Boolx1 a(false); _Boolx1 b(true);  RETURN(a and b); });
    CHECK_RESULT_INLINE(false, bool(), { _Boolx1 a(true);  _Boolx1 b(false); RETURN(a and b); });
    CHECK_RESULT_INLINE(false, bool(), { _Boolx1 a(false); _Boolx1 b(false); RETURN(a and b); });
    // and_not() not supported for scalar types
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(true);  _Boolx1 b(true);  RETURN(a or b); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(false); _Boolx1 b(true);  RETURN(a or b); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(true);  _Boolx1 b(false); RETURN(a or b); });
    CHECK_RESULT_INLINE(false, bool(), { _Boolx1 a(false); _Boolx1 b(false); RETURN(a or b); });

    CHECK_RESULT_INLINE( Approx(5.85), double(), { _Doublex1 a(3.14); _Doublex1 b(2.71); RETURN(a + b); });
    CHECK_RESULT_INLINE( Approx(0.43), double(), { _Doublex1 a(3.14); _Doublex1 b(2.71); RETURN(a - b); });
    CHECK_RESULT_INLINE( Approx(1.57), double(), { _Doublex1 a(3.14); _Doublex1 b(0.5);  RETURN(a * b); });
    CHECK_RESULT_INLINE( Approx(6.28), double(), { _Doublex1 a(3.14); _Doublex1 b(0.5);  RETURN(a / b); });
    // operator% not supported for floating-point types
    CHECK_RESULT_INLINE( 3.14, double(), { _Doublex1 a(3.14); _Doublex1 b( 1.0);  RETURN(copy_sign(a, b)); });
    CHECK_RESULT_INLINE(-3.14, double(), { _Doublex1 a(3.14); _Doublex1 b(-1.0);  RETURN(copy_sign(a, b)); });
    CHECK_RESULT_INLINE( 2.71, double(), { _Doublex1 a(3.14); _Doublex1 b( 2.71); RETURN(min(a, b)); });
    CHECK_RESULT_INLINE( 3.14, double(), { _Doublex1 a(3.14); _Doublex1 b( 2.71); RETURN(max(a, b)); });
    // avg() not supported for scalar types
    // operator& not supported for floating-point types
    // operator| not supported for floating-point types
    // operator^ not supported for floating-point types
    // operator<< not supported for floating-point types
    // operator>> not supported for floating-point types
    // rotl() not supported for floating-point types
    // rotr() not supported for floating-point types
    CHECK_RESULT_INLINE( true, bool(),   { _Doublex1 a(3.14); _Doublex1 b(3.14); RETURN(a == b); });
    CHECK_RESULT_INLINE(false, bool(),   { _Doublex1 a(3.14); _Doublex1 b(2.71); RETURN(a == b); });
    CHECK_RESULT_INLINE(false, bool(),   { _Doublex1 a(3.14); _Doublex1 b(3.14); RETURN(a != b); });
    CHECK_RESULT_INLINE( true, bool(),   { _Doublex1 a(3.14); _Doublex1 b(2.71); RETURN(a != b); });
    CHECK_RESULT_INLINE(false, bool(),   { _Doublex1 a(3.14); _Doublex1 b(3.14); RETURN(a < b); });
    CHECK_RESULT_INLINE(false, bool(),   { _Doublex1 a(3.14); _Doublex1 b(2.71); RETURN(a < b); });
    CHECK_RESULT_INLINE( true, bool(),   { _Doublex1 a(2.71); _Doublex1 b(3.14); RETURN(a < b); });
    CHECK_RESULT_INLINE( true, bool(),   { _Doublex1 a(3.14); _Doublex1 b(3.14); RETURN(a <= b); });
    CHECK_RESULT_INLINE(false, bool(),   { _Doublex1 a(3.14); _Doublex1 b(2.71); RETURN(a <= b); });
    CHECK_RESULT_INLINE( true, bool(),   { _Doublex1 a(2.71); _Doublex1 b(3.14); RETURN(a <= b); });
    CHECK_RESULT_INLINE(false, bool(),   { _Doublex1 a(3.14); _Doublex1 b(3.14); RETURN(a > b); });
    CHECK_RESULT_INLINE( true, bool(),   { _Doublex1 a(3.14); _Doublex1 b(2.71); RETURN(a > b); });
    CHECK_RESULT_INLINE(false, bool(),   { _Doublex1 a(2.71); _Doublex1 b(3.14); RETURN(a > b); });
    CHECK_RESULT_INLINE( true, bool(),   { _Doublex1 a(3.14); _Doublex1 b(3.14); RETURN(a >= b); });
    CHECK_RESULT_INLINE( true, bool(),   { _Doublex1 a(3.14); _Doublex1 b(2.71); RETURN(a >= b); });
    CHECK_RESULT_INLINE(false, bool(),   { _Doublex1 a(2.71); _Doublex1 b(3.14); RETURN(a >= b); });
    // operator&& not supported for floating-point types
    // and_not() not supported for scalar types
    // operator|| not supported for floating-point types

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Expr/vectorial/operations/binary", "[core][wasm]")
{
    Module::Init();

    CHECK_RESULT_INLINE(std::to_array({  25,  34 }), I32x2(), { _I32x2 a(42, 31); _I32x2 b(-17, 3); RETURN(a + b); });
    CHECK_RESULT_INLINE(std::to_array({  59,  28 }), I32x2(), { _I32x2 a(42, 31); _I32x2 b(-17, 3); RETURN(a - b); });
    CHECK_RESULT_INLINE(std::to_array({ -714, 93 }), I32x2(), { _I32x2 a(42, 31); _I32x2 b(-17, 3); RETURN(a * b); });
    // operator/() not supported for vectorial integral types
    // operator%() not supported for vectorial types
    // copy_sign() not supported for vectorial types
    CHECK_RESULT_INLINE(std::to_array({ -17,  3 }), I32x2(), { _I32x2 a(42, 3); _I32x2 b(-17, 31); RETURN(min(a, b)); });
    CHECK_RESULT_INLINE(std::to_array({  42, 31 }), I32x2(), { _I32x2 a(42, 3); _I32x2 b(-17, 31); RETURN(max(a, b)); });
    CHECK_RESULT_INLINE(std::to_array<uint16_t>({ 30, 17 }), U16x2(), { _U16x2 a(42, 31); _U16x2 b(17, 3); RETURN(avg(a, b)); });
    CHECK_RESULT_INLINE(std::to_array<int8_t>({ 0b00001000, 0b00000001 }), I8x2(), {
        _I8x2 a(0b00101010, 0b11001001); _I8x2 b(0b00001001, 0b00000101); RETURN(a bitand b);
    });
    CHECK_RESULT_INLINE(std::to_array<int8_t>({ 0b00101011, int8_t(0b11001101) }), I8x2(), {
        _I8x2 a(0b00101010, 0b11001001); _I8x2 b(0b00001001, 0b00000101); RETURN(a bitor b);
    });
    CHECK_RESULT_INLINE(std::to_array<int8_t>({ 0b00100011, int8_t(0b11001100) }), I8x2(), {
        _I8x2 a(0b00101010, 0b11001001); _I8x2 b(0b00001001, 0b00000101); RETURN(a xor b);
    });
    CHECK_RESULT_INLINE(std::to_array<int8_t>({ 0b01000000, 0b00100000 }), I8x2(), {
        _I8x2 a(0b00101010, 0b11001001); _I8x1 b(5); RETURN(a << b);
    });
    CHECK_RESULT_INLINE(std::to_array<int8_t>({ 0b00001010, int8_t(0b11110010) }), I8x2(), {
        _I8x2 a(0b00101010, 0b11001001); _I8x1 b(2); RETURN(a >> b);
    });
    // rotl() not supported for vectorial types
    // rotr() not supported for vectorial types
    CHECK_RESULT_INLINE(std::to_array({ true, false }), Boolx2(), {
        _I32x2 a(42, 42); _I32x2 b(42, 17); RETURN(a == b);
    });
    CHECK_RESULT_INLINE(std::to_array({ false, true }), Boolx2(), {
        _I32x2 a(42, 42); _I32x2 b(42, 17); RETURN(a != b);
    });
    CHECK_RESULT_INLINE(std::to_array({ false, false, true, false }), Boolx4(), {
        _I32x4 a(42, 42, 42, 42); _I32x4 b(42, 17, 81, -42); RETURN(a < b);
    });
    CHECK_RESULT_INLINE(std::to_array({ false, true }),  Boolx2(), {
        _U64x2 a(42, 42); _U64x2 b(42, -1UL); RETURN(a < b);
    });
    CHECK_RESULT_INLINE(std::to_array({ true, false, true, false }),  Boolx4(), {
        _I32x4 a(42, 42, 42, 42); _I32x4 b(42, 17, 81, -42); RETURN(a <= b);
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }),   Boolx2(), {
        _U64x2 a(42, 42); _U64x2 b(42, -1UL); RETURN(a <= b);
    });
    CHECK_RESULT_INLINE(std::to_array({ false, true, false, true }),  Boolx4(), {
        _I32x4 a(42, 42, 42, 42); _I32x4 b(42, 17, 81, -42); RETURN(a > b);
    });
    CHECK_RESULT_INLINE(std::to_array({ false, false }), Boolx2(), {
        _U64x2 a(42, 42); _U64x2 b(42, -1UL); RETURN(a > b);
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true, false, true }),   Boolx4(), {
        _I32x4 a(42, 42, 42, 42); _I32x4 b(42, 17, 81, -42); RETURN(a >= b);
    });
    CHECK_RESULT_INLINE(std::to_array({ true, false }),  Boolx2(), {
        _U64x2 a(42, 42); _U64x2 b(42, -1UL); RETURN(a >= b);
    });
    CHECK_RESULT_INLINE(std::to_array({ false, false, false, true }), Boolx4(), {
        _Boolx4 a(false, true, false, true); _Boolx4 b(false, false, true, true); RETURN(a and b);
    });
    CHECK_RESULT_INLINE(std::to_array({ false, true, false, false }), Boolx4(), {
        _Boolx4 a(false, true, false, true); _Boolx4 b(false, false, true, true); RETURN(and_not(a, b));
    });
    CHECK_RESULT_INLINE(std::to_array({ false, true, true, true }),   Boolx4(), {
        _Boolx4 a(false, true, false, true); _Boolx4 b(false, false, true, true); RETURN(a or b);
    });

    CHECK_RESULT_INLINE(m::testutil::Approx(std::to_array({ 0.43, 4.44 })),    Doublex2(), {
        _Doublex2 a(3.14, 1.23); _Doublex2 b(-2.71, 3.21); RETURN(a + b);
    });
    CHECK_RESULT_INLINE(m::testutil::Approx(std::to_array({ 5.85, -1.98 })),   Doublex2(), {
        _Doublex2 a(3.14, 1.23); _Doublex2 b(-2.71, 3.21); RETURN(a - b);
    });
    CHECK_RESULT_INLINE(m::testutil::Approx(std::to_array({ -1.57, 4.92 })),   Doublex2(), {
        _Doublex2 a(3.14, 1.23); _Doublex2 b(-0.5,  4.0);  RETURN(a * b);
    });
    CHECK_RESULT_INLINE(m::testutil::Approx(std::to_array({ -6.28, 0.3075 })), Doublex2(), {
        _Doublex2 a(3.14, 1.23); _Doublex2 b(-0.5,  4.0);  RETURN(a / b);
    });
    // operator% not supported for vectorial types
    // copy_sign() not supported for vectorial types
    CHECK_RESULT_INLINE(std::to_array({ -2.71, 1.23 }),   Doublex2(), {
        _Doublex2 a(3.14, 1.23); _Doublex2 b(-2.71, 3.21); RETURN(min(a, b));
    });
    CHECK_RESULT_INLINE(std::to_array({ 3.14, 3.21 }),    Doublex2(), {
        _Doublex2 a(3.14, 1.23); _Doublex2 b(-2.71, 3.21); RETURN(max(a, b));
    });
    // avg() not supported for floating-point types
    // operator& not supported for floating-point types
    // operator| not supported for floating-point types
    // operator^ not supported for floating-point types
    // operator<< not supported for floating-point types
    // operator>> not supported for floating-point types
    // rotl() not supported for vectorial types
    // rotr() not supported for vectorial types
    CHECK_RESULT_INLINE(std::to_array({ true, false }), Boolx2(), {
        _Doublex2 a(3.14, 3.14); _Doublex2 b(3.14, 2.71); RETURN(a == b);
    });
    CHECK_RESULT_INLINE(std::to_array({ false, true }), Boolx2(), {
        _Doublex2 a(3.14, 3.14); _Doublex2 b(3.14, 2.71); RETURN(a != b);
    });
    CHECK_RESULT_INLINE(std::to_array({ false, false, true, false }), Boolx4(), {
        _Floatx4 a(3.14f, 3.14f, 3.14f, 3.14f); _Floatx4 b(3.14f, 2.71f, 9.87f, -3.14f); RETURN(a < b);
    });
    CHECK_RESULT_INLINE(std::to_array({ true, false, true, false }),  Boolx4(), {
        _Floatx4 a(3.14f, 3.14f, 3.14f, 3.14f); _Floatx4 b(3.14f, 2.71f, 9.87f, -3.14f); RETURN(a <= b);
    });
    CHECK_RESULT_INLINE(std::to_array({ false, true, false, true }),  Boolx4(), {
        _Floatx4 a(3.14f, 3.14f, 3.14f, 3.14f); _Floatx4 b(3.14f, 2.71f, 9.87f, -3.14f); RETURN(a > b);
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true, false, true }),   Boolx4(), {
        _Floatx4 a(3.14f, 3.14f, 3.14f, 3.14f); _Floatx4 b(3.14f, 2.71f, 9.87f, -3.14f); RETURN(a >= b);
    });
    // operator&& not supported for floating-point types
    // and_not() not supported for scalar types
    // operator|| not supported for floating-point types

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Expr/vectorial/double pumping", "[core][wasm]")
{
    Module::Init();

    CHECK_RESULT_INLINE(std::to_array({ true,  false, false, false, false, false, false, false,
                                        false, false, false, false, false, false, false, false }), Boolx16(), {
        _I32x16 a(42); _I32x16 b(42, -42, 17, 81, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0); RETURN(a == b);
    });
    CHECK_RESULT_INLINE(std::to_array({ false, true,  true,  true,  true,  true,  true,  true,
                                        true,  true,  true,  true,  true,  true,  true,  true  }), Boolx16(), {
        _I32x16 a(42); _I32x16 b(42, -42, 17, 81, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0); RETURN(a != b);
    });
    CHECK_RESULT_INLINE(std::to_array({ false, false, false, true,  false, false, false, false,
                                        false, false, false, false, false, false, false, false }), Boolx16(), {
        _I32x16 a(42); _I32x16 b(42, -42, 17, 81, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0); RETURN(a < b);
    });
    CHECK_RESULT_INLINE(std::to_array({ true,  false, false, true,  false, false, false, false,
                                        false, false, false, false, false, false, false, false }), Boolx16(), {
        _I32x16 a(42); _I32x16 b(42, -42, 17, 81, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0); RETURN(a <= b);
    });
    CHECK_RESULT_INLINE(std::to_array({ false, true,  true,  false, true,  true,  true,  true,
                                        true,  true,  true,  true,  true,  true,  true,  true  }), Boolx16(), {
        _I32x16 a(42); _I32x16 b(42, -42, 17, 81, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0); RETURN(a > b);
    });
    CHECK_RESULT_INLINE(std::to_array({ true,  true,  true,  false, true,  true,  true,  true,
                                        true,  true,  true,  true,  true,  true,  true,  true  }), Boolx16(), {
        _I32x16 a(42); _I32x16 b(42, -42, 17, 81, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0); RETURN(a >= b);
    });

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Expr/vectorial/modifications", "[core][wasm]")
{
    Module::Init();

    CHECK_RESULT_INLINE(-17, int32_t(), { _I32x2 a(42, -17); RETURN(a.extract<1>()); });
    CHECK_RESULT_INLINE(std::to_array({ 42, 123 }), I32x2(), { _I32x2 a(42, -17); _I32x2 b(a.replace<1>(123)); RETURN(b); });

    CHECK_RESULT_INLINE(int16_t(0x0100), int16_t(), {
        I16x2 a(0x0123, 0xff12); auto i = std::to_array<uint8_t>({ 4, 1 }); RETURN(a.swizzle_bytes(i));
    });
    CHECK_RESULT_INLINE(std::to_array<int16_t>({ 0x1200, 0x2301, int16_t(0xff00), 0x0001 }), I16x4(), {
        I16x2 a(0x0123, 0xff12); auto i = std::to_array<uint8_t>({ 4, 2, 1, 0, 4, 3, 1, 4 }); RETURN(a.swizzle_bytes(i));
    });
    CHECK_RESULT_INLINE(std::to_array<int16_t>({ 0, -17 }), I16x2(), {
        _I16x4 a(42, -17, 123, -1); auto i = std::to_array<uint8_t>({ 4, 1 }); RETURN(a.swizzle_lanes(i));
    });
    CHECK_RESULT_INLINE(std::to_array<int16_t>({ 0, 123, -17, 42, 0, -1, -17, 0 }), I16x8(), {
        _I16x4 a(42, -17, 123, -1); auto i = std::to_array<uint8_t>({ 4, 2, 1, 0, 4, 3, 1, 4 }); RETURN(a.swizzle_lanes(i));
    });

    CHECK_RESULT_INLINE(int16_t(0x0112), int16_t(), {
        I16x2 a(0x0123, 0xff12); I16x2 b(0x4567, 0xee34); auto i = std::to_array<uint8_t>({ 2, 1 });
        RETURN(ShuffleBytes(a, b, i));
    });
    CHECK_RESULT_INLINE(std::to_array<int16_t>({ 0x1245, 0x2301, int16_t(0xffee), 0x6701 }), I16x4(), {
        I16x2 a(0x0123, 0xff12); I16x2 b(0x4567, 0xee34); auto i = std::to_array<uint8_t>({ 5, 2, 1, 0, 7, 3, 1, 4 });
        RETURN(ShuffleBytes(a, b, i));
    });
    CHECK_RESULT_INLINE(std::to_array<int16_t>({ 123, -17 }), I16x2(), {
        _I16x2 a(42, -17); _I16x2 b(123, -1); auto i = std::to_array<uint8_t>({ 2, 1 }); RETURN(ShuffleLanes(a, b, i));
    });
    CHECK_RESULT_INLINE(std::to_array<int16_t>({ 123, -17, 42, -1 }), I16x4(), {
        _I16x2 a(42, -17); _I16x2 b(123, -1); auto i = std::to_array<uint8_t>({ 2, 1, 0, 3 }); RETURN(ShuffleLanes(a, b, i));
    });

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Expr/conversions", "[core][wasm]")
{
    Module::Init();

    /* 8bit -> 32bit */
    CHECK_RESULT_INLINE( 42, uint32_t(), { _U8x1 a(42);  RETURN(a); });
    CHECK_RESULT_INLINE( 42, uint32_t(), { _U8x1 a(42);  RETURN(a.to<uint32_t>()); });
    CHECK_RESULT_INLINE(-17, int32_t(),  { _I8x1 a(-17); RETURN(a); });
    CHECK_RESULT_INLINE(-17, int32_t(),  { _I8x1 a(-17); RETURN(a.to<int32_t>()); });

    CHECK_RESULT_INLINE(std::to_array({ 42U, 17U }), U32x2(), { _U8x2 a(42,  17); RETURN(a); });
    CHECK_RESULT_INLINE(std::to_array({ 42U, 17U }), U32x2(), { _U8x2 a(42,  17); RETURN(a.to<uint32_t>()); });
    CHECK_RESULT_INLINE(std::to_array({ 42, -17 }),  I32x2(), { _I8x2 a(42, -17); RETURN(a); });
    CHECK_RESULT_INLINE(std::to_array({ 42, -17 }),  I32x2(), { _I8x2 a(42, -17); RETURN(a.to<int32_t>()); });

    /* 32bit -> 8bit */
    // implicit conversion from uint32_t to uint8_t not allowed
    CHECK_RESULT_INLINE(0x78, uint8_t(), { _U32x1 a(0x12345678); RETURN(a.to<uint8_t>()); });
    // implicit conversion from int32_t to int8_t not allowed
    CHECK_RESULT_INLINE(0x21, int8_t(),  { _I32x1 a(0x87654321); RETURN(a.to<int8_t>()); });

    // implicit conversion from uint32_t to uint8_t not allowed
    CHECK_RESULT_INLINE(        42,  uint8_t(), { _U32x16 a(42);  RETURN(a.to<uint8_t>().template extract<0>()); });
    CHECK_RESULT_INLINE(uint8_t(-1), uint8_t(), { _U32x16 a(-1U); RETURN(a.to<uint8_t>().template extract<0>()); });
    // implicit conversion from int32_t to int8_t not allowed
    CHECK_RESULT_INLINE(        42, int8_t(), { _I32x16 a(42);  RETURN(a.to<int8_t>().template extract<0>()); });
    CHECK_RESULT_INLINE(       -17, int8_t(), { _I32x16 a(-17); RETURN(a.to<int8_t>().template extract<0>()); });
    CHECK_RESULT_INLINE(int8_t(-1), int8_t(), { _I32x16 a(-1);  RETURN(a.to<int8_t>().template extract<0>()); });

    CHECK_RESULT_INLINE( 42, int32_t(), { _I64x4 a(42);  RETURN(a.to<int32_t>().template extract<0>()); });
    CHECK_RESULT_INLINE(-17, int32_t(), { _I64x4 a(-17); RETURN(a.to<int32_t>().template extract<0>()); });

    /* float -> double */
    CHECK_RESULT_INLINE(Approx( 3.14), double(), { _Floatx1 a(3.14f);  RETURN(a); });
    CHECK_RESULT_INLINE(Approx( 3.14), double(), { _Floatx1 a(3.14f);  RETURN(a.to<double>()); });
    CHECK_RESULT_INLINE(Approx(-2.71), double(), { _Floatx1 a(-2.71f); RETURN(a); });
    CHECK_RESULT_INLINE(Approx(-2.71), double(), { _Floatx1 a(-2.71f); RETURN(a.to<double>()); });

    /* double -> float */
    // implicit conversion from double to float not allowed
    CHECK_RESULT_INLINE(Approx( 3.1415926535f), float(), { _Doublex1 a(3.1415926535);  RETURN(a.to<float>()); });
    // implicit conversion from double to float not allowed
    CHECK_RESULT_INLINE(Approx(-2.7182818284f), float(), { _Doublex1 a(-2.7182818284); RETURN(a.to<float>()); });

    /* T -> bool */
    CHECK_RESULT_INLINE( true, bool(), { Expr<unsigned> a(42); RETURN(a.to<bool>()); });
    CHECK_RESULT_INLINE(false, bool(), { Expr<unsigned> a(0U); RETURN(a.to<bool>()); });
    CHECK_RESULT_INLINE( true, bool(), { _I32x1 a(42);      RETURN(a.to<bool>()); });
    CHECK_RESULT_INLINE(false, bool(), { _I32x1 a(0);       RETURN(a.to<bool>()); });
    CHECK_RESULT_INLINE( true, bool(), { _Doublex1 a(42.0); RETURN(a.to<bool>()); });
    CHECK_RESULT_INLINE(false, bool(), { _Doublex1 a(0.0);  RETURN(a.to<bool>()); });
    CHECK_RESULT_INLINE( true, bool(), { _Floatx1 a(42.0f); RETURN(a.to<bool>()); });
    CHECK_RESULT_INLINE(false, bool(), { _Floatx1 a(0.0f);  RETURN(a.to<bool>()); });
    // implicit conversion to bool not allowed

    /* implicit conversions */
    CHECK_RESULT_INLINE(0x5a,       uint8_t(),  { _U8x1 a(0x12); _U8x1  b(0x48); RETURN(a | b); });
    CHECK_RESULT_INLINE(0x0000005a, uint32_t(), { _U8x1 a(0x12); _U8x1  b(0x48); RETURN(a | b); });
    // implicit conversion from uint32_t (i.e. result of uint8_t and uint16_t) to uint8_t not allowed
    CHECK_RESULT_INLINE(0x0000005a, uint32_t(), { _U8x1 a(0x12); _U16x1 b(0x48); RETURN(a | b); });
    CHECK_RESULT_INLINE(0x5a,       int8_t(),   { _I8x1  a(0x12); _I8x1   b(0x48); RETURN(a | b); });
    CHECK_RESULT_INLINE(0x0000005a, int32_t(),  { _I8x1  a(0x12); _I8x1   b(0x48); RETURN(a | b); });
    // implicit conversion from int32_t (i.e. result of int8_t and int16_t) to int8_t not allowed
    CHECK_RESULT_INLINE(0x0000005a, int32_t(),  { _I8x1  a(0x12); _I16x1  b(0x48); RETURN(a | b); });

    CHECK_RESULT_INLINE(Approx(5.85f), float(),  { _Floatx1 a(3.14f); _Floatx1  b(2.71f); RETURN(a + b); });
    CHECK_RESULT_INLINE(Approx(5.85),  double(), { _Floatx1 a(3.14f); _Floatx1  b(2.71f); RETURN(a + b); });
    // implicit conversion from double (i.e. result of float and double) to float not allowed
    CHECK_RESULT_INLINE(Approx(5.85),  double(), { _Floatx1 a(3.14f); _Doublex1 b(2.71);  RETURN(a + b); });

    /* change signedness */
    CHECK_RESULT_INLINE(          42,  int(),      { Expr<unsigned> a( 42);  RETURN(a.make_signed()); });
    CHECK_RESULT_INLINE(         -42,  int(),      { Expr<unsigned> a(-42);  RETURN(a.make_signed()); });
    CHECK_RESULT_INLINE(          42,  unsigned(), { _I32x1 a( 42);  RETURN(a.make_unsigned()); });
    CHECK_RESULT_INLINE(unsigned(-42), unsigned(), { _I32x1 a(-42);  RETURN(a.make_unsigned()); });

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Expr/null_semantics", "[core][wasm]")
{
    Module::Init();

    /* with unary operations */
    CHECK_RESULT_INLINE( true, bool(), { _I32x1 a(_I32x1::Null()); _I32x1 b(-a); RETURN(b.is_null()); });
    CHECK_RESULT_INLINE(false, bool(), { _I32x1 a(42); _I32x1 b(-a); RETURN(b.is_null()); });

    /* with binary operations */
    CHECK_RESULT_INLINE( true, bool(), { _I32x1 a(_I32x1::Null()); _I32x1 b(_I32x1::Null()); _I32x1 c(a + b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _I32x1 a(_I32x1::Null()); _I32x1 b(17); _I32x1 c(a + b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _I32x1 a(42); _I32x1 b(_I32x1::Null()); _I32x1 c(a + b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE(false, bool(), { _I32x1 a(42); _I32x1 b(17); _I32x1 c(a + b); RETURN(c.is_null()); });

    /* special cases with `add_pairwise` */
    CHECK_RESULT_INLINE(std::to_array({ false, true, true, true }), Boolx4(), {
        _I8x8 a(I8x8(42, -17, 123, 31, -7, 21, 99, 1), Boolx8(false, false, false, true, true, false, true, true));
        RETURN(a.add_pairwise().is_null());
    });

    /* special cases with `bitmask`, `any_true`, and `all_true` */
    CHECK_RESULT_INLINE(false, bool(), { _I8x2 a(I8x2(42, -17), Boolx2(false, false)); RETURN(a.bitmask().is_null()); })
    CHECK_RESULT_INLINE( true, bool(), { _I8x2 a(I8x2(42, -17), Boolx2(false,  true)); RETURN(a.bitmask().is_null()); })
    CHECK_RESULT_INLINE( true, bool(), { _I8x2 a(I8x2(42, -17), Boolx2( true, false)); RETURN(a.bitmask().is_null()); })
    CHECK_RESULT_INLINE( true, bool(), { _I8x2 a(I8x2(42, -17), Boolx2( true,  true)); RETURN(a.bitmask().is_null()); })
    CHECK_RESULT_INLINE(false, bool(), { _I8x2 a(I8x2(42, -17), Boolx2(false, false)); RETURN(a.any_true().is_null()); })
    CHECK_RESULT_INLINE( true, bool(), { _I8x2 a(I8x2(42, -17), Boolx2(false,  true)); RETURN(a.any_true().is_null()); })
    CHECK_RESULT_INLINE( true, bool(), { _I8x2 a(I8x2(42, -17), Boolx2( true, false)); RETURN(a.any_true().is_null()); })
    CHECK_RESULT_INLINE( true, bool(), { _I8x2 a(I8x2(42, -17), Boolx2( true,  true)); RETURN(a.any_true().is_null()); })
    CHECK_RESULT_INLINE(false, bool(), { _I8x2 a(I8x2(42, -17), Boolx2(false, false)); RETURN(a.all_true().is_null()); })
    CHECK_RESULT_INLINE( true, bool(), { _I8x2 a(I8x2(42, -17), Boolx2(false,  true)); RETURN(a.all_true().is_null()); })
    CHECK_RESULT_INLINE( true, bool(), { _I8x2 a(I8x2(42, -17), Boolx2( true, false)); RETURN(a.all_true().is_null()); })
    CHECK_RESULT_INLINE( true, bool(), { _I8x2 a(I8x2(42, -17), Boolx2( true,  true)); RETURN(a.all_true().is_null()); })

    /* special cases with logical `or`, `and_not`, and `and` */
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(_Boolx1::Null()); _Boolx1 b(_Boolx1::Null()); _Boolx1 c(a or b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(_Boolx1::Null()); _Boolx1 b(false); _Boolx1 c(a or b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(_Boolx1::Null()); _Boolx1 b(true);  _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(false); _Boolx1 b(_Boolx1::Null()); _Boolx1 c(a or b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(false); _Boolx1 b(false); _Boolx1 c(a or b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(false); _Boolx1 b(true);  _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(true);  _Boolx1 b(_Boolx1::Null()); _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(true);  _Boolx1 b(false); _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(true);  _Boolx1 b(true);  _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(_Boolx1::Null()); Boolx1 b(false); _Boolx1 c(a or b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(_Boolx1::Null()); Boolx1 b(true);  _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(false); Boolx1 b(false); _Boolx1 c(a or b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(false); Boolx1 b(true);  _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(true);  Boolx1 b(false); _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(true);  Boolx1 b(true);  _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { Boolx1 a(false); _Boolx1 b(_Boolx1::Null()); _Boolx1 c(a or b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { Boolx1 a(false); _Boolx1 b(false); _Boolx1 c(a or b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { Boolx1 a(false); _Boolx1 b(true);  _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { Boolx1 a(true);  _Boolx1 b(_Boolx1::Null()); _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { Boolx1 a(true);  _Boolx1 b(false); _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { Boolx1 a(true);  _Boolx1 b(true);  _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });

    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        _Boolx2 a(_Boolx2::Null()); _Boolx2 b(_Boolx2::Null()); _Boolx2 c(and_not(a, b)); RETURN(c.is_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        _Boolx2 a(_Boolx2::Null()); _Boolx2 b(false); _Boolx2 c(and_not(a, b)); RETURN(c.is_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        _Boolx2 a(_Boolx2::Null()); _Boolx2 b(true);  _Boolx2 c(and_not(a, b)); RETURN(c.is_false_and_not_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        _Boolx2 a(false); _Boolx2 b(_Boolx2::Null()); _Boolx2 c(and_not(a, b)); RETURN(c.is_false_and_not_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        _Boolx2 a(false); _Boolx2 b(false); _Boolx2 c(and_not(a, b)); RETURN(c.is_false_and_not_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        _Boolx2 a(false); _Boolx2 b(true);  _Boolx2 c(and_not(a, b)); RETURN(c.is_false_and_not_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        _Boolx2 a(true);  _Boolx2 b(_Boolx2::Null()); _Boolx2 c(and_not(a, b)); RETURN(c.is_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        _Boolx2 a(true);  _Boolx2 b(false); _Boolx2 c(and_not(a, b)); RETURN(c.is_true_and_not_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        _Boolx2 a(true);  _Boolx2 b(true);  _Boolx2 c(and_not(a, b)); RETURN(c.is_false_and_not_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        _Boolx2 a(_Boolx2::Null()); Boolx2 b(false); _Boolx2 c(and_not(a, b)); RETURN(c.is_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        _Boolx2 a(_Boolx2::Null()); Boolx2 b(true);  _Boolx2 c(and_not(a, b)); RETURN(c.is_false_and_not_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        _Boolx2 a(false); Boolx2 b(false); _Boolx2 c(and_not(a, b)); RETURN(c.is_false_and_not_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        _Boolx2 a(false); Boolx2 b(true);  _Boolx2 c(and_not(a, b)); RETURN(c.is_false_and_not_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        _Boolx2 a(true);  Boolx2 b(false); _Boolx2 c(and_not(a, b)); RETURN(c.is_true_and_not_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        _Boolx2 a(true);  Boolx2 b(true);  _Boolx2 c(and_not(a, b)); RETURN(c.is_false_and_not_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        Boolx2 a(false); _Boolx2 b(_Boolx2::Null()); _Boolx2 c(and_not(a, b)); RETURN(c.is_false_and_not_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        Boolx2 a(false); _Boolx2 b(false); _Boolx2 c(and_not(a, b)); RETURN(c.is_false_and_not_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        Boolx2 a(false); _Boolx2 b(true);  _Boolx2 c(and_not(a, b)); RETURN(c.is_false_and_not_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        Boolx2 a(true);  _Boolx2 b(_Boolx2::Null()); _Boolx2 c(and_not(a, b)); RETURN(c.is_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        Boolx2 a(true);  _Boolx2 b(false); _Boolx2 c(and_not(a, b)); RETURN(c.is_true_and_not_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, true }), Boolx2(), {
        Boolx2 a(true);  _Boolx2 b(true);  _Boolx2 c(and_not(a, b)); RETURN(c.is_false_and_not_null());
    });

    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(_Boolx1::Null()); _Boolx1 b(_Boolx1::Null()); _Boolx1 c(a and b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(_Boolx1::Null()); _Boolx1 b(false); _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(_Boolx1::Null()); _Boolx1 b(true);  _Boolx1 c(a and b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(false); _Boolx1 b(_Boolx1::Null()); _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(false); _Boolx1 b(false); _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(false); _Boolx1 b(true);  _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(true);  _Boolx1 b(_Boolx1::Null()); _Boolx1 c(a and b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(true);  _Boolx1 b(false); _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(true);  _Boolx1 b(true);  _Boolx1 c(a and b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(_Boolx1::Null()); Boolx1 b(false); _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(_Boolx1::Null()); Boolx1 b(true);  _Boolx1 c(a and b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(false); Boolx1 b(false); _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(false); Boolx1 b(true);  _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(true);  Boolx1 b(false); _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Boolx1 a(true);  Boolx1 b(true);  _Boolx1 c(a and b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { Boolx1 a(false); _Boolx1 b(_Boolx1::Null()); _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { Boolx1 a(false); _Boolx1 b(false); _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { Boolx1 a(false); _Boolx1 b(true);  _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { Boolx1 a(true);  _Boolx1 b(_Boolx1::Null()); _Boolx1 c(a and b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { Boolx1 a(true);  _Boolx1 b(false); _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { Boolx1 a(true);  _Boolx1 b(true);  _Boolx1 c(a and b); RETURN(c.is_true_and_not_null()); });

    /* with conversions */
    CHECK_RESULT_INLINE( true, bool(), { _I8x1 a(_I8x1::Null()); _I32x1 b(a); RETURN(b.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _I32x1 a(_I32x1::Null()); _I8x1 b(a.to<int8_t>()); RETURN(b.is_null()); });

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Variable/operations/unary", "[core][wasm]")
{
    Module::Init();

    CHECK_RESULT_INLINE( 42, int(), { Var<I32x1> a(42); RETURN(+a); });
    CHECK_RESULT_INLINE(-42, int(), { Var<I32x1> a(42); RETURN(-a); });
    // abs() not supported for integral types
    // ceil() not supported for integral types
    // floor() not supported for integral types
    // sqrt() not supported for integral types
    CHECK_RESULT_INLINE(  ~42, int(),      { Var<I32x1> a(42); RETURN(~a); });
    CHECK_RESULT_INLINE(   11, unsigned(), { Var<U32x1> a(0x00101010U); RETURN(a.clz()); });
    CHECK_RESULT_INLINE(    1, unsigned(), { Var<U8x1>  a(0b00101010U); RETURN(a.ctz()); });
    CHECK_RESULT_INLINE(    3, unsigned(), { Var<U8x1>  a(0b00101010U); RETURN(a.popcnt()); });
    CHECK_RESULT_INLINE(false, bool(),     { Var<I32x1> a(42); RETURN(a.eqz()); });
    CHECK_RESULT_INLINE( true, bool(),     { Var<I32x1> a(0);  RETURN(a.eqz()); });
    CHECK_RESULT_INLINE(false, bool(),     { Var<Boolx1> a(true);  RETURN(not a); });
    CHECK_RESULT_INLINE( true, bool(),     { Var<Boolx1> a(false); RETURN(not a); });
    CHECK_RESULT_INLINE( true, bool(),     { Var<_I32x1> a(_I32x1::Null()); RETURN(a.is_null()); });
    CHECK_RESULT_INLINE(false, bool(),     { Var<_I32x1> a(42); RETURN(a.is_null()); });
    CHECK_RESULT_INLINE(false, bool(),     { Var<_I32x1> a(_I32x1::Null()); RETURN(a.not_null()); });
    CHECK_RESULT_INLINE( true, bool(),     { Var<_I32x1> a(42); RETURN(a.not_null()); });
    CHECK_RESULT_INLINE(false, bool(),     { Var<_Boolx1> a(_Boolx1::Null()); RETURN(a.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(),     { Var<_Boolx1> a(true);               RETURN(a.is_true_and_not_null()); });
    CHECK_RESULT_INLINE(false, bool(),     { Var<_Boolx1> a(false);              RETURN(a.is_true_and_not_null()); });
    CHECK_RESULT_INLINE(false, bool(),     { Var<_Boolx1> a(_Boolx1::Null()); RETURN(a.is_false_and_not_null()); });
    CHECK_RESULT_INLINE(false, bool(),     { Var<_Boolx1> a(true);               RETURN(a.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(),     { Var<_Boolx1> a(false);              RETURN(a.is_false_and_not_null()); });

    CHECK_RESULT_INLINE( 3.14, double(), { Var<Doublex1> a( 3.14); RETURN(+a); });
    CHECK_RESULT_INLINE(-3.14, double(), { Var<Doublex1> a( 3.14); RETURN(-a); });
    CHECK_RESULT_INLINE( 3.14, double(), { Var<Doublex1> a( 3.14); RETURN(a.abs()); });
    CHECK_RESULT_INLINE( 3.14, double(), { Var<Doublex1> a(-3.14); RETURN(a.abs()); });
    CHECK_RESULT_INLINE( 4.0,  double(), { Var<Doublex1> a( 3.14); RETURN(a.ceil()); });
    CHECK_RESULT_INLINE( 3.0,  double(), { Var<Doublex1> a( 3.14); RETURN(a.floor()); });
    CHECK_RESULT_INLINE( 3.0,  double(), { Var<Doublex1> a( 9.0);  RETURN(a.sqrt()); });
    // operator~ not supported for floating-point types
    // clz() not supported for floating-point types
    // ctz() not supported for floating-point types
    // popcnt() not supported for floating-point types
    // eqz() not supported for floating-point types
    // operator! not supported for floating-point types
    CHECK_RESULT_INLINE( true, bool(), { Var<_Doublex1> a(_Doublex1::Null()); RETURN(a.is_null()); });
    CHECK_RESULT_INLINE(false, bool(), { Var<_Doublex1> a(3.14);                 RETURN(a.is_null()); });
    CHECK_RESULT_INLINE(false, bool(), { Var<_Doublex1> a(_Doublex1::Null()); RETURN(a.not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { Var<_Doublex1> a(3.14);                 RETURN(a.not_null()); });
    // is_true_and_not_null() not supported for floating-point types
    // is_false_and_not_null() not supported for floating-point types

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Variable/operations/binary", "[core][wasm]")
{
    Module::Init();

#define TEST_OP(RES, RES_TYPE, VAL_A, VAL_A_TYPE, VAL_B, VAL_B_TYPE, OP) \
    CHECK_RESULT_INLINE(RES, RES_TYPE(), { \
        Var<VAL_A_TYPE> a(VAL_A); Var<VAL_B_TYPE> b(VAL_B); RETURN(a OP b); \
    }); \
    CHECK_RESULT_INLINE(RES, RES_TYPE(), { \
        Var<VAL_A_TYPE> a(VAL_A); VAL_B_TYPE b(VAL_B); RETURN(a OP b); \
    }); \
    CHECK_RESULT_INLINE(RES, RES_TYPE(), { \
        VAL_A_TYPE a(VAL_A); Var<VAL_B_TYPE> b(VAL_B); RETURN(a OP b); \
    }); \
    CHECK_RESULT_INLINE(RES, RES_TYPE(), { \
        Var<VAL_A_TYPE> a(VAL_A); RETURN(a OP VAL_B); \
    }); \
    CHECK_RESULT_INLINE(RES, RES_TYPE(), { \
        Var<VAL_B_TYPE> b(VAL_B); RETURN(VAL_A OP b); \
    });
#define TEST_FUNC(RES, RES_TYPE, VAL_A, VAL_A_TYPE, VAL_B, VAL_B_TYPE, FUNC) \
    CHECK_RESULT_INLINE(RES, RES_TYPE(), { \
        Var<VAL_A_TYPE> a(VAL_A); Var<VAL_B_TYPE> b(VAL_B); RETURN(FUNC(a, b)); \
    }); \
    CHECK_RESULT_INLINE(RES, RES_TYPE(), { \
        Var<VAL_A_TYPE> a(VAL_A); VAL_B_TYPE b(VAL_B); RETURN(FUNC(a, b)); \
    }); \
    CHECK_RESULT_INLINE(RES, RES_TYPE(), { \
        VAL_A_TYPE a(VAL_A); Var<VAL_B_TYPE> b(VAL_B); RETURN(FUNC(a, b)); \
    }); \
    CHECK_RESULT_INLINE(RES, RES_TYPE(), { \
        Var<VAL_A_TYPE> a(VAL_A); RETURN(FUNC(a, VAL_B)); \
    }); \
    CHECK_RESULT_INLINE(RES, RES_TYPE(), { \
        Var<VAL_B_TYPE> b(VAL_B); RETURN(FUNC(VAL_A, b)); \
    });
#define TEST_ASSIGN(RES, RES_TYPE, VAL_A, VAL_A_TYPE, VAL_B, VAL_B_TYPE, OP) \
    CHECK_RESULT_INLINE(RES, RES_TYPE(), { \
        Var<VAL_A_TYPE> a(VAL_A); Var<VAL_B_TYPE> b(VAL_B); a OP##= b; RETURN(a); \
    }); \
    CHECK_RESULT_INLINE(RES, RES_TYPE(), { \
        Var<VAL_A_TYPE> a(VAL_A); VAL_B_TYPE b(VAL_B); a OP##= b; RETURN(a); \
    }); \
    CHECK_RESULT_INLINE(RES, RES_TYPE(), { \
        Var<VAL_A_TYPE> a(VAL_A); a OP##= VAL_B_TYPE::type(VAL_B); RETURN(a); \
    });
#define TEST_OP_INT(RES, VALUE_A, VALUE_B, OP) TEST_OP(RES, I32x1, VALUE_A, I32x1, VALUE_B, I32x1, OP)
#define TEST_OP_BOOL(RES, VALUE_A, VALUE_B, OP) TEST_OP(RES, Boolx1, VALUE_A, Boolx1, VALUE_B, Boolx1, OP)
#define TEST_ASSIGN_INT(RES, VALUE_A, VALUE_B, OP) TEST_ASSIGN(RES, I32x1, VALUE_A, I32x1, VALUE_B, I32x1, OP)
#define TEST_OP_INT_TO_BOOL(RES, VALUE_A, VALUE_B, OP) TEST_OP(RES, Boolx1, VALUE_A, I32x1, VALUE_B, I32x1, OP)
#define TEST_OP_DOUBLE(RES, VALUE_A, VALUE_B, OP) TEST_OP(RES, Doublex1, VALUE_A, Doublex1, VALUE_B, Doublex1, OP)
#define TEST_FUNC_DOUBLE(RES, VALUE_A, VALUE_B, FUNC) TEST_FUNC(RES, Doublex1, VALUE_A, Doublex1, VALUE_B, Doublex1, FUNC)
#define TEST_ASSIGN_DOUBLE(RES, VALUE_A, VALUE_B, OP) TEST_ASSIGN(RES, Doublex1, VALUE_A, Doublex1, VALUE_B, Doublex1, OP)
#define TEST_OP_DOUBLE_TO_BOOL(RES, VALUE_A, VALUE_B, OP) TEST_OP(RES, Boolx1, VALUE_A, Doublex1, VALUE_B, Doublex1, OP)

    TEST_OP_INT( 59, 42, 17, +);
    TEST_OP_INT( 25, 42, 17, -);
    TEST_OP_INT(714, 42, 17, *);
    TEST_OP_INT(  2, 42, 17, /);
    TEST_OP_INT(  8, 42, 17, %);
    // copy_sign() not supported for integral types
    // min() not supported for integral types
    // max() not supported for integral types
    TEST_OP(0b00001000, I32x1, 0b00101010, I8x1,  0b00001001, I8x1, &);
    TEST_OP(0b00101011, I32x1, 0b00101010, I8x1,  0b00001001, I8x1, |);
    TEST_OP(0b00100011, I32x1, 0b00101010, I8x1,  0b00001001, I8x1, ^);
    TEST_OP(0b10101000, I32x1, 0b00101010, I8x1,  2, I32x1, <<);
    TEST_OP(0b00001010, I32x1, 0b00101010, I8x1,  2, I32x1, >>);
    TEST_FUNC(0x01010001, I32x1, 0x00101010, I32x1, 12, I32x1, rotl);
    TEST_FUNC(0x01000101, I32x1, 0x00101010, I32x1, 12, I32x1, rotr);
    TEST_OP_INT_TO_BOOL( true, 42, 42, ==);
    TEST_OP_INT_TO_BOOL(false, 42, 17, ==);
    TEST_OP_INT_TO_BOOL(false, 42, 42, !=);
    TEST_OP_INT_TO_BOOL( true, 42, 17, !=);
    TEST_OP_INT_TO_BOOL(false, 42, 42, <);
    TEST_OP_INT_TO_BOOL(false, 42, 17, <);
    TEST_OP_INT_TO_BOOL( true, 17, 42, <);
    TEST_OP_INT_TO_BOOL( true, 42, 42, <=);
    TEST_OP_INT_TO_BOOL(false, 42, 17, <=);
    TEST_OP_INT_TO_BOOL( true, 17, 42, <=);
    TEST_OP_INT_TO_BOOL(false, 42, 42, >);
    TEST_OP_INT_TO_BOOL( true, 42, 17, >);
    TEST_OP_INT_TO_BOOL(false, 17, 42, >);
    TEST_OP_INT_TO_BOOL( true, 42, 42, >=);
    TEST_OP_INT_TO_BOOL( true, 42, 17, >=);
    TEST_OP_INT_TO_BOOL(false, 17, 42, >=);
    TEST_OP_BOOL( true, true,  true,  and);
    TEST_OP_BOOL(false, false, true,  and);
    TEST_OP_BOOL(false, true,  false, and);
    TEST_OP_BOOL(false, false, false, and);
    TEST_OP_BOOL( true, true,  true,  or);
    TEST_OP_BOOL( true, false, true,  or);
    TEST_OP_BOOL( true, true,  false, or);
    TEST_OP_BOOL(false, false, false, or);

    TEST_OP_DOUBLE(Approx(5.85), 3.14, 2.71, +);
    TEST_OP_DOUBLE(Approx(0.43), 3.14, 2.71, -);
    TEST_OP_DOUBLE(Approx(1.57), 3.14, 0.5,  *);
    TEST_OP_DOUBLE(Approx(6.28), 3.14, 0.5,  /);
    // operator% not supported for floating-point types
    TEST_FUNC_DOUBLE( 3.14, 3.14, 1.0,  copy_sign);
    TEST_FUNC_DOUBLE(-3.14, 3.14, -1.0, copy_sign);
    TEST_FUNC_DOUBLE( 2.71, 3.14, 2.71, min);
    TEST_FUNC_DOUBLE( 3.14, 3.14, 2.71, max);
    // operator& not supported for floating-point types
    // operator| not supported for floating-point types
    // operator^ not supported for floating-point types
    // operator<< not supported for floating-point types
    // operator>> not supported for floating-point types
    // rotl() not supported for floating-point types
    // rotr() not supported for floating-point types
    TEST_OP_DOUBLE_TO_BOOL( true, 3.14, 3.14, ==);
    TEST_OP_DOUBLE_TO_BOOL(false, 3.14, 2.71, ==);
    TEST_OP_DOUBLE_TO_BOOL(false, 3.14, 3.14, !=);
    TEST_OP_DOUBLE_TO_BOOL( true, 3.14, 2.71, !=);
    TEST_OP_DOUBLE_TO_BOOL(false, 3.14, 3.14, <);
    TEST_OP_DOUBLE_TO_BOOL(false, 3.14, 2.71, <);
    TEST_OP_DOUBLE_TO_BOOL( true, 2.71, 3.14, <);
    TEST_OP_DOUBLE_TO_BOOL( true, 3.14, 3.14, <=);
    TEST_OP_DOUBLE_TO_BOOL(false, 3.14, 2.71, <=);
    TEST_OP_DOUBLE_TO_BOOL( true, 2.71, 3.14, <=);
    TEST_OP_DOUBLE_TO_BOOL(false, 3.14, 3.14, >);
    TEST_OP_DOUBLE_TO_BOOL( true, 3.14, 2.71, >);
    TEST_OP_DOUBLE_TO_BOOL(false, 2.71, 3.14, >);
    TEST_OP_DOUBLE_TO_BOOL( true, 3.14, 3.14, >=);
    TEST_OP_DOUBLE_TO_BOOL( true, 3.14, 2.71, >=);
    TEST_OP_DOUBLE_TO_BOOL(false, 2.71, 3.14, >=);
    // operator&& not supported for floating-point types
    // operator|| not supported for floating-point types

    TEST_ASSIGN_INT( 59, 42, 17, +);
    TEST_ASSIGN_INT( 25, 42, 17, -);
    TEST_ASSIGN_INT(714, 42, 17, *);
    TEST_ASSIGN_INT(  2, 42, 17, /);
    TEST_ASSIGN_INT(  8, 42, 17, %);
    TEST_ASSIGN(0b00001000U, U32x1, 0b00101010U, U8x1,  0b00001001U, U8x1, &);
    TEST_ASSIGN(0b00101011U, U32x1, 0b00101010U, U8x1,  0b00001001U, U8x1, |);
    TEST_ASSIGN(0b00100011U, U32x1, 0b00101010U, U8x1,  0b00001001U, U8x1, ^);
    TEST_ASSIGN(0b10101000, I32x1, 0b00101010, I8x1,  2, I8x1, <<);
    TEST_ASSIGN(0b00001010, I32x1, 0b00101010, I8x1,  2, I8x1, >>);
    TEST_ASSIGN_DOUBLE(Approx(5.85), 3.14, 2.71, +);
    TEST_ASSIGN_DOUBLE(Approx(0.43), 3.14, 2.71, -);
    TEST_ASSIGN_DOUBLE(Approx(1.57), 3.14, 0.5,  *);
    TEST_ASSIGN_DOUBLE(Approx(6.28), 3.14, 0.5,  /);

#undef TEST_OP_DOUBLE_TO_BOOL
#undef TEST_ASSIGN_DOUBLE
#undef TEST_FUNC_DOUBLE
#undef TEST_OP_DOUBLE
#undef TEST_OP_INT_TO_BOOL
#undef TEST_ASSIGN_INT
#undef TEST_OP_INT
#undef TEST_ASSIGN
#undef TEST_FUNC
#undef TEST_OP

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Variable/vectorial", "[core][wasm]")
{
    Module::Init();

    CHECK_RESULT_INLINE(std::to_array({ false, true }), Boolx2(), {
        Var<Boolx2> a(false, true); RETURN(a);
    });
    CHECK_RESULT_INLINE(std::to_array({ true, false, false, true }), Boolx4(), {
        Var<Boolx2> a(false, true); Var<Boolx4> b(true, false, false, true); a.val().discard(); RETURN(b);
    });
    CHECK_RESULT_INLINE(std::to_array({ true, false, false, true }), Boolx4(), {
        Var<Boolx16> a(true); Var<Boolx4> b(true, false, false, true); a.val().discard(); RETURN(b);
    });
    CHECK_RESULT_INLINE(std::to_array({ true, false, false, true }), Boolx4(), {
        Var<Boolx32> a(true); Var<Boolx4> b(true, false, false, true); a.val().discard(); RETURN(b);
    });

    CHECK_RESULT_INLINE(std::to_array({ true, false }), Boolx2(), {
        _Var<I32x2> a(I32x2(42, -17), Boolx2(true, false)); RETURN(a.is_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, false }), Boolx2(), {
        Var<Boolx2> a(false, true); _Var<I32x2> b(I32x2(42, -17), Boolx2(true, false)); a.val().discard(); RETURN(b.is_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, false }), Boolx2(), {
        Var<Boolx16> a(true); _Var<I32x2> b(I32x2(42, -17), Boolx2(true, false)); a.val().discard(); RETURN(b.is_null());
    });
    CHECK_RESULT_INLINE(std::to_array({ true, false }), Boolx2(), {
        Var<Boolx32> a(true); _Var<I32x2> b(I32x2(42, -17), Boolx2(true, false)); a.val().discard(); RETURN(b.is_null());
    });

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Variable/null_semantics", "[core][wasm]")
{
    Module::Init();

    /* with unary operations */
    CHECK_RESULT_INLINE(true, bool(), {
        _Var<I32x1> a(_I32x1::Null()); _Var<I32x1> b(-a); /* b must allocate new NULL bit */
        RETURN(b.is_null());
    });
    CHECK_RESULT_INLINE(false, bool(), {
        _Var<I32x1> a(_I32x1::Null()); _Var<I32x1> b(-a); /* b must allocate new NULL bit */ a = 42;
        b.val().discard(); /* artificial use of `b` to silence diagnostics */
        RETURN(a.is_null());
    });
    CHECK_RESULT_INLINE(true, bool(), {
        _Var<I32x1> a(_I32x1::Null()); _Var<I32x1> b(-a); /* b must allocate new NULL bit */ a = 42;
        RETURN(b.is_null());
    });
    CHECK_RESULT_INLINE(true, bool(), {
        _Var<I32x1> a(_I32x1::Null()); _Var<I32x1> b(-a); /* b must allocate new NULL bit */ b = 17;
        b.val().discard(); /* artificial use of `b` to silence diagnostics */
        RETURN(a.is_null());
    });
    CHECK_RESULT_INLINE(false, bool(), {
        _Var<I32x1> a(_I32x1::Null()); _Var<I32x1> b(-a); /* b must allocate new NULL bit */ b = 17;
        RETURN(b.is_null());
    });

    /* with binary operations */
    CHECK_RESULT_INLINE( true, bool(), {
        _Var<I32x1> a(_I32x1::Null()); _Var<I32x1> b(_I32x1::Null()); _Var<I32x1> c(a + b);
        /* a, b, and c must allocate their own NULL bit */
        RETURN(c.is_null());
    });
    CHECK_RESULT_INLINE( true, bool(), {
        _Var<I32x1> a(_I32x1::Null()); Var<I32x1> b(17); _Var<I32x1> c(a + b);
        /* a and c must allocate their own NULL bit */
        RETURN(c.is_null());
    });
    CHECK_RESULT_INLINE( true, bool(), {
        Var<I32x1> a(42); _Var<I32x1> b(_I32x1::Null()); _Var<I32x1> c(a + b);
        /* b and c must allocate their own NULL bit */
        RETURN(c.is_null());
    });

    CHECK_RESULT_INLINE(false, bool(), {
        _Var<I32x1> a(_I32x1::Null()); _Var<I32x1> b(_I32x1::Null()); _Var<I32x1> c(a + b); a = 42;
        /* a, b, and c must allocate their own NULL bit */
        c.val().discard(); /* artificial use of `c` to silence diagnostics */
        RETURN(a.is_null());
    });
    CHECK_RESULT_INLINE( true, bool(), {
        _Var<I32x1> a(_I32x1::Null()); _Var<I32x1> b(_I32x1::Null()); _Var<I32x1> c(a + b); a = 42;
        /* a, b, and c must allocate their own NULL bit */
        c.val().discard(); /* artificial use of `c` to silence diagnostics */
        RETURN(b.is_null());
    });
    CHECK_RESULT_INLINE( true, bool(), {
        _Var<I32x1> a(_I32x1::Null()); _Var<I32x1> b(_I32x1::Null()); _Var<I32x1> c(a + b); a = 42;
        /* a, b, and c must allocate their own NULL bit */
        RETURN(c.is_null());
    });
    CHECK_RESULT_INLINE( true, bool(), {
        _Var<I32x1> a(_I32x1::Null()); _Var<I32x1> b(_I32x1::Null()); _Var<I32x1> c(a + b); b = 17;
        /* a, b, and c must allocate their own NULL bit */
        c.val().discard(); /* artificial use of `c` to silence diagnostics */
        RETURN(a.is_null());
    });
    CHECK_RESULT_INLINE(false, bool(), {
        _Var<I32x1> a(_I32x1::Null()); _Var<I32x1> b(_I32x1::Null()); _Var<I32x1> c(a + b); b = 17;
        /* a, b, and c must allocate their own NULL bit */
        c.val().discard(); /* artificial use of `c` to silence diagnostics */
        RETURN(b.is_null());
    });
    CHECK_RESULT_INLINE( true, bool(), {
        _Var<I32x1> a(_I32x1::Null()); _Var<I32x1> b(_I32x1::Null()); _Var<I32x1> c(a + b); b = 17;
        /* a, b, and c must allocate their own NULL bit */
        RETURN(c.is_null());
    });
    CHECK_RESULT_INLINE( true, bool(), {
        _Var<I32x1> a(_I32x1::Null()); _Var<I32x1> b(_I32x1::Null()); _Var<I32x1> c(a + b); c = 99;
        /* a, b, and c must allocate their own NULL bit */
        c.val().discard(); /* artificial use of `c` to silence diagnostics */
        RETURN(a.is_null());
    });
    CHECK_RESULT_INLINE( true, bool(), {
        _Var<I32x1> a(_I32x1::Null()); _Var<I32x1> b(_I32x1::Null()); _Var<I32x1> c(a + b); c = 99;
        /* a, b, and c must allocate their own NULL bit */
        c.val().discard(); /* artificial use of `c` to silence diagnostics */
        RETURN(b.is_null());
    });
    CHECK_RESULT_INLINE(false, bool(), {
        _Var<I32x1> a(_I32x1::Null()); _Var<I32x1> b(_I32x1::Null()); _Var<I32x1> c(a + b); c = 99;
        /* a, b, and c must allocate their own NULL bit */
        RETURN(c.is_null());
    });

    /* special cases with logical `or` and `and` */
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(_Boolx1::Null()); _Var<Boolx1> b(_Boolx1::Null()); _Boolx1 c(a or b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(_Boolx1::Null()); _Var<Boolx1> b(false); _Boolx1 c(a or b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(_Boolx1::Null()); _Var<Boolx1> b(true);  _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(false); _Var<Boolx1> b(_Boolx1::Null()); _Boolx1 c(a or b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(true);  _Var<Boolx1> b(_Boolx1::Null()); _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(false); _Var<Boolx1> b(false); _Boolx1 c(a or b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(false); _Var<Boolx1> b(true);  _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(true);  _Var<Boolx1> b(false); _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(true);  _Var<Boolx1> b(true);  _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });

    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(_Boolx1::Null()); _Var<Boolx1> b(false); _Boolx1 c(a or b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(_Boolx1::Null()); _Var<Boolx1> b(true);  _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(false); _Var<Boolx1> b(false); _Boolx1 c(a or b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(false); _Var<Boolx1> b(true);  _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(true);  _Var<Boolx1> b(false); _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(true);  _Var<Boolx1> b(true);  _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });

    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(false); _Var<Boolx1> b(_Boolx1::Null()); _Boolx1 c(a or b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(true);  _Var<Boolx1> b(_Boolx1::Null()); _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(false); _Var<Boolx1> b(false); _Boolx1 c(a or b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(false); _Var<Boolx1> b(true);  _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(true);  _Var<Boolx1> b(false); _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(true);  _Var<Boolx1> b(true);  _Boolx1 c(a or b); RETURN(c.is_true_and_not_null()); });

    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(_Boolx1::Null()); _Var<Boolx1> b(_Boolx1::Null()); _Boolx1 c(a and b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(_Boolx1::Null()); _Var<Boolx1> b(false); _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(_Boolx1::Null()); _Var<Boolx1> b(true);  _Boolx1 c(a and b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(false); _Var<Boolx1> b(_Boolx1::Null()); _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(true);  _Var<Boolx1> b(_Boolx1::Null()); _Boolx1 c(a and b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(false); _Var<Boolx1> b(false); _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(false); _Var<Boolx1> b(true);  _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(true);  _Var<Boolx1> b(false); _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(true);  _Var<Boolx1> b(true);  _Boolx1 c(a and b); RETURN(c.is_true_and_not_null()); });

    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(_Boolx1::Null()); _Var<Boolx1> b(false); _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(_Boolx1::Null()); _Var<Boolx1> b(true);  _Boolx1 c(a and b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(false); _Var<Boolx1> b(false); _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(false); _Var<Boolx1> b(true);  _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(true);  _Var<Boolx1> b(false); _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(true);  _Var<Boolx1> b(true);  _Boolx1 c(a and b); RETURN(c.is_true_and_not_null()); });

    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(false); _Var<Boolx1> b(_Boolx1::Null()); _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(true);  _Var<Boolx1> b(_Boolx1::Null()); _Boolx1 c(a and b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(false); _Var<Boolx1> b(false); _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(false); _Var<Boolx1> b(true);  _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(true);  _Var<Boolx1> b(false); _Boolx1 c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Boolx1> a(true);  _Var<Boolx1> b(true);  _Boolx1 c(a and b); RETURN(c.is_true_and_not_null()); });

    /* with NULL bit reuse */
    CHECK_RESULT_INLINE(false, bool(), {
        _Var<I32x1> a(_I32x1::Null()); // to make a nullable
        a = 1; /* a's NULL bit is updated but no new NULL bit needed */
        RETURN(a.is_null());
    });
    CHECK_RESULT_INLINE(true, bool(), {
        _Var<I32x1> a(_I32x1::Null()); { _Var<I32x1> b(_I32x1::Null()); _I32x1 c(a + b); c.discard(); }
        _Var<I32x1> d(_I32x1::Null()); /* d should reuse b's NULL bit (i.e. offset 1) */ RETURN(d.is_null());
    });

    /* with conversions */
    CHECK_RESULT_INLINE( true, bool(), { _Var<I8x1> a(_I8x1::Null()); _Var<I32x1> b(a.val()); RETURN(b.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<I32x1> a(_I32x1::Null()); _Var<I8x1> b(a.to<int8_t>()); RETURN(b.is_null());
    });

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Select", "[core][wasm]")
{
    Module::Init();

    CHECK_RESULT_INLINE(0, int(), { Boolx1 cond(true);  Var<I32x1> a(0); Var<I32x1> b(1); RETURN(Select(cond, a, b)); });
    CHECK_RESULT_INLINE(1, int(), { Boolx1 cond(false); Var<I32x1> a(0); Var<I32x1> b(1); RETURN(Select(cond, a, b)); });
    CHECK_RESULT_INLINE(0, int(), { Boolx1 cond(true);  Var<I32x1> a(0); I32x1 b(1); RETURN(Select(cond, a, b)); });
    CHECK_RESULT_INLINE(1, int(), { Boolx1 cond(false); Var<I32x1> a(0); I32x1 b(1); RETURN(Select(cond, a, b)); });
    CHECK_RESULT_INLINE(0, int(), { Boolx1 cond(true);  I32x1 a(0); Var<I32x1> b(1); RETURN(Select(cond, a, b)); });
    CHECK_RESULT_INLINE(1, int(), { Boolx1 cond(false); I32x1 a(0); Var<I32x1> b(1); RETURN(Select(cond, a, b)); });
    CHECK_RESULT_INLINE(0, int(), { Boolx1 cond(true);  I32x1 a(0); I32x1 b(1); RETURN(Select(cond, a, b)); });
    CHECK_RESULT_INLINE(1, int(), { Boolx1 cond(false); I32x1 a(0); I32x1 b(1); RETURN(Select(cond, a, b)); });
    CHECK_RESULT_INLINE(0, int(), { Boolx1 cond(true);  Var<I32x1> a(0); RETURN(Select(cond, a, 1)); });
    CHECK_RESULT_INLINE(1, int(), { Boolx1 cond(false); Var<I32x1> a(0); RETURN(Select(cond, a, 1)); });
    CHECK_RESULT_INLINE(0, int(), { Boolx1 cond(true);  Var<I32x1> b(1); RETURN(Select(cond, 0, b)); });
    CHECK_RESULT_INLINE(1, int(), { Boolx1 cond(false); Var<I32x1> b(1); RETURN(Select(cond, 0, b)); });
    CHECK_RESULT_INLINE(0, int(), { Boolx1 cond(true);  I32x1 a(0); RETURN(Select(cond, a, 1)); });
    CHECK_RESULT_INLINE(1, int(), { Boolx1 cond(false); I32x1 a(0); RETURN(Select(cond, a, 1)); });
    CHECK_RESULT_INLINE(0, int(), { Boolx1 cond(true);  I32x1 b(1); RETURN(Select(cond, 0, b)); });
    CHECK_RESULT_INLINE(1, int(), { Boolx1 cond(false); I32x1 b(1); RETURN(Select(cond, 0, b)); });

    CHECK_RESULT_INLINE(std::to_array({ 0, 1 }), I32x2(), {
        Boolx1 cond(true);  I32x2 a(0, 1); I32x2 b(2, 3); RETURN(Select(cond, a, b));
    });
    CHECK_RESULT_INLINE(std::to_array({ 2, 3 }), I32x2(), {
        Boolx1 cond(false); I32x2 a(0, 1); I32x2 b(2, 3); RETURN(Select(cond, a, b));
    });
    CHECK_RESULT_INLINE(std::to_array<int32_t>({ 0x01234567, int32_t(0x9abcdef0) }), I32x2(), {
        Boolx2 cond(true, false); I32x2 a(0x01234567, 0x89abcdef); I32x2 b(0x12345678, 0x9abcdef0); RETURN(Select(cond, a, b));
    });
    CHECK_RESULT_INLINE(std::to_array<int64_t>({ 0x01234567, int64_t(0x9abcdef0) }), I64x2(), {
        Boolx4 cond(true, false, false, true);
        I64x4 a(0x01234567, 0x89abcdef, 0x76543210, 0xfedcba98);
        I64x4 b(0x12345678, 0x9abcdef0, 0x87654321, 0x0fedcba9);
        RETURN(Select(cond, a, b).swizzle_lanes(std::to_array<uint8_t>({ 0, 1 }))); // lower part
    });
    CHECK_RESULT_INLINE(std::to_array<int64_t>({ int64_t(0x87654321), int64_t(0xfedcba98) }), I64x2(), {
        Boolx4 cond(true, false, false, true);
        I64x4 a(0x01234567, 0x89abcdef, 0x76543210, 0xfedcba98);
        I64x4 b(0x12345678, 0x9abcdef0, 0x87654321, 0x0fedcba9);
        RETURN(Select(cond, a, b).swizzle_lanes(std::to_array<uint8_t>({ 2, 3 }))); // higher part
    });

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/If", "[core][wasm]")
{
    Module::Init();

    /* if-then */
    CHECK_RESULT_INLINE(1, int(bool), {
        IF (PARAMETER(0)) { RETURN(1); };
        RETURN(0);
    }, true);
    CHECK_RESULT_INLINE(0, int(bool), {
        IF (PARAMETER(0)) { RETURN(1); };
        RETURN(0);
    }, false);

    CHECK_RESULT_INLINE(1, int(int), {
        IF (PARAMETER(0) == 17) { RETURN(1); };
        RETURN(0);
    }, 17);
    CHECK_RESULT_INLINE(0, int(int), {
        IF (PARAMETER(0) == 17) { RETURN(1); };
        RETURN(0);
    }, 42);

    CHECK_RESULT_INLINE(1, int(unsigned), {
        IF (PARAMETER(0).to<bool>()) { RETURN(1); };
        RETURN(0);
    }, 1);
    CHECK_RESULT_INLINE(0, int(unsigned), {
        IF (PARAMETER(0).to<bool>()) { RETURN(1); };
        RETURN(0);
    }, 0);

    /* if-then-else */
    CHECK_RESULT_INLINE(1, int(bool), {
        IF (PARAMETER(0)) { RETURN(1); } ELSE { RETURN(2); };
    }, true);
    CHECK_RESULT_INLINE(2, int(bool), {
        IF (PARAMETER(0)) { RETURN(1); } ELSE { RETURN(2); };
    }, false);

    CHECK_RESULT_INLINE(1, int(int), {
        IF (PARAMETER(0) == 17) { RETURN(1); } ELSE { RETURN(2); };
    }, 17);
    CHECK_RESULT_INLINE(2, int(int), {
        IF (PARAMETER(0) == 17) { RETURN(1); } ELSE { RETURN(2); };
    }, 42);

    CHECK_RESULT_INLINE(1, int(unsigned), {
        IF (PARAMETER(0).to<bool>()) { RETURN(1); } ELSE { RETURN(2); };
    }, 1);
    CHECK_RESULT_INLINE(2, int(unsigned), {
        IF (PARAMETER(0).to<bool>()) { RETURN(1); } ELSE { RETURN(2); };
    }, 0);

    /* side-effects */
    CHECK_RESULT_INLINE(1, int(), {
        Var<I32x1> res(0);
        Var<U32x1> test(2);
        IF ((test = 1U).to<bool>()) { res = 1; } ELSE { res = 2; };
        RETURN(res);
    });
    CHECK_RESULT_INLINE(1, unsigned(), {
        Var<I32x1> res(0);
        Var<U32x1> test(2);
        IF ((test = 1U).to<bool>()) { res = 1; } ELSE { res = 2; };
        res.val().discard(); /* artificial use of `res` to silence diagnostics */
        RETURN(test);
    });

    CHECK_RESULT_INLINE(2, int(), {
        Var<I32x1> res(0);
        Var<U32x1> test(2);
        IF ((test = 0U).to<bool>()) { res = 1; } ELSE { res = 2; };
        RETURN(res);
    });
    CHECK_RESULT_INLINE(0, unsigned(), {
        Var<I32x1> res(0);
        Var<U32x1> test(2);
        IF ((test = 0U).to<bool>()) { res = 1; } ELSE { res = 2; };
        res.val().discard(); /* artificial use of `res` to silence diagnostics */
        RETURN(test);
    });

    /* nested if */
#define MAX(RES, VALUE_A, VALUE_B, VALUE_C) \
    CHECK_RESULT_INLINE(RES, int(int, int, int), { \
        const Parameter<int> &x = PARAMETER(0); \
        const Parameter<int> &y = PARAMETER(1); \
        const Parameter<int> &z = PARAMETER(2); \
        IF (x > y) { \
            IF (x > z) { \
                RETURN(x); \
            } ELSE { \
                RETURN(z); \
            }; \
        } ELSE { \
            IF (y > z) { \
                RETURN(y); \
            } ELSE { \
                RETURN(z); \
            }; \
        }; \
    }, VALUE_A, VALUE_B, VALUE_C);

    MAX(314, 17, 42, 314);
    MAX(314, 17, 314, 42);
    MAX(314, 314, 17, 42);

#undef MAX

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Loop/Loop", "[core][wasm]")
{
    Module::Init();

    /* zero iterations */
    CHECK_RESULT_INLINE(0, int(), {
        Var<I32x1> res(0);
        LOOP() {
            BREAK();
            res += 1;
            CONTINUE();
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(0, int(), {
        Var<I32x1> res(0);
        LOOP() {
            BREAK(res == 0);
            res += 1;
            CONTINUE();
        }
        RETURN(res);
    });

    /* one iteration */
    CHECK_RESULT_INLINE(1, int(), {
        Var<I32x1> res(0);
        LOOP() {
            BREAK(res == 1);
            res += 1;
            CONTINUE();
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(1, int(), {
        Var<I32x1> res(0);
        LOOP() {
            res += 1;
            BREAK(res == 1);
            CONTINUE();
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(1, int(), {
        Var<I32x1> res(0);
        LOOP() {
            res += 1;
        }
        RETURN(res);
    });

    /* multiple iterations */
    CHECK_RESULT_INLINE(2, int(), {
        Var<I32x1> res(0);
        LOOP() {
            BREAK(res == 2);
            res += 1;
            CONTINUE();
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(2, int(), {
        Var<I32x1> res(0);
        LOOP() {
            res += 1;
            BREAK(res == 2);
            CONTINUE();
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(42, int(), {
        Var<I32x1> res(0);
        LOOP() {
            BREAK(res == 42);
            res += 1;
            CONTINUE();
        }
        RETURN(res);
    });

    /* continue */
    CHECK_RESULT_INLINE(0, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        LOOP() {
            BREAK(i == 10);
            i += 1;
            CONTINUE();
            res += 1;
            CONTINUE();
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(5, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        LOOP() {
            BREAK(i == 10);
            i += 1;
            CONTINUE(i % 2 == 0);
            res += 1;
            CONTINUE();
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(10, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        LOOP() {
            BREAK(i == 10);
            i += 1;
            CONTINUE(i > 10);
            res += 1;
            CONTINUE();
        }
        RETURN(res);
    });

    /* nested loop */
    CHECK_RESULT_INLINE(15, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        LOOP() {
            BREAK(i == 3);
            Var<I32x1> j(0);
            LOOP() {
                BREAK(j == 5);
                res += 1;
                j += 1;
                CONTINUE();
            }
            i += 1;
            CONTINUE();
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(5, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        LOOP() {
            BREAK(i == 3);
            Var<I32x1> j(0);
            LOOP() {
                BREAK(j == 5, 2);
                res += 1;
                j += 1;
                CONTINUE();
            }
            i += 1;
            CONTINUE();
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(3, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        LOOP() {                // <-- CONTINUE(2)
            BREAK(i == 3);
            i += 1;
            Var<I32x1> j(0);
            LOOP() {
                BREAK(j == 5);
                res += 1;
                j += 1;
                CONTINUE(2);    // 2 levels above
            }
            CONTINUE();
        }
        RETURN(res);
    });

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Loop/Do_While", "[core][wasm]")
{
    Module::Init();

    /* one iteration */
    CHECK_RESULT_INLINE(1, int(), {
        Var<I32x1> res(0);
        DO_WHILE(res < 1) {
            res += 1;
        }
        RETURN(res);
    });

    /* multiple iterations */
    CHECK_RESULT_INLINE(2, int(), {
        Var<I32x1> res(0);
        DO_WHILE(res < 2) {
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(42, int(), {
        Var<I32x1> res(0);
        DO_WHILE(res < 42) {
            res += 1;
        }
        RETURN(res);
    });

    /* break */
    CHECK_RESULT_INLINE(0, int(), {
        Var<I32x1> res(0);
        DO_WHILE(res < 17) {
            BREAK();
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(1, int(), {
        Var<I32x1> res(0);
        DO_WHILE(res < 17) {
            res += 1;
            BREAK();
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(5, int(), {
        Var<I32x1> res(0);
        DO_WHILE(res < 17) {
            BREAK(res == 5);
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(5, int(), {
        Var<I32x1> res(0);
        DO_WHILE(res < 17) {
            res += 1;
            BREAK(res == 5);
        }
        RETURN(res);
    });

    /* continue */
    CHECK_RESULT_INLINE(0, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        DO_WHILE(i != 10) {
            i += 1;
            CONTINUE();
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(5, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        DO_WHILE(i != 10) {
            i += 1;
            CONTINUE(i % 2 == 0);
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(10, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        DO_WHILE(i != 10) {
            i += 1;
            CONTINUE(i > 10);
            res += 1;
        }
        RETURN(res);
    });

    /* nested do while */
    CHECK_RESULT_INLINE(15, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        DO_WHILE(i != 3) {
            Var<I32x1> j(0);
            DO_WHILE(j != 5) {
                res += 1;
                j += 1;
            }
            i += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(12, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        DO_WHILE(i != 3) {
            Var<I32x1> j(0);
            DO_WHILE(j != 5) {
                BREAK(j == 4);
                res += 1;
                j += 1;
            }
            i += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(4, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        DO_WHILE(i != 3) {
            Var<I32x1> j(0);
            DO_WHILE(j != 5) {
                BREAK(j == 4, 2);
                res += 1;
                j += 1;
            }
            i += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(6, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        DO_WHILE(i != 3) {
            Var<I32x1> j(0);
            DO_WHILE(j != 5) {
                j += 1;
                CONTINUE(j > 2);
                res += 1;
            }
            i += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(6, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        DO_WHILE(i != 3) {
            i += 1;
            Var<I32x1> j(0);
            DO_WHILE(j != 5) {
                j += 1;
                CONTINUE(j > 2, 2);
                res += 1;
            }
        }
        RETURN(res);
    });

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Loop/While", "[core][wasm]")
{
    Module::Init();

    /* zero iterations */
    CHECK_RESULT_INLINE(0, int(), {
        Var<I32x1> res(0);
        WHILE(res < 0) {
            res += 1;
        }
        RETURN(res);
    });

    /* one iteration */
    CHECK_RESULT_INLINE(1, int(), {
        Var<I32x1> res(0);
        WHILE(res < 1) {
            res += 1;
        }
        RETURN(res);
    });

    /* multiple iterations */
    CHECK_RESULT_INLINE(2, int(), {
        Var<I32x1> res(0);
        WHILE(res < 2) {
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(42, int(), {
        Var<I32x1> res(0);
        WHILE(res < 42) {
            res += 1;
        }
        RETURN(res);
    });

    /* break */
    CHECK_RESULT_INLINE(0, int(), {
        Var<I32x1> res(0);
        WHILE(res < 17) {
            BREAK();
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(1, int(), {
        Var<I32x1> res(0);
        WHILE(res < 17) {
            res += 1;
            BREAK();
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(5, int(), {
        Var<I32x1> res(0);
        WHILE(res < 17) {
            BREAK(res == 5);
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(5, int(), {
        Var<I32x1> res(0);
        WHILE(res < 17) {
            res += 1;
            BREAK(res == 5);
        }
        RETURN(res);
    });

    /* continue */
    CHECK_RESULT_INLINE(0, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        WHILE(i != 10) {
            i += 1;
            CONTINUE();
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(5, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        WHILE(i != 10) {
            i += 1;
            CONTINUE(i % 2 == 0);
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(10, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        WHILE(i != 10) {
            i += 1;
            CONTINUE(i > 10);
            res += 1;
        }
        RETURN(res);
    });

    /* nested while */
    CHECK_RESULT_INLINE(15, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        WHILE(i != 3) {
            Var<I32x1> j(0);
            WHILE(j != 5) {
                res += 1;
                j += 1;
            }
            i += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(12, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        WHILE(i != 3) {
            Var<I32x1> j(0);
            WHILE(j != 5) {
                BREAK(j == 4);
                res += 1;
                j += 1;
            }
            i += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(4, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        WHILE(i != 3) {
            Var<I32x1> j(0);
            WHILE(j != 5) {
                BREAK(j == 4, 2);
                res += 1;
                j += 1;
            }
            i += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(6, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        WHILE(i != 3) {
            Var<I32x1> j(0);
            WHILE(j != 5) {
                j += 1;
                CONTINUE(j > 2);
                res += 1;
            }
            i += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(6, int(), {
        Var<I32x1> res(0);
        Var<I32x1> i(0);
        WHILE(i != 3) {
            i += 1;
            Var<I32x1> j(0);
            WHILE(j != 5) {
                j += 1;
                CONTINUE(j > 2, 2);
                res += 1;
            }
        }
        RETURN(res);
    });

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Pointer/operators", "[core][wasm]")
{
    Module::Init();

    /* addition/subtraction */
    CHECK_RESULT_INLINE( true, bool(), { Var<Ptr<I32x1>> a(Module::Allocator().malloc<int>()); RETURN((a + 1) - 1 == a); });
    CHECK_RESULT_INLINE( true, bool(), { Var<Ptr<I32x1>> a(Module::Allocator().malloc<int>()); RETURN((a + 2) - 1 > a); });

    /* pointer difference */
    CHECK_RESULT_INLINE(0, int32_t(), { Var<Ptr<I32x1>> a(Module::Allocator().malloc<int>()); RETURN(a - a); });
    CHECK_RESULT_INLINE(4, int32_t(), { Var<Ptr<I32x1>> a(Module::Allocator().malloc<int>()); RETURN((a + 4) - a); });

    /* comparison */
    CHECK_RESULT_INLINE( true, bool(), { Var<Ptr<I32x1>> a(Module::Allocator().malloc<int>()); RETURN(a == a); });
    CHECK_RESULT_INLINE(false, bool(), { Var<Ptr<I32x1>> a(Module::Allocator().malloc<int>()); RETURN(a + 1 == a); });
    CHECK_RESULT_INLINE(false, bool(), { Var<Ptr<I32x1>> a(Module::Allocator().malloc<int>()); RETURN(a != a); });
    CHECK_RESULT_INLINE( true, bool(), { Var<Ptr<I32x1>> a(Module::Allocator().malloc<int>()); RETURN(a + 1 != a); });
    CHECK_RESULT_INLINE(false, bool(), { Var<Ptr<I32x1>> a(Module::Allocator().malloc<int>()); RETURN(a < a); });
    CHECK_RESULT_INLINE(false, bool(), { Var<Ptr<I32x1>> a(Module::Allocator().malloc<int>()); RETURN(a + 1 < a); });
    CHECK_RESULT_INLINE( true, bool(), { Var<Ptr<I32x1>> a(Module::Allocator().malloc<int>()); RETURN(a < a + 1); });
    CHECK_RESULT_INLINE( true, bool(), { Var<Ptr<I32x1>> a(Module::Allocator().malloc<int>()); RETURN(a <= a); });
    CHECK_RESULT_INLINE(false, bool(), { Var<Ptr<I32x1>> a(Module::Allocator().malloc<int>()); RETURN(a + 1 <= a); });
    CHECK_RESULT_INLINE( true, bool(), { Var<Ptr<I32x1>> a(Module::Allocator().malloc<int>()); RETURN(a <= a + 1); });
    CHECK_RESULT_INLINE(false, bool(), { Var<Ptr<I32x1>> a(Module::Allocator().malloc<int>()); RETURN(a > a); });
    CHECK_RESULT_INLINE( true, bool(), { Var<Ptr<I32x1>> a(Module::Allocator().malloc<int>()); RETURN(a + 1 > a); });
    CHECK_RESULT_INLINE(false, bool(), { Var<Ptr<I32x1>> a(Module::Allocator().malloc<int>()); RETURN(a > a + 1); });
    CHECK_RESULT_INLINE( true, bool(), { Var<Ptr<I32x1>> a(Module::Allocator().malloc<int>()); RETURN(a >= a); });
    CHECK_RESULT_INLINE( true, bool(), { Var<Ptr<I32x1>> a(Module::Allocator().malloc<int>()); RETURN(a + 1 >= a); });
    CHECK_RESULT_INLINE(false, bool(), { Var<Ptr<I32x1>> a(Module::Allocator().malloc<int>()); RETURN(a >= a + 1); });

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Memory", "[core][wasm]")
{
    Module::Init();

    /* malloc<T>() */
    CHECK_RESULT_INLINE(3, char(void), {
        Var<Ptr<Charx1>> ptr(Module::Allocator().malloc<char>());
        *ptr = char(3);
        RETURN(*ptr);
    });
    CHECK_RESULT_INLINE(42, int(void), {
        Var<Ptr<I32x1>> ptr(Module::Allocator().malloc<int>());
        *ptr = 42;
        RETURN(*ptr);
    });
    CHECK_RESULT_INLINE(3.14, double(void), {
        Var<Ptr<Doublex1>> ptr(Module::Allocator().malloc<double>());
        *ptr = 3.14;
        RETURN(*ptr);
    });
    CHECK_RESULT_INLINE(std::to_array({ 42, 17, -123, 314 }), I32x4(void), {
        Var<Ptr<I32x4>> ptr(Module::Allocator().malloc<int, 4>());
        *ptr = I32x4(42, 17, -123, 314);
        RETURN(*ptr);
    });

    /* malloc<T>(length) */
    CHECK_RESULT_INLINE(-29, char(void), {
        Var<Ptr<Charx1>> ptr(Module::Allocator().malloc<char>(5U));
        *ptr = char(42);
        *(ptr + 1) = char(17);
        *(ptr + 2) = char(123);
        *(ptr + 3) = char(-29);
        *(ptr + 4) = char(3);
        RETURN(*(ptr + 3));
    });
    CHECK_RESULT_INLINE(123, int(void), {
        Var<Ptr<I32x1>> ptr(Module::Allocator().malloc<int>(3U));
        *ptr = 42;
        *(ptr + 1) = 17;
        *(ptr + 2) = 123;
        RETURN(*(ptr + 2));
    });

    /* allocate(bytes) */
    CHECK_RESULT_INLINE(0x78561234, uint32_t(void), {
        Var<Ptr<void>> ptr(Module::Allocator().allocate(4U, 4U)); // aligned alloc
        *ptr.to<uint16_t*>() = uint16_t(0x1234); // bytes 0 and 1
        *(ptr.to<uint8_t*>() + 2) = uint8_t(0x56); // byte 2
        *(ptr.to<uint8_t*>() + 3) = uint8_t(0x78); // byte 3
        RETURN(*ptr.to<uint32_t*>()); // bytes 0 to 3
    });

    /* free() */
    CHECK_RESULT_INLINE(true, bool(void), {
        Module::Allocator().malloc<uint16_t>().val().discard(); // to align to 2 bytes
        Var<Ptr<U8x1>> ptr1(Module::Allocator().malloc<uint8_t>()); // byte 0
        *ptr1 = uint8_t(0x12);
        Module::Allocator().free(ptr1);
        Var<Ptr<U16x1>> ptr2(Module::Allocator().malloc<uint16_t>()); // reuses byte 0 and additionally uses byte 1
        RETURN(ptr1.to<uint32_t>() == ptr2.to<uint32_t>());
    });

    /* returning pointer */
    CHECK_RESULT_INLINE(42, int(void), {
        FUNCTION(alloc, int*(unsigned))
        {
            RETURN(Module::Allocator().malloc<int>(PARAMETER(0)));
        }
        Var<Ptr<I32x1>> ptr(alloc(1U));
        *ptr = 42;
        RETURN(*ptr);
    });

    /* passing pointer */
    CHECK_RESULT_INLINE(42, int(void), {
        FUNCTION(assign, void(int*))
        {
            *PARAMETER(0) = 42;
        }
        Var<Ptr<I32x1>> ptr(Module::Allocator().malloc<int>());
        assign(ptr);
        RETURN(*ptr);
    });

    Module::Dispose();
}

#if 0
TEST_CASE("Wasm/" BACKEND_NAME "/Wasm_insist", "[core][wasm]")
{
    Module::Init();

    std::ostringstream oss;
    auto old = std::cerr.rdbuf(oss.rdbuf());

    SECTION("return NULL")
    {
        REQUIRE(oss.str().empty());
        INVOKE_INLINE(void(void), {
            FUNCTION(f, int(void))
            {
                RETURN(_I32x1::Null());
            }
            f().discard();
        });
        CHECK(not oss.str().empty());
    }

    SECTION("return nullable")
    {
        REQUIRE(oss.str().empty());
        INVOKE_INLINE(void(void), {
            FUNCTION(f, int(void))
            {
                _Var<I32x1> res(_I32x1::Null());
                res = 1;
                RETURN(res);
            }
            f().discard();
        });
        CHECK(oss.str().empty());
    }

    SECTION("argument NULL")
    {
        REQUIRE(oss.str().empty());

        SECTION("void return, single argument")
        {
            INVOKE_INLINE(void(void), {
                FUNCTION(f, void(int)) { }
                f(_I32x1::Null());
            });
        }
        SECTION("void return, multiple arguments")
        {
            INVOKE_INLINE(void(void), {
                FUNCTION(f, void(int, double, float, char)) { }
                f(1, _Doublex1::Null(), 3.14f, 17);
            });
        }
        SECTION("non-void return, single argument")
        {
            INVOKE_INLINE(void(void), {
                FUNCTION(f, int(int))
                {
                    RETURN(PARAMETER(0));
                }
                f(_I32x1::Null()).discard();
            });
        }
        SECTION("non-void return, multiple arguments")
        {
            INVOKE_INLINE(void(void), {
                FUNCTION(f, int(int, double, float, char))
                {
                    RETURN(PARAMETER(0));
                }
                f(1, _Doublex1::Null(), 3.14f, 17).discard();
            });
        }

        CHECK(not oss.str().empty());
    }

    SECTION("argument nullable")
    {
        REQUIRE(oss.str().empty());

        SECTION("void return, single argument")
        {
            INVOKE_INLINE(void(void), {
                FUNCTION(f, void(int)) { }
                Var<I32x1> param(_I32x1::Null()); // to make param nullable
                param = 1;
                f(param);
            });
        }
        SECTION("void return, multiple arguments")
        {
            INVOKE_INLINE(void(void), {
                FUNCTION(f, void(int, double, float, char)) { }
                Var<Doublex1> param(_Doublex1::Null()); // to make param nullable
                param = 1.23;
                f(1, param, 3.14f, 17);
            });
        }
        SECTION("non-void return, single argument")
        {
            INVOKE_INLINE(void(void), {
                FUNCTION(f, int(int))
                {
                    RETURN(PARAMETER(0));
                }
                Var<I32x1> param(_I32x1::Null()); // to make param nullable
                param = 1;
                f(param).discard();
            });
        }
        SECTION("non-void return, multiple arguments")
        {
            INVOKE_INLINE(void(void), {
                FUNCTION(f, int(int, double, float, char))
                {
                    RETURN(PARAMETER(0));
                }
                Var<Doublex1> param(_Doublex1::Null()); // to make param nullable
                param = 1.23;
                f(1, param, 3.14f, 17).discard();
            });
        }

        CHECK(oss.str().empty());
    }

    std::cerr.rdbuf(old);

    Module::Dispose();
}
#endif
