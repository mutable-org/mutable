/* vim: set filetype=cpp: */
#include "backend/WasmDSL.hpp"
#include "backend/WasmMacro.hpp"

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

TEST_CASE("Wasm/" BACKEND_NAME "/make_type", "[core][wasm]")
{
    /*----- Void -----*/
    REQUIRE(wasm_type<void>() == ::wasm::Type::none);

    /*----- Integers -----*/
    REQUIRE(wasm_type<signed char>() == ::wasm::Type::i32);
    REQUIRE(wasm_type<signed short>() == ::wasm::Type::i32);
    REQUIRE(wasm_type<signed int>() == ::wasm::Type::i32);
    REQUIRE(wasm_type<int64_t>() == ::wasm::Type::i64);
    REQUIRE(wasm_type<unsigned char>() == ::wasm::Type::i32);
    REQUIRE(wasm_type<unsigned short>() == ::wasm::Type::i32);
    REQUIRE(wasm_type<unsigned int>() == ::wasm::Type::i32);
    REQUIRE(wasm_type<uint64_t>() == ::wasm::Type::i64);

    /*----- Floating points -----*/
    REQUIRE(wasm_type<float>() == ::wasm::Type::f32);
    REQUIRE(wasm_type<double>() == ::wasm::Type::f64);
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

    /*----- Ts... -> void -----*/
    INVOKE_INLINE(void(int, float), { }, 42, 3.14f);
    INVOKE_INLINE(void(char, unsigned, double, float, int), { }, 42, 2U, 2.72, 3.14f, 1337);

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
        RETURN(_I64(-123456789012));
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
        RETURN(_U64(123456789012));
    });
    CHECK_RESULT_INLINE(3.14f, float(), {
        RETURN(_Float(3.14f));
    });
    CHECK_RESULT_INLINE(3.14, double(), {
        RETURN(_Double(3.14));
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

    /*----- Ts -> T -----*/
    CHECK_RESULT_INLINE(42, int(int, float), {
        RETURN(PARAMETER(0));
    }, 42, 3.14f);
    CHECK_RESULT_INLINE(2.72, double(char, unsigned, double, float, int), {
        RETURN(PARAMETER(2));
    }, 42, 2U, 2.72, 3.14f, 1337);

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
        Global<I32> global(0);
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
        Global<I32> global(0);
        FUNCTION(f, void(void))
        {
            global = 1;
        }
        f();
        RETURN(global);
    });

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Expr/operations/unary", "[core][wasm]")
{
    Module::Init();

    CHECK_RESULT_INLINE( 42, int(), { _I32 a(42); RETURN(+a); });
    CHECK_RESULT_INLINE(-42, int(), { _I32 a(42); RETURN(-a); });
    // abs() not supported for integral types
    // ceil() not supported for integral types
    // floor() not supported for integral types
    // sqrt() not supported for integral types
    CHECK_RESULT_INLINE(  ~42, int32_t(),  { _I32 a(42); RETURN(~a); });
    CHECK_RESULT_INLINE(   28, uint32_t(), { _U32 a(0b1010);  RETURN(a.clz()); });
    CHECK_RESULT_INLINE(   12, uint16_t(), { _U16 a(0b1010);  RETURN(a.clz()); });
    CHECK_RESULT_INLINE(    4, uint8_t(),  { _U8  a(0b1010);  RETURN(a.clz()); });
    CHECK_RESULT_INLINE(    1, uint32_t(), { _U32 a(0b1010);  RETURN(a.ctz()); });
    CHECK_RESULT_INLINE(    1, uint16_t(), { _U16 a(0b1010);  RETURN(a.ctz()); });
    CHECK_RESULT_INLINE(    1, uint8_t(),  { _U8  a(0b1010);  RETURN(a.ctz()); });
    CHECK_RESULT_INLINE(    2, uint32_t(), { _U32 a(0b1010);  RETURN(a.popcnt()); });
    CHECK_RESULT_INLINE(    2, uint16_t(), { _U16 a(0b1010);  RETURN(a.popcnt()); });
    CHECK_RESULT_INLINE(    2, uint8_t(),  { _U8  a(0b1010);  RETURN(a.popcnt()); });
    CHECK_RESULT_INLINE(false, bool(), { _I32 a(42); RETURN(a.eqz()); });
    CHECK_RESULT_INLINE( true, bool(), { _I32 a(0);  RETURN(a.eqz()); });
    CHECK_RESULT_INLINE(false, bool(), { _Bool a(true);  RETURN(not a); });
    CHECK_RESULT_INLINE( true, bool(), { _Bool a(false); RETURN(not a); });
    CHECK_RESULT_INLINE( true, bool(), { _I32 a(_I32::Null()); RETURN(a.is_null()); });
    CHECK_RESULT_INLINE(false, bool(), { _I32 a(42); RETURN(a.is_null()); });
    CHECK_RESULT_INLINE(false, bool(), { _I32 a(_I32::Null()); RETURN(a.not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _I32 a(42); RETURN(a.not_null()); });
    CHECK_RESULT_INLINE(false, bool(), { _Bool a(_Bool::Null()); RETURN(a.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Bool a(true); RETURN(a.is_true_and_not_null()); });
    CHECK_RESULT_INLINE(false, bool(), { _Bool a(false); RETURN(a.is_true_and_not_null()); });
    CHECK_RESULT_INLINE(false, bool(), { _Bool a(_Bool::Null()); RETURN(a.is_false_and_not_null()); });
    CHECK_RESULT_INLINE(false, bool(), { _Bool a(true); RETURN(a.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Bool a(false); RETURN(a.is_false_and_not_null()); });

    CHECK_RESULT_INLINE( 3.14, double(), { _Double a( 3.14); RETURN(+a); });
    CHECK_RESULT_INLINE(-3.14, double(), { _Double a( 3.14); RETURN(-a); });
    CHECK_RESULT_INLINE( 3.14, double(), { _Double a( 3.14); RETURN(a.abs()); });
    CHECK_RESULT_INLINE( 3.14, double(), { _Double a(-3.14); RETURN(a.abs()); });
    CHECK_RESULT_INLINE( 4.0,  double(), { _Double a( 3.14); RETURN(a.ceil()); });
    CHECK_RESULT_INLINE( 3.0,  double(), { _Double a( 3.14); RETURN(a.floor()); });
    CHECK_RESULT_INLINE( 3.0,  double(), { _Double a( 9.0);  RETURN(a.sqrt()); });
    // operator~ not supported for floating-point types
    // clz() not supported for floating-point types
    // ctz() not supported for floating-point types
    // popcnt() not supported for floating-point types
    // eqz() not supported for floating-point types
    // operator! not supported for floating-point types
    CHECK_RESULT_INLINE( true, bool(), { _Double a(_Double::Null()); RETURN(a.is_null()); });
    CHECK_RESULT_INLINE(false, bool(), { _Double a(3.14);                 RETURN(a.is_null()); });
    CHECK_RESULT_INLINE(false, bool(), { _Double a(_Double::Null()); RETURN(a.not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Double a(3.14);                 RETURN(a.not_null()); });
    // is_true_and_not_null() not supported for floating-point types
    // is_false_and_not_null() not supported for floating-point types

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Expr/operations/binary", "[core][wasm]")
{
    Module::Init();

    CHECK_RESULT_INLINE( 59, int(), { _I32 a(42); _I32 b(17); RETURN(a + b); });
    CHECK_RESULT_INLINE( 25, int(), { _I32 a(42); _I32 b(17); RETURN(a - b); });
    CHECK_RESULT_INLINE(714, int(), { _I32 a(42); _I32 b(17); RETURN(a * b); });
    CHECK_RESULT_INLINE(  2, int(), { _I32 a(42); _I32 b(17); RETURN(a / b); });
    CHECK_RESULT_INLINE(  8, int(), { _I32 a(42); _I32 b(17); RETURN(a % b); });
    // copy_sign() not supported for integral types
    // min() not supported for integral types
    // max() not supported for integral types
    CHECK_RESULT_INLINE(0b00001000, int(), { _I8 a(0b00101010);  _I8 b(0b00001001); RETURN(a & b); });
    CHECK_RESULT_INLINE(0b00101011, int(), { _I8 a(0b00101010);  _I8 b(0b00001001); RETURN(a | b); });
    CHECK_RESULT_INLINE(0b00100011, int(), { _I8 a(0b00101010);  _I8 b(0b00001001); RETURN(a ^ b); });
    CHECK_RESULT_INLINE(0b01000000, int8_t(), { _I8 a(0b00101010);  _I8 b(5);  RETURN(a << b); });
    CHECK_RESULT_INLINE(0b00001010, int8_t(), { _I8 a(0b00101010);  _I8 b(2);  RETURN(a >> b); });
    CHECK_RESULT_INLINE(0x01010001, int(), { _I32 a(0x00101010); _I32 b(12); RETURN(rotl(a, b)); });
    CHECK_RESULT_INLINE(0x01000101, int(), { _I32 a(0x00101010); _I32 b(12); RETURN(rotr(a, b)); });
    CHECK_RESULT_INLINE( true, bool(), { _I32 a(42); _I32 b(42); RETURN(a == b); });
    CHECK_RESULT_INLINE(false, bool(), { _I32 a(42); _I32 b(17); RETURN(a == b); });
    CHECK_RESULT_INLINE(false, bool(), { _I32 a(42); _I32 b(42); RETURN(a != b); });
    CHECK_RESULT_INLINE( true, bool(), { _I32 a(42); _I32 b(17); RETURN(a != b); });
    CHECK_RESULT_INLINE(false, bool(), { _I32 a(42); _I32 b(42); RETURN(a < b); });
    CHECK_RESULT_INLINE(false, bool(), { _I32 a(42); _I32 b(17); RETURN(a < b); });
    CHECK_RESULT_INLINE( true, bool(), { _I32 a(17); _I32 b(42); RETURN(a < b); });
    CHECK_RESULT_INLINE( true, bool(), { _I32 a(42); _I32 b(42); RETURN(a <= b); });
    CHECK_RESULT_INLINE(false, bool(), { _I32 a(42); _I32 b(17); RETURN(a <= b); });
    CHECK_RESULT_INLINE( true, bool(), { _I32 a(17); _I32 b(42); RETURN(a <= b); });
    CHECK_RESULT_INLINE(false, bool(), { _I32 a(42); _I32 b(42); RETURN(a > b); });
    CHECK_RESULT_INLINE( true, bool(), { _I32 a(42); _I32 b(17); RETURN(a > b); });
    CHECK_RESULT_INLINE(false, bool(), { _I32 a(17); _I32 b(42); RETURN(a > b); });
    CHECK_RESULT_INLINE( true, bool(), { _I32 a(42); _I32 b(42); RETURN(a >= b); });
    CHECK_RESULT_INLINE( true, bool(), { _I32 a(42); _I32 b(17); RETURN(a >= b); });
    CHECK_RESULT_INLINE(false, bool(), { _I32 a(17); _I32 b(42); RETURN(a >= b); });
    CHECK_RESULT_INLINE( true, bool(), { _Bool a(true);  _Bool b(true);  RETURN(a and b); });
    CHECK_RESULT_INLINE(false, bool(), { _Bool a(false); _Bool b(true);  RETURN(a and b); });
    CHECK_RESULT_INLINE(false, bool(), { _Bool a(true);  _Bool b(false); RETURN(a and b); });
    CHECK_RESULT_INLINE(false, bool(), { _Bool a(false); _Bool b(false); RETURN(a and b); });
    CHECK_RESULT_INLINE( true, bool(), { _Bool a(true);  _Bool b(true);  RETURN(a or b); });
    CHECK_RESULT_INLINE( true, bool(), { _Bool a(false); _Bool b(true);  RETURN(a or b); });
    CHECK_RESULT_INLINE( true, bool(), { _Bool a(true);  _Bool b(false); RETURN(a or b); });
    CHECK_RESULT_INLINE(false, bool(), { _Bool a(false); _Bool b(false); RETURN(a or b); });

    CHECK_RESULT_INLINE( Approx(5.85), double(), { _Double a(3.14); _Double b(2.71); RETURN(a + b); });
    CHECK_RESULT_INLINE( Approx(0.43), double(), { _Double a(3.14); _Double b(2.71); RETURN(a - b); });
    CHECK_RESULT_INLINE( Approx(1.57), double(), { _Double a(3.14); _Double b(0.5);  RETURN(a * b); });
    CHECK_RESULT_INLINE( Approx(6.28), double(), { _Double a(3.14); _Double b(0.5);  RETURN(a / b); });
    // operator% not supported for floating-point types
    CHECK_RESULT_INLINE( 3.14, double(), { _Double a(3.14); _Double b( 1.0);  RETURN(copy_sign(a, b)); });
    CHECK_RESULT_INLINE(-3.14, double(), { _Double a(3.14); _Double b(-1.0);  RETURN(copy_sign(a, b)); });
    CHECK_RESULT_INLINE( 2.71, double(), { _Double a(3.14); _Double b( 2.71); RETURN(min(a, b)); });
    CHECK_RESULT_INLINE( 3.14, double(), { _Double a(3.14); _Double b( 2.71); RETURN(max(a, b)); });
    // operator& not supported for floating-point types
    // operator| not supported for floating-point types
    // operator^ not supported for floating-point types
    // operator<< not supported for floating-point types
    // operator>> not supported for floating-point types
    // rotl() not supported for floating-point types
    // rotr() not supported for floating-point types
    CHECK_RESULT_INLINE( true, bool(),   { _Double a(3.14); _Double b(3.14); RETURN(a == b); });
    CHECK_RESULT_INLINE(false, bool(),   { _Double a(3.14); _Double b(2.71); RETURN(a == b); });
    CHECK_RESULT_INLINE(false, bool(),   { _Double a(3.14); _Double b(3.14); RETURN(a != b); });
    CHECK_RESULT_INLINE( true, bool(),   { _Double a(3.14); _Double b(2.71); RETURN(a != b); });
    CHECK_RESULT_INLINE(false, bool(),   { _Double a(3.14); _Double b(3.14); RETURN(a < b); });
    CHECK_RESULT_INLINE(false, bool(),   { _Double a(3.14); _Double b(2.71); RETURN(a < b); });
    CHECK_RESULT_INLINE( true, bool(),   { _Double a(2.71); _Double b(3.14); RETURN(a < b); });
    CHECK_RESULT_INLINE( true, bool(),   { _Double a(3.14); _Double b(3.14); RETURN(a <= b); });
    CHECK_RESULT_INLINE(false, bool(),   { _Double a(3.14); _Double b(2.71); RETURN(a <= b); });
    CHECK_RESULT_INLINE( true, bool(),   { _Double a(2.71); _Double b(3.14); RETURN(a <= b); });
    CHECK_RESULT_INLINE(false, bool(),   { _Double a(3.14); _Double b(3.14); RETURN(a > b); });
    CHECK_RESULT_INLINE( true, bool(),   { _Double a(3.14); _Double b(2.71); RETURN(a > b); });
    CHECK_RESULT_INLINE(false, bool(),   { _Double a(2.71); _Double b(3.14); RETURN(a > b); });
    CHECK_RESULT_INLINE( true, bool(),   { _Double a(3.14); _Double b(3.14); RETURN(a >= b); });
    CHECK_RESULT_INLINE( true, bool(),   { _Double a(3.14); _Double b(2.71); RETURN(a >= b); });
    CHECK_RESULT_INLINE(false, bool(),   { _Double a(2.71); _Double b(3.14); RETURN(a >= b); });
    // operator&& not supported for floating-point types
    // operator|| not supported for floating-point types

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Expr/conversions", "[core][wasm]")
{
    Module::Init();

    /* 8bit -> 32bit */
    CHECK_RESULT_INLINE( 42, uint32_t(), { _U8 a(42); RETURN(a); });
    CHECK_RESULT_INLINE( 42, uint32_t(), { _U8 a(42); RETURN(a.to<uint32_t>()); });
    CHECK_RESULT_INLINE(-17, int32_t(),  { _I8 a(-17); RETURN(a); });
    CHECK_RESULT_INLINE(-17, int32_t(),  { _I8 a(-17); RETURN(a.to<int32_t>()); });

    /* 32bit -> 8bit */
    // implicit conversion from uint32_t to uint8_t not allowed
    CHECK_RESULT_INLINE(0x78, uint8_t(), { _U32 a(0x12345678); RETURN(a.to<uint8_t>()); });
    // implicit conversion from int32_t to int8_t not allowed
    CHECK_RESULT_INLINE(0x21, int8_t(),  { _I32 a(0x87654321);  RETURN(a.to<int8_t>()); });

    /* float -> double */
    CHECK_RESULT_INLINE(Approx( 3.14), double(), { _Float a(3.14f);  RETURN(a); });
    CHECK_RESULT_INLINE(Approx( 3.14), double(), { _Float a(3.14f);  RETURN(a.to<double>()); });
    CHECK_RESULT_INLINE(Approx(-2.71), double(), { _Float a(-2.71f); RETURN(a); });
    CHECK_RESULT_INLINE(Approx(-2.71), double(), { _Float a(-2.71f); RETURN(a.to<double>()); });

    /* double -> float */
    // implicit conversion from double to float not allowed
    CHECK_RESULT_INLINE(Approx( 3.1415926535f), float(), { _Double a(3.1415926535);  RETURN(a.to<float>()); });
    // implicit conversion from double to float not allowed
    CHECK_RESULT_INLINE(Approx(-2.7182818284f), float(), { _Double a(-2.7182818284); RETURN(a.to<float>()); });

    /* T -> bool */
    CHECK_RESULT_INLINE( true, bool(), { Expr<unsigned> a(42); RETURN(a.to<bool>()); });
    CHECK_RESULT_INLINE(false, bool(), { Expr<unsigned> a(0U); RETURN(a.to<bool>()); });
    CHECK_RESULT_INLINE( true, bool(), { _I32 a(42);      RETURN(a.to<bool>()); });
    CHECK_RESULT_INLINE(false, bool(), { _I32 a(0);       RETURN(a.to<bool>()); });
    CHECK_RESULT_INLINE( true, bool(), { _Double a(42.0); RETURN(a.to<bool>()); });
    CHECK_RESULT_INLINE(false, bool(), { _Double a(0.0);  RETURN(a.to<bool>()); });
    CHECK_RESULT_INLINE( true, bool(), { _Float a(42.0f); RETURN(a.to<bool>()); });
    CHECK_RESULT_INLINE(false, bool(), { _Float a(0.0f);  RETURN(a.to<bool>()); });
    // implicit conversion to bool not allowed

    /* implicit conversions */
    CHECK_RESULT_INLINE(0x5a,       uint8_t(),  { _U8 a(0x12); _U8  b(0x48); RETURN(a | b); });
    CHECK_RESULT_INLINE(0x0000005a, uint32_t(), { _U8 a(0x12); _U8  b(0x48); RETURN(a | b); });
    // implicit conversion from uint32_t (i.e. result of uint8_t and uint16_t) to uint8_t not allowed
    CHECK_RESULT_INLINE(0x0000005a, uint32_t(), { _U8 a(0x12); _U16 b(0x48); RETURN(a | b); });
    CHECK_RESULT_INLINE(0x5a,       int8_t(),   { _I8  a(0x12); _I8   b(0x48); RETURN(a | b); });
    CHECK_RESULT_INLINE(0x0000005a, int32_t(),  { _I8  a(0x12); _I8   b(0x48); RETURN(a | b); });
    // implicit conversion from int32_t (i.e. result of int8_t and int16_t) to int8_t not allowed
    CHECK_RESULT_INLINE(0x0000005a, int32_t(),  { _I8  a(0x12); _I16  b(0x48); RETURN(a | b); });

    CHECK_RESULT_INLINE(Approx(5.85f), float(),  { _Float a(3.14f); _Float  b(2.71f); RETURN(a + b); });
    CHECK_RESULT_INLINE(Approx(5.85),  double(), { _Float a(3.14f); _Float  b(2.71f); RETURN(a + b); });
    // implicit conversion from double (i.e. result of float and double) to float not allowed
    CHECK_RESULT_INLINE(Approx(5.85),  double(), { _Float a(3.14f); _Double b(2.71);  RETURN(a + b); });

    /* change signedness */
    CHECK_RESULT_INLINE(          42,  int(),      { Expr<unsigned> a( 42);  RETURN(a.make_signed()); });
    CHECK_RESULT_INLINE(         -42,  int(),      { Expr<unsigned> a(-42);  RETURN(a.make_signed()); });
    CHECK_RESULT_INLINE(          42,  unsigned(), { _I32 a( 42);  RETURN(a.make_unsigned()); });
    CHECK_RESULT_INLINE(unsigned(-42), unsigned(), { _I32 a(-42);  RETURN(a.make_unsigned()); });

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Expr/null_semantics", "[core][wasm]")
{
    Module::Init();

    /* with unary operations */
    CHECK_RESULT_INLINE( true, bool(), { _I32 a(_I32::Null()); _I32 b(-a); RETURN(b.is_null()); });
    CHECK_RESULT_INLINE(false, bool(), { _I32 a(42); _I32 b(-a); RETURN(b.is_null()); });

    /* with binary operations */
    CHECK_RESULT_INLINE( true, bool(), { _I32 a(_I32::Null()); _I32 b(_I32::Null()); _I32 c(a + b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _I32 a(_I32::Null()); _I32 b(17); _I32 c(a + b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _I32 a(42); _I32 b(_I32::Null()); _I32 c(a + b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE(false, bool(), { _I32 a(42); _I32 b(17); _I32 c(a + b); RETURN(c.is_null()); });

    /* special cases with logical `or` and `and` */
    CHECK_RESULT_INLINE( true, bool(), { _Bool a(_Bool::Null()); _Bool b(_Bool::Null()); _Bool c(a or b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Bool a(_Bool::Null()); Bool b(false); _Bool c(a or b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Bool a(_Bool::Null()); Bool b(true);  _Bool c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { Bool a(false); _Bool b(_Bool::Null()); _Bool c(a or b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { Bool a(true);  _Bool b(_Bool::Null()); _Bool c(a or b); RETURN(c.is_true_and_not_null()); });

    CHECK_RESULT_INLINE( true, bool(), { _Bool a(_Bool::Null()); _Bool b(_Bool::Null()); _Bool c(a and b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Bool a(_Bool::Null()); Bool b(false); _Bool c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Bool a(_Bool::Null()); Bool b(true);  _Bool c(a and b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { Bool a(false); _Bool b(_Bool::Null()); _Bool c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { Bool a(true);  _Bool b(_Bool::Null()); _Bool c(a and b); RETURN(c.is_null()); });

    /* with conversions */
    CHECK_RESULT_INLINE( true, bool(), { _I8 a(_I8::Null()); _I32 b(a); RETURN(b.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _I32 a(_I32::Null()); _I8 b(a.to<int8_t>()); RETURN(b.is_null()); });

    // CHECK_RESULT_INLINE(1, int(), { _Bool a(_Bool::Null()); RETURN(Select(a, 0, 1)); });
    // CHECK_RESULT_INLINE(1, int(), { _Bool a(_Bool::Null()); RETURN(Select(a or false, 0, 1)); });
    // CHECK_RESULT_INLINE(0, int(), { _Bool a(_Bool::Null()); RETURN(Select(a or true, 0, 1)); });

    /* with control flow */
    CHECK_RESULT_INLINE(1, int(), { _Bool a(_Bool::Null()); IF (a) { RETURN(0); } ELSE { RETURN(1); }; });
    CHECK_RESULT_INLINE(1, int(), { _Bool a(_Bool::Null()); IF (a or false) { RETURN(0); } ELSE { RETURN(1); }; });
    CHECK_RESULT_INLINE(0, int(), { _Bool a(_Bool::Null()); IF (a or true) { RETURN(0); } ELSE { RETURN(1); }; });

    CHECK_RESULT_INLINE(1, int(), {
        Var<I32> res(0);
        LOOP() {
            BREAK(res < _I32::Null());
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(1, int(), {
        Var<I32> res(0);
        LOOP() {
            res += 1;
            CONTINUE(res < _I32::Null());
        }
        RETURN(res);
    });

    CHECK_RESULT_INLINE(1, int(), {
        Var<I32> res(0);
        _I32 i(_I32::Null());
        DO_WHILE (i < 10) {
            res += 1;
        }
        RETURN(res);
    });

    CHECK_RESULT_INLINE(0, int(), {
        Var<I32> res(0);
        _I32 i(_I32::Null());
        WHILE (i < 10) {
            res += 1;
        }
        RETURN(res);
    });

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Variable/operations/unary", "[core][wasm]")
{
    Module::Init();

    CHECK_RESULT_INLINE( 42, int(), { Var<I32> a(42); RETURN(+a); });
    CHECK_RESULT_INLINE(-42, int(), { Var<I32> a(42); RETURN(-a); });
    // abs() not supported for integral types
    // ceil() not supported for integral types
    // floor() not supported for integral types
    // sqrt() not supported for integral types
    CHECK_RESULT_INLINE(  ~42, int(),      { Var<I32> a(42); RETURN(~a); });
    CHECK_RESULT_INLINE(   11, unsigned(), { Var<U32> a(0x00101010U); RETURN(a.clz()); });
    CHECK_RESULT_INLINE(    1, unsigned(), { Var<U8>  a(0b00101010U); RETURN(a.ctz()); });
    CHECK_RESULT_INLINE(    3, unsigned(), { Var<U8>  a(0b00101010U); RETURN(a.popcnt()); });
    CHECK_RESULT_INLINE(false, bool(),     { Var<I32> a(42); RETURN(a.eqz()); });
    CHECK_RESULT_INLINE( true, bool(),     { Var<I32> a(0);  RETURN(a.eqz()); });
    CHECK_RESULT_INLINE(false, bool(),     { Var<Bool> a(true);  RETURN(not a); });
    CHECK_RESULT_INLINE( true, bool(),     { Var<Bool> a(false); RETURN(not a); });
    CHECK_RESULT_INLINE( true, bool(),     { Var<_I32> a(_I32::Null()); RETURN(a.is_null()); });
    CHECK_RESULT_INLINE(false, bool(),     { Var<_I32> a(42); RETURN(a.is_null()); });
    CHECK_RESULT_INLINE(false, bool(),     { Var<_I32> a(_I32::Null()); RETURN(a.not_null()); });
    CHECK_RESULT_INLINE( true, bool(),     { Var<_I32> a(42); RETURN(a.not_null()); });
    CHECK_RESULT_INLINE(false, bool(),     { Var<_Bool> a(_Bool::Null()); RETURN(a.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(),     { Var<_Bool> a(true);               RETURN(a.is_true_and_not_null()); });
    CHECK_RESULT_INLINE(false, bool(),     { Var<_Bool> a(false);              RETURN(a.is_true_and_not_null()); });
    CHECK_RESULT_INLINE(false, bool(),     { Var<_Bool> a(_Bool::Null()); RETURN(a.is_false_and_not_null()); });
    CHECK_RESULT_INLINE(false, bool(),     { Var<_Bool> a(true);               RETURN(a.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(),     { Var<_Bool> a(false);              RETURN(a.is_false_and_not_null()); });

    CHECK_RESULT_INLINE( 3.14, double(), { Var<Double> a( 3.14); RETURN(+a); });
    CHECK_RESULT_INLINE(-3.14, double(), { Var<Double> a( 3.14); RETURN(-a); });
    CHECK_RESULT_INLINE( 3.14, double(), { Var<Double> a( 3.14); RETURN(a.abs()); });
    CHECK_RESULT_INLINE( 3.14, double(), { Var<Double> a(-3.14); RETURN(a.abs()); });
    CHECK_RESULT_INLINE( 4.0,  double(), { Var<Double> a( 3.14); RETURN(a.ceil()); });
    CHECK_RESULT_INLINE( 3.0,  double(), { Var<Double> a( 3.14); RETURN(a.floor()); });
    CHECK_RESULT_INLINE( 3.0,  double(), { Var<Double> a( 9.0);  RETURN(a.sqrt()); });
    // operator~ not supported for floating-point types
    // clz() not supported for floating-point types
    // ctz() not supported for floating-point types
    // popcnt() not supported for floating-point types
    // eqz() not supported for floating-point types
    // operator! not supported for floating-point types
    CHECK_RESULT_INLINE( true, bool(), { Var<_Double> a(_Double::Null()); RETURN(a.is_null()); });
    CHECK_RESULT_INLINE(false, bool(), { Var<_Double> a(3.14);                 RETURN(a.is_null()); });
    CHECK_RESULT_INLINE(false, bool(), { Var<_Double> a(_Double::Null()); RETURN(a.not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { Var<_Double> a(3.14);                 RETURN(a.not_null()); });
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
#define TEST_OP_INT(RES, VALUE_A, VALUE_B, OP) TEST_OP(RES, I32, VALUE_A, I32, VALUE_B, I32, OP)
#define TEST_OP_BOOL(RES, VALUE_A, VALUE_B, OP) TEST_OP(RES, Bool, VALUE_A, Bool, VALUE_B, Bool, OP)
#define TEST_ASSIGN_INT(RES, VALUE_A, VALUE_B, OP) TEST_ASSIGN(RES, I32, VALUE_A, I32, VALUE_B, I32, OP)
#define TEST_OP_INT_TO_BOOL(RES, VALUE_A, VALUE_B, OP) TEST_OP(RES, Bool, VALUE_A, I32, VALUE_B, I32, OP)
#define TEST_OP_DOUBLE(RES, VALUE_A, VALUE_B, OP) TEST_OP(RES, Double, VALUE_A, Double, VALUE_B, Double, OP)
#define TEST_FUNC_DOUBLE(RES, VALUE_A, VALUE_B, FUNC) TEST_FUNC(RES, Double, VALUE_A, Double, VALUE_B, Double, FUNC)
#define TEST_ASSIGN_DOUBLE(RES, VALUE_A, VALUE_B, OP) TEST_ASSIGN(RES, Double, VALUE_A, Double, VALUE_B, Double, OP)
#define TEST_OP_DOUBLE_TO_BOOL(RES, VALUE_A, VALUE_B, OP) TEST_OP(RES, Bool, VALUE_A, Double, VALUE_B, Double, OP)

    TEST_OP_INT( 59, 42, 17, +);
    TEST_OP_INT( 25, 42, 17, -);
    TEST_OP_INT(714, 42, 17, *);
    TEST_OP_INT(  2, 42, 17, /);
    TEST_OP_INT(  8, 42, 17, %);
    // copy_sign() not supported for integral types
    // min() not supported for integral types
    // max() not supported for integral types
    TEST_OP(0b00001000, I32, 0b00101010, I8,  0b00001001, I8, &);
    TEST_OP(0b00101011, I32, 0b00101010, I8,  0b00001001, I8, |);
    TEST_OP(0b00100011, I32, 0b00101010, I8,  0b00001001, I8, ^);
    TEST_OP(0b10101000, I32, 0b00101010, I8,  2, I32, <<);
    TEST_OP(0b00001010, I32, 0b00101010, I8,  2, I32, >>);
    TEST_FUNC(0x01010001, I32, 0x00101010, I32, 12, I32, rotl);
    TEST_FUNC(0x01000101, I32, 0x00101010, I32, 12, I32, rotr);
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
    TEST_ASSIGN(0b00001000U, U32, 0b00101010U, U8,  0b00001001U, U8, &);
    TEST_ASSIGN(0b00101011U, U32, 0b00101010U, U8,  0b00001001U, U8, |);
    TEST_ASSIGN(0b00100011U, U32, 0b00101010U, U8,  0b00001001U, U8, ^);
    TEST_ASSIGN(0b10101000, I32, 0b00101010, I8,  2, I8, <<);
    TEST_ASSIGN(0b00001010, I32, 0b00101010, I8,  2, I8, >>);
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

TEST_CASE("Wasm/" BACKEND_NAME "/Variable/null_semantics", "[core][wasm]")
{
    Module::Init();

    /* with unary operations */
    CHECK_RESULT_INLINE(true, bool(), {
        _Var<I32> a(_I32::Null()); _Var<I32> b(-a); /* b must allocate new NULL bit */
        RETURN(b.is_null());
    });
    CHECK_RESULT_INLINE(false, bool(), {
        _Var<I32> a(_I32::Null()); _Var<I32> b(-a); /* b must allocate new NULL bit */ a = 42;
        RETURN(a.is_null());
    });
    CHECK_RESULT_INLINE(true, bool(), {
        _Var<I32> a(_I32::Null()); _Var<I32> b(-a); /* b must allocate new NULL bit */ a = 42;
        RETURN(b.is_null());
    });
    CHECK_RESULT_INLINE(true, bool(), {
        _Var<I32> a(_I32::Null()); _Var<I32> b(-a); /* b must allocate new NULL bit */ b = 17;
        RETURN(a.is_null());
    });
    CHECK_RESULT_INLINE(false, bool(), {
        _Var<I32> a(_I32::Null()); _Var<I32> b(-a); /* b must allocate new NULL bit */ b = 17;
        RETURN(b.is_null());
    });

    /* with binary operations */
    CHECK_RESULT_INLINE( true, bool(), {
        _Var<I32> a(_I32::Null()); _Var<I32> b(_I32::Null()); _Var<I32> c(a + b);
        /* a, b, and c must allocate their own NULL bit */
        RETURN(c.is_null());
    });
    CHECK_RESULT_INLINE( true, bool(), {
        _Var<I32> a(_I32::Null()); Var<I32> b(17); _Var<I32> c(a + b);
        /* a and c must allocate their own NULL bit */
        RETURN(c.is_null());
    });
    CHECK_RESULT_INLINE( true, bool(), {
        Var<I32> a(42); _Var<I32> b(_I32::Null()); _Var<I32> c(a + b);
        /* b and c must allocate their own NULL bit */
        RETURN(c.is_null());
    });

    CHECK_RESULT_INLINE(false, bool(), {
        _Var<I32> a(_I32::Null()); _Var<I32> b(_I32::Null()); _Var<I32> c(a + b); a = 42;
        /* a, b, and c must allocate their own NULL bit */
        RETURN(a.is_null());
    });
    CHECK_RESULT_INLINE( true, bool(), {
        _Var<I32> a(_I32::Null()); _Var<I32> b(_I32::Null()); _Var<I32> c(a + b); a = 42;
        /* a, b, and c must allocate their own NULL bit */
        RETURN(b.is_null());
    });
    CHECK_RESULT_INLINE( true, bool(), {
        _Var<I32> a(_I32::Null()); _Var<I32> b(_I32::Null()); _Var<I32> c(a + b); a = 42;
        /* a, b, and c must allocate their own NULL bit */
        RETURN(c.is_null());
    });
    CHECK_RESULT_INLINE( true, bool(), {
        _Var<I32> a(_I32::Null()); _Var<I32> b(_I32::Null()); _Var<I32> c(a + b); b = 17;
        /* a, b, and c must allocate their own NULL bit */
        RETURN(a.is_null());
    });
    CHECK_RESULT_INLINE(false, bool(), {
        _Var<I32> a(_I32::Null()); _Var<I32> b(_I32::Null()); _Var<I32> c(a + b); b = 17;
        /* a, b, and c must allocate their own NULL bit */
        RETURN(b.is_null());
    });
    CHECK_RESULT_INLINE( true, bool(), {
        _Var<I32> a(_I32::Null()); _Var<I32> b(_I32::Null()); _Var<I32> c(a + b); b = 17;
        /* a, b, and c must allocate their own NULL bit */
        RETURN(c.is_null());
    });
    CHECK_RESULT_INLINE( true, bool(), {
        _Var<I32> a(_I32::Null()); _Var<I32> b(_I32::Null()); _Var<I32> c(a + b); c = 99;
        /* a, b, and c must allocate their own NULL bit */
        RETURN(a.is_null());
    });
    CHECK_RESULT_INLINE( true, bool(), {
        _Var<I32> a(_I32::Null()); _Var<I32> b(_I32::Null()); _Var<I32> c(a + b); c = 99;
        /* a, b, and c must allocate their own NULL bit */
        RETURN(b.is_null());
    });
    CHECK_RESULT_INLINE(false, bool(), {
        _Var<I32> a(_I32::Null()); _Var<I32> b(_I32::Null()); _Var<I32> c(a + b); c = 99;
        /* a, b, and c must allocate their own NULL bit */
        RETURN(c.is_null());
    });

    /* special cases with logical `or` and `and` */
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(_Bool::Null()); _Var<Bool> b(_Bool::Null()); _Bool c(a or b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(_Bool::Null()); _Var<Bool> b(false); _Bool c(a or b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(_Bool::Null()); _Var<Bool> b(true);  _Bool c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(false); _Var<Bool> b(_Bool::Null()); _Bool c(a or b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(true);  _Var<Bool> b(_Bool::Null()); _Bool c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(false); _Var<Bool> b(false); _Bool c(a or b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(false); _Var<Bool> b(true);  _Bool c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(true);  _Var<Bool> b(false); _Bool c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(true);  _Var<Bool> b(true);  _Bool c(a or b); RETURN(c.is_true_and_not_null()); });

    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(_Bool::Null()); _Var<Bool> b(false); _Bool c(a or b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(_Bool::Null()); _Var<Bool> b(true);  _Bool c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(false); _Var<Bool> b(false); _Bool c(a or b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(false); _Var<Bool> b(true);  _Bool c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(true);  _Var<Bool> b(false); _Bool c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(true);  _Var<Bool> b(true);  _Bool c(a or b); RETURN(c.is_true_and_not_null()); });

    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(false); _Var<Bool> b(_Bool::Null()); _Bool c(a or b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(true);  _Var<Bool> b(_Bool::Null()); _Bool c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(false); _Var<Bool> b(false); _Bool c(a or b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(false); _Var<Bool> b(true);  _Bool c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(true);  _Var<Bool> b(false); _Bool c(a or b); RETURN(c.is_true_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(true);  _Var<Bool> b(true);  _Bool c(a or b); RETURN(c.is_true_and_not_null()); });

    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(_Bool::Null()); _Var<Bool> b(_Bool::Null()); _Bool c(a and b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(_Bool::Null()); _Var<Bool> b(false); _Bool c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(_Bool::Null()); _Var<Bool> b(true);  _Bool c(a and b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(false); _Var<Bool> b(_Bool::Null()); _Bool c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(true);  _Var<Bool> b(_Bool::Null()); _Bool c(a and b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(false); _Var<Bool> b(false); _Bool c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(false); _Var<Bool> b(true);  _Bool c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(true);  _Var<Bool> b(false); _Bool c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(true);  _Var<Bool> b(true);  _Bool c(a and b); RETURN(c.is_true_and_not_null()); });

    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(_Bool::Null()); _Var<Bool> b(false); _Bool c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(_Bool::Null()); _Var<Bool> b(true);  _Bool c(a and b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(false); _Var<Bool> b(false); _Bool c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(false); _Var<Bool> b(true);  _Bool c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(true);  _Var<Bool> b(false); _Bool c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(true);  _Var<Bool> b(true);  _Bool c(a and b); RETURN(c.is_true_and_not_null()); });

    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(false); _Var<Bool> b(_Bool::Null()); _Bool c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(true);  _Var<Bool> b(_Bool::Null()); _Bool c(a and b); RETURN(c.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(false); _Var<Bool> b(false); _Bool c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(false); _Var<Bool> b(true);  _Bool c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(true);  _Var<Bool> b(false); _Bool c(a and b); RETURN(c.is_false_and_not_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<Bool> a(true);  _Var<Bool> b(true);  _Bool c(a and b); RETURN(c.is_true_and_not_null()); });

    /* with NULL bit reuse */
    CHECK_RESULT_INLINE(false, bool(), {
        _Var<I32> a(_I32::Null()); // to make a nullable
        a = 1; /* a's NULL bit is updated but no new NULL bit needed */
        RETURN(a.is_null());
    });
    CHECK_RESULT_INLINE(true, bool(), {
        _Var<I32> a(_I32::Null()); { _Var<I32> b(_I32::Null()); _I32 c(a + b); c.discard(); }
        _Var<I32> d(_I32::Null()); /* d should reuse b's NULL bit (i.e. offset 1) */ RETURN(d.is_null());
    });

    /* with conversions */
    CHECK_RESULT_INLINE( true, bool(), { _Var<I8> a(_I8::Null()); _Var<I32> b(a.val()); RETURN(b.is_null()); });
    CHECK_RESULT_INLINE( true, bool(), { _Var<I32> a(_I32::Null()); _Var<I8> b(a.to<int8_t>()); RETURN(b.is_null());
    });

    // CHECK_RESULT_INLINE(1, int(), { _Var<Bool> a(_Bool::Null()); RETURN(Select(a, 0, 1)); });
    // CHECK_RESULT_INLINE(0, int(), { _Var<Bool> a(_Bool::Null()); a = true; RETURN(Select(a, 0, 1)); });
    // CHECK_RESULT_INLINE(1, int(), { _Var<Bool> a(_Bool::Null()); a = false; RETURN(Select(a, 0, 1)); });
    // CHECK_RESULT_INLINE(1, int(), { _Var<Bool> a(_Bool::Null()); RETURN(Select(a or false, 0, 1)); });
    // CHECK_RESULT_INLINE(0, int(), { _Var<Bool> a(_Bool::Null()); RETURN(Select(a or true, 0, 1)); });

    /*----- with control flow -----*/
    CHECK_RESULT_INLINE(1, int(), {
        _Var<Bool> a(_Bool::Null()); IF (a) { RETURN(0); } ELSE { RETURN(1); };
    });
    CHECK_RESULT_INLINE(0, int(), {
        _Var<Bool> a(_Bool::Null()); a = true; IF (a) { RETURN(0); } ELSE { RETURN(1); };
    });
    CHECK_RESULT_INLINE(1, int(), {
        _Var<Bool> a(_Bool::Null()); a = false; IF (a) { RETURN(0); } ELSE { RETURN(1); };
    });
    CHECK_RESULT_INLINE(1, int(), {
        _Var<Bool> a(_Bool::Null()); IF (a or false) { RETURN(0); } ELSE { RETURN(1); };
    });
    CHECK_RESULT_INLINE(0, int(), {
        _Var<Bool> a(_Bool::Null()); IF (a or true) { RETURN(0); } ELSE { RETURN(1); };
    });

    CHECK_RESULT_INLINE(10, int(), {
        Var<I32> res(0);
        _Var<I32> i(0);
        LOOP() {
            BREAK(not (i < 5));
            res += 1;
            i += 1;
            IF (i == 2) { i = _I32::Null(); };
            IF (res == 10) { i = 10; };
            CONTINUE();
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(2, int(), {
        Var<I32> res(0);
        _Var<I32> i(0);
        LOOP() {
            res += 1;
            i += 1;
            IF (i == 2) { i = _I32::Null(); };
            CONTINUE(i < 5);
        }
        RETURN(res);
    });

    CHECK_RESULT_INLINE(1, int(), {
        Var<I32> res(0);
        _Var<I32> i(_I32::Null());
        DO_WHILE (i < 10) {
            res += 1;
            i += 1;
            IF (i == 5) { i = _I32::Null(); };
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(5, int(), {
        Var<I32> res(0);
        _Var<I32> i(0);
        DO_WHILE (i < 10) {
            res += 1;
            i += 1;
            IF (i == 5) { i = _I32::Null(); };
        }
        RETURN(res);
    });

    CHECK_RESULT_INLINE(0, int(), {
        Var<I32> res(0);
        _Var<I32> i(_I32::Null());
        WHILE (i < 10) {
            res += 1;
            i += 1;
            IF (i == 5) { i = _I32::Null(); };
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(5, int(), {
        Var<I32> res(0);
        _Var<I32> i(0);
        WHILE (i < 10) {
            res += 1;
            i += 1;
            IF (i == 5) { i = _I32::Null(); };
        }
        RETURN(res);
    });

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Select", "[core][wasm]")
{
    Module::Init();

    CHECK_RESULT_INLINE(0, int(), { Bool cond(true);  Var<I32> a(0); Var<I32> b(1); RETURN(Select(cond, a, b)); });
    CHECK_RESULT_INLINE(1, int(), { Bool cond(false); Var<I32> a(0); Var<I32> b(1); RETURN(Select(cond, a, b)); });
    CHECK_RESULT_INLINE(0, int(), { Bool cond(true);  Var<I32> a(0); I32 b(1); RETURN(Select(cond, a, b)); });
    CHECK_RESULT_INLINE(1, int(), { Bool cond(false); Var<I32> a(0); I32 b(1); RETURN(Select(cond, a, b)); });
    CHECK_RESULT_INLINE(0, int(), { Bool cond(true);  I32 a(0); Var<I32> b(1); RETURN(Select(cond, a, b)); });
    CHECK_RESULT_INLINE(1, int(), { Bool cond(false); I32 a(0); Var<I32> b(1); RETURN(Select(cond, a, b)); });
    CHECK_RESULT_INLINE(0, int(), { Bool cond(true);  I32 a(0); I32 b(1); RETURN(Select(cond, a, b)); });
    CHECK_RESULT_INLINE(1, int(), { Bool cond(false); I32 a(0); I32 b(1); RETURN(Select(cond, a, b)); });
    CHECK_RESULT_INLINE(0, int(), { Bool cond(true);  Var<I32> a(0); RETURN(Select(cond, a, 1)); });
    CHECK_RESULT_INLINE(1, int(), { Bool cond(false); Var<I32> a(0); RETURN(Select(cond, a, 1)); });
    CHECK_RESULT_INLINE(0, int(), { Bool cond(true);  Var<I32> b(1); RETURN(Select(cond, 0, b)); });
    CHECK_RESULT_INLINE(1, int(), { Bool cond(false); Var<I32> b(1); RETURN(Select(cond, 0, b)); });
    CHECK_RESULT_INLINE(0, int(), { Bool cond(true);  I32 a(0); RETURN(Select(cond, a, 1)); });
    CHECK_RESULT_INLINE(1, int(), { Bool cond(false); I32 a(0); RETURN(Select(cond, a, 1)); });
    CHECK_RESULT_INLINE(0, int(), { Bool cond(true); I32 b(1); RETURN(Select(cond, 0, b)); });
    CHECK_RESULT_INLINE(1, int(), { Bool cond(false); I32 b(1); RETURN(Select(cond, 0, b)); });

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
        Var<I32> res(0);
        Var<U32> test(2);
        IF ((test = 1).to<bool>()) { res = 1; } ELSE { res = 2; };
        RETURN(res);
    });
    CHECK_RESULT_INLINE(1, unsigned(), {
        Var<I32> res(0);
        Var<U32> test(2);
        IF ((test = 1).to<bool>()) { res = 1; } ELSE { res = 2; };
        RETURN(test);
    });

    CHECK_RESULT_INLINE(2, int(), {
        Var<I32> res(0);
        Var<U32> test(2);
        IF ((test = 0).to<bool>()) { res = 1; } ELSE { res = 2; };
        RETURN(res);
    });
    CHECK_RESULT_INLINE(0, unsigned(), {
        Var<I32> res(0);
        Var<U32> test(2);
        IF ((test = 0).to<bool>()) { res = 1; } ELSE { res = 2; };
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
        Var<I32> res(0);
        LOOP() {
            BREAK();
            res += 1;
            CONTINUE();
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(0, int(), {
        Var<I32> res(0);
        LOOP() {
            BREAK(res == 0);
            res += 1;
            CONTINUE();
        }
        RETURN(res);
    });

    /* one iteration */
    CHECK_RESULT_INLINE(1, int(), {
        Var<I32> res(0);
        LOOP() {
            BREAK(res == 1);
            res += 1;
            CONTINUE();
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(1, int(), {
        Var<I32> res(0);
        LOOP() {
            res += 1;
            BREAK(res == 1);
            CONTINUE();
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(1, int(), {
        Var<I32> res(0);
        LOOP() {
            res += 1;
        }
        RETURN(res);
    });

    /* multiple iterations */
    CHECK_RESULT_INLINE(2, int(), {
        Var<I32> res(0);
        LOOP() {
            BREAK(res == 2);
            res += 1;
            CONTINUE();
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(2, int(), {
        Var<I32> res(0);
        LOOP() {
            res += 1;
            BREAK(res == 2);
            CONTINUE();
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(42, int(), {
        Var<I32> res(0);
        LOOP() {
            BREAK(res == 42);
            res += 1;
            CONTINUE();
        }
        RETURN(res);
    });

    /* continue */
    CHECK_RESULT_INLINE(0, int(), {
        Var<I32> res(0);
        Var<I32> i(0);
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
        Var<I32> res(0);
        Var<I32> i(0);
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
        Var<I32> res(0);
        Var<I32> i(0);
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
        Var<I32> res(0);
        Var<I32> i(0);
        LOOP() {
            BREAK(i == 3);
            Var<I32> j(0);
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
        Var<I32> res(0);
        Var<I32> i(0);
        LOOP() {
            BREAK(i == 3);
            Var<I32> j(0);
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
        Var<I32> res(0);
        Var<I32> i(0);
        LOOP() {                // <-- CONTINUE(2)
            BREAK(i == 3);
            i += 1;
            Var<I32> j(0);
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
        Var<I32> res(0);
        DO_WHILE(res < 1) {
            res += 1;
        }
        RETURN(res);
    });

    /* multiple iterations */
    CHECK_RESULT_INLINE(2, int(), {
        Var<I32> res(0);
        DO_WHILE(res < 2) {
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(42, int(), {
        Var<I32> res(0);
        DO_WHILE(res < 42) {
            res += 1;
        }
        RETURN(res);
    });

    /* break */
    CHECK_RESULT_INLINE(0, int(), {
        Var<I32> res(0);
        DO_WHILE(res < 17) {
            BREAK();
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(1, int(), {
        Var<I32> res(0);
        DO_WHILE(res < 17) {
            res += 1;
            BREAK();
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(5, int(), {
        Var<I32> res(0);
        DO_WHILE(res < 17) {
            BREAK(res == 5);
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(5, int(), {
        Var<I32> res(0);
        DO_WHILE(res < 17) {
            res += 1;
            BREAK(res == 5);
        }
        RETURN(res);
    });

    /* continue */
    CHECK_RESULT_INLINE(0, int(), {
        Var<I32> res(0);
        Var<I32> i(0);
        DO_WHILE(i != 10) {
            i += 1;
            CONTINUE();
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(5, int(), {
        Var<I32> res(0);
        Var<I32> i(0);
        DO_WHILE(i != 10) {
            i += 1;
            CONTINUE(i % 2 == 0);
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(10, int(), {
        Var<I32> res(0);
        Var<I32> i(0);
        DO_WHILE(i != 10) {
            i += 1;
            CONTINUE(i > 10);
            res += 1;
        }
        RETURN(res);
    });

    /* nested do while */
    CHECK_RESULT_INLINE(15, int(), {
        Var<I32> res(0);
        Var<I32> i(0);
        DO_WHILE(i != 3) {
            Var<I32> j(0);
            DO_WHILE(j != 5) {
                res += 1;
                j += 1;
            }
            i += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(12, int(), {
        Var<I32> res(0);
        Var<I32> i(0);
        DO_WHILE(i != 3) {
            Var<I32> j(0);
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
        Var<I32> res(0);
        Var<I32> i(0);
        DO_WHILE(i != 3) {
            Var<I32> j(0);
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
        Var<I32> res(0);
        Var<I32> i(0);
        DO_WHILE(i != 3) {
            Var<I32> j(0);
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
        Var<I32> res(0);
        Var<I32> i(0);
        DO_WHILE(i != 3) {
            i += 1;
            Var<I32> j(0);
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
        Var<I32> res(0);
        WHILE(res < 0) {
            res += 1;
        }
        RETURN(res);
    });

    /* one iteration */
    CHECK_RESULT_INLINE(1, int(), {
        Var<I32> res(0);
        WHILE(res < 1) {
            res += 1;
        }
        RETURN(res);
    });

    /* multiple iterations */
    CHECK_RESULT_INLINE(2, int(), {
        Var<I32> res(0);
        WHILE(res < 2) {
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(42, int(), {
        Var<I32> res(0);
        WHILE(res < 42) {
            res += 1;
        }
        RETURN(res);
    });

    /* break */
    CHECK_RESULT_INLINE(0, int(), {
        Var<I32> res(0);
        WHILE(res < 17) {
            BREAK();
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(1, int(), {
        Var<I32> res(0);
        WHILE(res < 17) {
            res += 1;
            BREAK();
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(5, int(), {
        Var<I32> res(0);
        WHILE(res < 17) {
            BREAK(res == 5);
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(5, int(), {
        Var<I32> res(0);
        WHILE(res < 17) {
            res += 1;
            BREAK(res == 5);
        }
        RETURN(res);
    });

    /* continue */
    CHECK_RESULT_INLINE(0, int(), {
        Var<I32> res(0);
        Var<I32> i(0);
        WHILE(i != 10) {
            i += 1;
            CONTINUE();
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(5, int(), {
        Var<I32> res(0);
        Var<I32> i(0);
        WHILE(i != 10) {
            i += 1;
            CONTINUE(i % 2 == 0);
            res += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(10, int(), {
        Var<I32> res(0);
        Var<I32> i(0);
        WHILE(i != 10) {
            i += 1;
            CONTINUE(i > 10);
            res += 1;
        }
        RETURN(res);
    });

    /* nested while */
    CHECK_RESULT_INLINE(15, int(), {
        Var<I32> res(0);
        Var<I32> i(0);
        WHILE(i != 3) {
            Var<I32> j(0);
            WHILE(j != 5) {
                res += 1;
                j += 1;
            }
            i += 1;
        }
        RETURN(res);
    });
    CHECK_RESULT_INLINE(12, int(), {
        Var<I32> res(0);
        Var<I32> i(0);
        WHILE(i != 3) {
            Var<I32> j(0);
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
        Var<I32> res(0);
        Var<I32> i(0);
        WHILE(i != 3) {
            Var<I32> j(0);
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
        Var<I32> res(0);
        Var<I32> i(0);
        WHILE(i != 3) {
            Var<I32> j(0);
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
        Var<I32> res(0);
        Var<I32> i(0);
        WHILE(i != 3) {
            i += 1;
            Var<I32> j(0);
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
    CHECK_RESULT_INLINE( true, bool(), { Var<Ptr<I32>> a(Module::Allocator().malloc<int>()); RETURN((a + 1) - 1 == a); });
    CHECK_RESULT_INLINE( true, bool(), { Var<Ptr<I32>> a(Module::Allocator().malloc<int>()); RETURN((a + 2) - 1 > a); });

    /* pointer difference */
    CHECK_RESULT_INLINE(0, int32_t(), { Var<Ptr<I32>> a(Module::Allocator().malloc<int>()); RETURN(a - a); });
    CHECK_RESULT_INLINE(4, int32_t(), { Var<Ptr<I32>> a(Module::Allocator().malloc<int>()); RETURN((a + 4) - a); });

    /* comparison */
    CHECK_RESULT_INLINE( true, bool(), { Var<Ptr<I32>> a(Module::Allocator().malloc<int>()); RETURN(a == a); });
    CHECK_RESULT_INLINE(false, bool(), { Var<Ptr<I32>> a(Module::Allocator().malloc<int>()); RETURN(a + 1 == a); });
    CHECK_RESULT_INLINE(false, bool(), { Var<Ptr<I32>> a(Module::Allocator().malloc<int>()); RETURN(a != a); });
    CHECK_RESULT_INLINE( true, bool(), { Var<Ptr<I32>> a(Module::Allocator().malloc<int>()); RETURN(a + 1 != a); });
    CHECK_RESULT_INLINE(false, bool(), { Var<Ptr<I32>> a(Module::Allocator().malloc<int>()); RETURN(a < a); });
    CHECK_RESULT_INLINE(false, bool(), { Var<Ptr<I32>> a(Module::Allocator().malloc<int>()); RETURN(a + 1 < a); });
    CHECK_RESULT_INLINE( true, bool(), { Var<Ptr<I32>> a(Module::Allocator().malloc<int>()); RETURN(a < a + 1); });
    CHECK_RESULT_INLINE( true, bool(), { Var<Ptr<I32>> a(Module::Allocator().malloc<int>()); RETURN(a <= a); });
    CHECK_RESULT_INLINE(false, bool(), { Var<Ptr<I32>> a(Module::Allocator().malloc<int>()); RETURN(a + 1 <= a); });
    CHECK_RESULT_INLINE( true, bool(), { Var<Ptr<I32>> a(Module::Allocator().malloc<int>()); RETURN(a <= a + 1); });
    CHECK_RESULT_INLINE(false, bool(), { Var<Ptr<I32>> a(Module::Allocator().malloc<int>()); RETURN(a > a); });
    CHECK_RESULT_INLINE( true, bool(), { Var<Ptr<I32>> a(Module::Allocator().malloc<int>()); RETURN(a + 1 > a); });
    CHECK_RESULT_INLINE(false, bool(), { Var<Ptr<I32>> a(Module::Allocator().malloc<int>()); RETURN(a > a + 1); });
    CHECK_RESULT_INLINE( true, bool(), { Var<Ptr<I32>> a(Module::Allocator().malloc<int>()); RETURN(a >= a); });
    CHECK_RESULT_INLINE( true, bool(), { Var<Ptr<I32>> a(Module::Allocator().malloc<int>()); RETURN(a + 1 >= a); });
    CHECK_RESULT_INLINE(false, bool(), { Var<Ptr<I32>> a(Module::Allocator().malloc<int>()); RETURN(a >= a + 1); });

    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/Memory", "[core][wasm]")
{
    Module::Init();

    /* malloc<T>() */
    CHECK_RESULT_INLINE(3, char(void), {
        Var<Ptr<Char>> ptr(Module::Allocator().malloc<char>());
        *ptr = char(3);
        RETURN(*ptr);
    });
    CHECK_RESULT_INLINE(42, int(void), {
        Var<Ptr<I32>> ptr(Module::Allocator().malloc<int>());
        *ptr = 42;
        RETURN(*ptr);
    });
    CHECK_RESULT_INLINE(3.14, double(void), {
        Var<Ptr<Double>> ptr(Module::Allocator().malloc<double>());
        *ptr = 3.14;
        RETURN(*ptr);
    });

    /* malloc<T>(length) */
    CHECK_RESULT_INLINE(-29, char(void), {
        Var<Ptr<Char>> ptr(Module::Allocator().malloc<char>(5U));
        *ptr = char(42);
        *(ptr + 1) = char(17);
        *(ptr + 2) = char(123);
        *(ptr + 3) = char(-29);
        *(ptr + 4) = char(3);
        RETURN(*(ptr + 3));
    });
    CHECK_RESULT_INLINE(123, int(void), {
        Var<Ptr<I32>> ptr(Module::Allocator().malloc<int>(3U));
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
        Module::Allocator().malloc<uint16_t>(); // to align to 2 bytes
        Var<Ptr<U8>> ptr1(Module::Allocator().malloc<uint8_t>()); // byte 0
        *ptr1 = uint8_t(0x12);
        Module::Allocator().free(ptr1);
        Var<Ptr<U16>> ptr2(Module::Allocator().malloc<uint16_t>()); // reuses byte 0 and additionally uses byte 1
        RETURN(ptr1.to<uint32_t>() == ptr2.to<uint32_t>());
    });

    /* returning pointer */
    CHECK_RESULT_INLINE(42, int(void), {
        FUNCTION(alloc, int*(unsigned))
        {
            RETURN(Module::Allocator().malloc<int>(PARAMETER(0)));
        }
        Var<Ptr<I32>> ptr(alloc(1U));
        *ptr = 42;
        RETURN(*ptr);
    });

    /* passing pointer */
    CHECK_RESULT_INLINE(42, int(void), {
        FUNCTION(assign, void(int*))
        {
            *PARAMETER(0) = 42;
        }
        Var<Ptr<I32>> ptr(Module::Allocator().malloc<int>());
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
                RETURN(_I32::Null());
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
                _Var<I32> res(_I32::Null());
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
                f(_I32::Null());
            });
        }
        SECTION("void return, multiple arguments")
        {
            INVOKE_INLINE(void(void), {
                FUNCTION(f, void(int, double, float, char)) { }
                f(1, _Double::Null(), 3.14f, 17);
            });
        }
        SECTION("non-void return, single argument")
        {
            INVOKE_INLINE(void(void), {
                FUNCTION(f, int(int))
                {
                    RETURN(PARAMETER(0));
                }
                f(_I32::Null()).discard();
            });
        }
        SECTION("non-void return, multiple arguments")
        {
            INVOKE_INLINE(void(void), {
                FUNCTION(f, int(int, double, float, char))
                {
                    RETURN(PARAMETER(0));
                }
                f(1, _Double::Null(), 3.14f, 17).discard();
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
                Var<I32> param(_I32::Null()); // to make param nullable
                param = 1;
                f(param);
            });
        }
        SECTION("void return, multiple arguments")
        {
            INVOKE_INLINE(void(void), {
                FUNCTION(f, void(int, double, float, char)) { }
                Var<Double> param(_Double::Null()); // to make param nullable
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
                Var<I32> param(_I32::Null()); // to make param nullable
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
                Var<Double> param(_Double::Null()); // to make param nullable
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
