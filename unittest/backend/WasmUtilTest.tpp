/* vim: set filetype=cpp: */
#include "backend/WasmUtil.hpp"

#ifndef BACKEND_NAME
#error "must define BACKEND_NAME before including this file"
#endif

using namespace m::wasm;


TEST_CASE("Wasm/" BACKEND_NAME "/setbit", "[core][wasm]")
{
    Module::Init();
    CodeGenContext::Init();

    /* with bit offset */
    CHECK_RESULT_INLINE(0b10010100, uint8_t(void), {
        auto ptr = Module::Allocator().malloc<uint8_t>();
        *ptr = uint8_t(0b10000100);
        setbit<U8x1>(ptr, Boolx1(true), 4);
        RETURN(*ptr);
    });
   CHECK_RESULT_INLINE(0b10000000, uint8_t(void), {
        auto ptr = Module::Allocator().malloc<uint8_t>();
        *ptr = uint8_t(0b10000100);
        setbit<U8x1>(ptr, Boolx1(false), 2);
        RETURN(*ptr);
    });

   /* with mask */
    CHECK_RESULT_INLINE(0b10010100, uint8_t(void), {
        auto ptr = Module::Allocator().malloc<uint8_t>();
        *ptr = uint8_t(0b10000100);
        setbit(ptr, Boolx1(true), U8x1(0b10000));
        RETURN(*ptr);
    });
   CHECK_RESULT_INLINE(0b10000000, uint8_t(void), {
        auto ptr = Module::Allocator().malloc<uint8_t>();
        *ptr = uint8_t(0b10000100);
        setbit(ptr, Boolx1(false), U8x1(0b100));
        RETURN(*ptr);
    });

    CodeGenContext::Dispose();
    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/string comparison", "[core][wasm]")
{
    Module::Init();
    CodeGenContext::Init();

    SECTION("strncmp with equal-length chars")
    {
        auto cs = m::Type::Get_Char(m::Type::TY_Scalar, 4);

        SECTION("compare all, equal")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(4);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 's'; *(left + 3) = 't';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = 's'; *(right + 3) = 't';
                auto res = strncmp(NChar(left, false, cs), NChar(right, false, cs), U32x1(5));
                WASM_CHECK(res == 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare all, less")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(4);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 'a'; *(left + 3) = 't';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = 's'; *(right + 3) = 't';
                auto res = strncmp(NChar(left, false, cs), NChar(right, false, cs), U32x1(5));
                WASM_CHECK(res < 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare all, greater")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(4);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 's'; *(left + 3) = 't';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = 's'; *(right + 3) = 'a';
                auto res = strncmp(NChar(left, false, cs), NChar(right, false, cs), U32x1(5));
                WASM_CHECK(res > 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare partial due to argument length, equal")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(4);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 's'; *(left + 3) = 'a';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = 's'; *(right + 3) = 't';
                auto res = strncmp(NChar(left, false, cs), NChar(right, false, cs), U32x1(3));
                WASM_CHECK(res == 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare partial due to string length, equal")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(4);
                *left = 'T'; *(left + 1) = '\0'; *(left + 2) = 's'; *(left + 3) = 'a';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = '\0'; *(right + 2) = 's'; *(right + 3) = 't';
                auto res = strncmp(NChar(left, false, cs), NChar(right, false, cs), U32x1(5));
                WASM_CHECK(res == 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare partial due to argument length, less")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(4);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 'a'; *(left + 3) = 't';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = 's'; *(right + 3) = 'a';
                auto res = strncmp(NChar(left, false, cs), NChar(right, false, cs), U32x1(3));
                WASM_CHECK(res < 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare partial due to string length, less")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(4);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 's'; *(left + 3) = '\0';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = 's'; *(right + 3) = 't';
                auto res = strncmp(NChar(left, false, cs), NChar(right, false, cs), U32x1(5));
                WASM_CHECK(res < 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare partial due to argument length, greater")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(4);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 's'; *(left + 3) = 'a';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = 'a'; *(right + 3) = 't';
                auto res = strncmp(NChar(left, false, cs), NChar(right, false, cs), U32x1(3));
                WASM_CHECK(res > 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare partial due to string length, greater")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(4);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 's'; *(left + 3) = 't';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = '\0'; *(right + 3) = 't';
                auto res = strncmp(NChar(left, false, cs), NChar(right, false, cs), U32x1(5));
                WASM_CHECK(res > 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }
    }

    SECTION("strncmp with non-equal-length chars")
    {
        auto cs_left = m::Type::Get_Char(m::Type::TY_Scalar, 3);
        auto cs_right = m::Type::Get_Char(m::Type::TY_Scalar, 4);

        SECTION("compare all, equal")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(3);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 's';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = 's'; *(right + 3) = '\0';
                auto res = strncmp(NChar(left, false, cs_left), NChar(right, false, cs_right), U32x1(5));
                WASM_CHECK(res == 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare all, less")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(3);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 's';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = 's'; *(right + 3) = 't';
                auto res = strncmp(NChar(left, false, cs_left), NChar(right, false, cs_right), U32x1(5));
                WASM_CHECK(res < 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare all, greater")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(4);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 's'; *(left + 3) = 't';
                auto right = Module::Allocator().malloc<char>(3);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = 's';
                auto res = strncmp(NChar(left, false, cs_right), NChar(right, false, cs_left), U32x1(5));
                WASM_CHECK(res > 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }
    }

    SECTION("strncmp with varchars")
    {
        auto cs = m::Type::Get_Varchar(m::Type::TY_Scalar, 4);

        SECTION("compare all, equal")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(4);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 's'; *(left + 3) = '\0';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = 's'; *(right + 3) = '\0';
                auto res = strncmp(NChar(left, false, cs), NChar(right, false, cs), U32x1(5));
                WASM_CHECK(res == 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare all, less")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(4);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 'a'; *(left + 3) = '\0';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = 's'; *(right + 3) = '\0';
                auto res = strncmp(NChar(left, false, cs), NChar(right, false, cs), U32x1(5));
                WASM_CHECK(res < 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare all, greater")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(4);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 't'; *(left + 3) = '\0';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = 's'; *(right + 3) = '\0';
                auto res = strncmp(NChar(left, false, cs), NChar(right, false, cs), U32x1(5));
                WASM_CHECK(res > 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare partial due to argument length, equal")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(4);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 'a'; *(left + 3) = '\0';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = 's'; *(right + 3) = '\0';
                auto res = strncmp(NChar(left, false, cs), NChar(right, false, cs), U32x1(2));
                WASM_CHECK(res == 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare partial due to string length, equal")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(4);
                *left = 'T'; *(left + 1) = '\0'; *(left + 2) = 's'; *(left + 3) = 'a';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = '\0'; *(right + 2) = 's'; *(right + 3) = 't';
                auto res = strncmp(NChar(left, false, cs), NChar(right, false, cs), U32x1(5));
                WASM_CHECK(res == 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare partial due to argument length, less")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(4);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 's'; *(left + 3) = '\0';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 's'; *(right + 2) = 's'; *(right + 3) = '\0';
                auto res = strncmp(NChar(left, false, cs), NChar(right, false, cs), U32x1(2));
                WASM_CHECK(res < 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare partial due to string length, less")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(4);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = '\0'; *(left + 3) = 'a';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = 's'; *(right + 3) = '\0';
                auto res = strncmp(NChar(left, false, cs), NChar(right, false, cs), U32x1(5));
                WASM_CHECK(res < 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare partial due to argument length, greater")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(4);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 's'; *(left + 3) = '\0';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'a'; *(right + 2) = 's'; *(right + 3) = '\0';
                auto res = strncmp(NChar(left, false, cs), NChar(right, false, cs), U32x1(2));
                WASM_CHECK(res > 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare partial due to string length, greater")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(4);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 's'; *(left + 3) = '\0';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = '\0'; *(right + 3) = 't';
                auto res = strncmp(NChar(left, false, cs), NChar(right, false, cs), U32x1(5));
                WASM_CHECK(res > 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }
    }

    SECTION("strcmp")
    {
        auto cs_left = m::Type::Get_Char(m::Type::TY_Scalar, 3);
        auto cs_right = m::Type::Get_Char(m::Type::TY_Scalar, 4);

        SECTION("compare all, equal")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(3);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 's';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = 's'; *(right + 3) = '\0';
                auto res = strcmp(NChar(left, false, cs_left), NChar(right, false, cs_right));
                WASM_CHECK(res == 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare all, less")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(3);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 's';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = 's'; *(right + 3) = 't';
                auto res = strcmp(NChar(left, false, cs_left), NChar(right, false, cs_right));
                WASM_CHECK(res < 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare all, greater")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(4);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 's'; *(left + 3) = 't';
                auto right = Module::Allocator().malloc<char>(3);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = 's';
                auto res = strcmp(NChar(left, false, cs_right), NChar(right, false, cs_left));
                WASM_CHECK(res > 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare partial, equal")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(3);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = '\0';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = '\0'; *(right + 3) = 't';
                auto res = strcmp(NChar(left, false, cs_left), NChar(right, false, cs_right));
                WASM_CHECK(res == 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare partial, less")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(3);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = '\0';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = 's'; *(right + 3) = 't';
                auto res = strcmp(NChar(left, false, cs_left), NChar(right, false, cs_right));
                WASM_CHECK(res < 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }

        SECTION("compare all, greater")
        {
            FUNCTION(test, void(void)) {
                auto left = Module::Allocator().malloc<char>(3);
                *left = 'T'; *(left + 1) = 'e'; *(left + 2) = 's';
                auto right = Module::Allocator().malloc<char>(4);
                *right = 'T'; *(right + 1) = 'e'; *(right + 2) = '\0'; *(right + 3) = 't';
                auto res = strcmp(NChar(left, false, cs_left), NChar(right, false, cs_right));
                WASM_CHECK(res > 0, "result mismatch");
            }
            REQUIRE_NOTHROW(INVOKE(test));
        }
    }

    CodeGenContext::Dispose();
    Module::Dispose();
}

TEST_CASE("Wasm/" BACKEND_NAME "/strncpy", "[core][wasm]")
{
    Module::Init();
    CodeGenContext::Init();

    SECTION("copy all, terminated")
    {
        FUNCTION(test, void(void)) {
            auto src = Module::Allocator().malloc<char>(5);
            *src = 'T'; *(src + 1) = 'e'; *(src + 2) = 's'; *(src + 3) = 't'; *(src + 4) = '\0';
            auto dst = Module::Allocator().malloc<char>(5);
            *dst = 'f'; *(dst + 1) = 'a'; *(dst + 2) = 'i'; *(dst + 3) = 'l'; *(dst + 4) = '!';
            auto res = strncpy(dst, src, U32x1(5));
            WASM_CHECK(res == dst + 4, "result mismatch");
            check_string("Test", dst, 5, "string mismatch");
        }
        REQUIRE_NOTHROW(INVOKE(test));
    }

    SECTION("copy all, non-termianted")
    {
        FUNCTION(test, void(void)) {
            auto src = Module::Allocator().malloc<char>(4);
            *src = 'T'; *(src + 1) = 'e'; *(src + 2) = 's'; *(src + 3) = 't';
            auto dst = Module::Allocator().malloc<char>(5);
            *dst = 'f'; *(dst + 1) = 'a'; *(dst + 2) = 'i'; *(dst + 3) = 'l'; *(dst + 4) = '!';
            auto res = strncpy(dst, src, U32x1(4));
            WASM_CHECK(res == dst + 4, "result mismatch");
            check_string("Test!", dst, 5, "string mismatch");
        }
        REQUIRE_NOTHROW(INVOKE(test));
    }

    SECTION("copy partial")
    {
        FUNCTION(test, void(void)) {
            auto src = Module::Allocator().malloc<char>(5);
            *src = 'T'; *(src + 1) = 'e'; *(src + 2) = 's'; *(src + 3) = 't'; *(src + 4) = '\0';
            auto dst = Module::Allocator().malloc<char>(5);
            *dst = 'f'; *(dst + 1) = 'a'; *(dst + 2) = 'i'; *(dst + 3) = 'l'; *(dst + 4) = '!';
            auto res = strncpy(dst, src, U32x1(3));
            WASM_CHECK(res == dst + 3, "result mismatch");
            check_string("Tesl!", dst, 5, "string mismatch");
        }
        REQUIRE_NOTHROW(INVOKE(test));
    }

    SECTION("copy nothing")
    {
        FUNCTION(test, void(void)) {
            auto src = Module::Allocator().malloc<char>(5);
            *src = 'T'; *(src + 1) = 'e'; *(src + 2) = 's'; *(src + 3) = 't'; *(src + 4) = '\0';
            auto dst = Module::Allocator().malloc<char>(5);
            *dst = 'f'; *(dst + 1) = 'a'; *(dst + 2) = 'i'; *(dst + 3) = 'l'; *(dst + 4) = '!';
            auto res = strncpy(dst, src, U32x1(0));
            WASM_CHECK(res == dst, "result mismatch");
            check_string("fail!", dst, 5, "string mismatch");
        }
        REQUIRE_NOTHROW(INVOKE(test));
    }

    CodeGenContext::Dispose();
    Module::Dispose();
}

TEMPLATE_TEST_CASE("Wasm/" BACKEND_NAME "/Decimal", "[core][wasm]",
    Decimal32, Decimal64)
{
    Module::Init();

    using decimal = TestType;
    using arithmetic_type = typename decimal::arithmetic_type;

#define A decimal( 142, 2)
#define B decimal(1337, 2)
#define C decimal(-256, 2)
#define D decimal( 113, 3)
#define E decimal(  12, 1)

#define I PrimitiveExpr<arithmetic_type>(13)

    SECTION("add")
    {
        CHECK_RESULT_INLINE(1479, arithmetic_type(void), { RETURN((A + B).val()); });
        CHECK_RESULT_INLINE(1479, arithmetic_type(void), { RETURN((B + A).val()); });

        CHECK_RESULT_INLINE(-114, arithmetic_type(void), { RETURN((A + C).val()); });
        CHECK_RESULT_INLINE(-114, arithmetic_type(void), { RETURN((C + A).val()); });

        CHECK_RESULT_INLINE(1533, arithmetic_type(void), { RETURN((A + D).val()); });
        CHECK_RESULT_INLINE(1533, arithmetic_type(void), { RETURN((D + A).val()); });

        CHECK_RESULT_INLINE(262, arithmetic_type(void), { RETURN((A + E).val()); });
        CHECK_RESULT_INLINE(262, arithmetic_type(void), { RETURN((E + A).val()); });

        CHECK_RESULT_INLINE(1313, arithmetic_type(void), { RETURN((D + E).val()); });
        CHECK_RESULT_INLINE(1313, arithmetic_type(void), { RETURN((E + D).val()); });

        CHECK_RESULT_INLINE(-2447, arithmetic_type(void), { RETURN((C + D).val()); });
        CHECK_RESULT_INLINE(-2447, arithmetic_type(void), { RETURN((D + C).val()); });

        CHECK_RESULT_INLINE(-136, arithmetic_type(void), { RETURN((C + E).val()); });
        CHECK_RESULT_INLINE(-136, arithmetic_type(void), { RETURN((E + C).val()); });

        CHECK_RESULT_INLINE(1442, arithmetic_type(void), { RETURN((A + I).val()); });
        CHECK_RESULT_INLINE(1442, arithmetic_type(void), { RETURN((I + A).val()); });
    }

    SECTION("subtract")
    {
        CHECK_RESULT_INLINE(-1195, arithmetic_type(void), { RETURN((A - B).val()); });
        CHECK_RESULT_INLINE(1195, arithmetic_type(void), { RETURN((B - A).val()); });

        CHECK_RESULT_INLINE(398, arithmetic_type(void), { RETURN((A - C).val()); });
        CHECK_RESULT_INLINE(-398, arithmetic_type(void), { RETURN((C - A).val()); });

        CHECK_RESULT_INLINE(1307, arithmetic_type(void), { RETURN((A - D).val()); });
        CHECK_RESULT_INLINE(-1307, arithmetic_type(void), { RETURN((D - A).val()); });

        CHECK_RESULT_INLINE(22, arithmetic_type(void), { RETURN((A - E).val()); });
        CHECK_RESULT_INLINE(-22, arithmetic_type(void), { RETURN((E - A).val()); });

        CHECK_RESULT_INLINE(-1087, arithmetic_type(void), { RETURN((D - E).val()); });
        CHECK_RESULT_INLINE(1087, arithmetic_type(void), { RETURN((E - D).val()); });

        CHECK_RESULT_INLINE(-2673, arithmetic_type(void), { RETURN((C - D).val()); });
        CHECK_RESULT_INLINE(2673, arithmetic_type(void), { RETURN((D - C).val()); });

        CHECK_RESULT_INLINE(-376, arithmetic_type(void), { RETURN((C - E).val()); });
        CHECK_RESULT_INLINE(376, arithmetic_type(void), { RETURN((E - C).val()); });

        CHECK_RESULT_INLINE(-1158, arithmetic_type(void), { RETURN((A - I).val()); });
        CHECK_RESULT_INLINE(1158, arithmetic_type(void), { RETURN((I - A).val()); });
    }

    SECTION("multiply")
    {
        CHECK_RESULT_INLINE(1898, arithmetic_type(void), { RETURN((A * B).val()); });
        CHECK_RESULT_INLINE(1898, arithmetic_type(void), { RETURN((B * A).val()); });

        CHECK_RESULT_INLINE(-363, arithmetic_type(void), { RETURN((A * C).val()); });
        CHECK_RESULT_INLINE(-363, arithmetic_type(void), { RETURN((C * A).val()); });

        CHECK_RESULT_INLINE(160, arithmetic_type(void), { RETURN((A * D).val()); });
        CHECK_RESULT_INLINE(160, arithmetic_type(void), { RETURN((D * A).val()); });

        CHECK_RESULT_INLINE(170, arithmetic_type(void), { RETURN((A * E).val()); });
        CHECK_RESULT_INLINE(170, arithmetic_type(void), { RETURN((E * A).val()); });

        CHECK_RESULT_INLINE(135, arithmetic_type(void), { RETURN((D * E).val()); });
        CHECK_RESULT_INLINE(135, arithmetic_type(void), { RETURN((E * D).val()); });

        CHECK_RESULT_INLINE(-289, arithmetic_type(void), { RETURN((C * D).val()); });
        CHECK_RESULT_INLINE(-289, arithmetic_type(void), { RETURN((D * C).val()); });

        CHECK_RESULT_INLINE(-307, arithmetic_type(void), { RETURN((C * E).val()); });
        CHECK_RESULT_INLINE(-307, arithmetic_type(void), { RETURN((E * C).val()); });

        CHECK_RESULT_INLINE(1846, arithmetic_type(void), { RETURN((A * I).val()); });
        CHECK_RESULT_INLINE(1846, arithmetic_type(void), { RETURN((I * A).val()); });
    }

    SECTION("divide")
    {
        CHECK_RESULT_INLINE(10, arithmetic_type(void), { RETURN((A / B).val()); });
        CHECK_RESULT_INLINE(941, arithmetic_type(void), { RETURN((B / A).val()); });

        CHECK_RESULT_INLINE(-55, arithmetic_type(void), { RETURN((A / C).val()); });
        CHECK_RESULT_INLINE(-180, arithmetic_type(void), { RETURN((C / A).val()); });

        CHECK_RESULT_INLINE(12566, arithmetic_type(void), { RETURN((A / D).val()); });
        CHECK_RESULT_INLINE(79, arithmetic_type(void), { RETURN((D / A).val()); });

        CHECK_RESULT_INLINE(118, arithmetic_type(void), { RETURN((A / E).val()); });
        CHECK_RESULT_INLINE(84, arithmetic_type(void), { RETURN((E / A).val()); });

        CHECK_RESULT_INLINE(94, arithmetic_type(void), { RETURN((D / E).val()); });
        CHECK_RESULT_INLINE(10619, arithmetic_type(void), { RETURN((E / D).val()); });

        CHECK_RESULT_INLINE(-22654, arithmetic_type(void), { RETURN((C / D).val()); });
        CHECK_RESULT_INLINE(-44, arithmetic_type(void), { RETURN((D / C).val()); });

        CHECK_RESULT_INLINE(-213, arithmetic_type(void), { RETURN((C / E).val()); });
        CHECK_RESULT_INLINE(-46, arithmetic_type(void), { RETURN((E / C).val()); });

        CHECK_RESULT_INLINE(10, arithmetic_type(void), { RETURN((A / I).val()); });
        CHECK_RESULT_INLINE(915, arithmetic_type(void), { RETURN((I / A).val()); });
    }

#undef A
#undef B
#undef C
#undef D
#undef E
#undef I
    Module::Dispose();
}
