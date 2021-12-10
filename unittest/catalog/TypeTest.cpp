#include "catch2/catch.hpp"

#include <mutable/catalog/Type.hpp>
#include <mutable/util/fn.hpp>
#include <cmath>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <string>


using namespace m;


TEST_CASE("Type/PrimitiveType c'tor", "[core][catalog][type]")
{
    const Boolean *scalar = Type::Get_Boolean(Type::TY_Scalar);
    const Boolean *vectorial = Type::Get_Boolean(Type::TY_Vector);

    CHECK(scalar->is_scalar());
    CHECK(not scalar->is_vectorial());

    CHECK(not vectorial->is_scalar());
    CHECK(vectorial->is_vectorial());
}

TEST_CASE("Type/Boolean", "[core][catalog][type]")
{
    const Boolean *b_scalar = Type::Get_Boolean(Type::TY_Scalar);
    const Boolean *b_vectorial = Type::Get_Boolean(Type::TY_Vector);
    REQUIRE(b_scalar != b_vectorial);
}

TEST_CASE("Type/CharacterSequence", "[core][catalog][type]")
{
    SECTION("CHAR(N)")
    {
        const CharacterSequence *chr42 = Type::Get_Char(Type::TY_Scalar, 42);
        CHECK(not chr42->is_varying);
        CHECK(chr42->length == 42);
        CHECK(chr42->size() == 42 * 8);
    }

    SECTION("VARCHAR(N)")
    {
        const CharacterSequence *chr42 = Type::Get_Varchar(Type::TY_Scalar, 42);
        CHECK(chr42->is_varying);
        CHECK(chr42->length == 42);
        CHECK(chr42->size() == 43 * 8);
    }
}

TEST_CASE("Type/Numeric c'tor", "[core][catalog][type]")
{
    using std::ceil;
    using std::log2;
    using std::pow;

    SECTION("8 byte integer")
    {
        const Numeric *i8 = Type::Get_Integer(Type::TY_Scalar, 8);
        CHECK(i8->kind == Numeric::N_Int);
        CHECK(i8->precision == 8);
        CHECK(i8->scale == 0);
    }

    SECTION("32 bit floating-point")
    {
        const Numeric *f = Type::Get_Float(Type::TY_Scalar);
        CHECK(f->kind == Numeric::N_Float);
        CHECK(f->precision == 32);
    }

    SECTION("64 bit floating-point")
    {
        const Numeric *d = Type::Get_Double(Type::TY_Scalar);
        CHECK(d->kind == Numeric::N_Float);
        CHECK(d->precision == 64);
    }

    SECTION("decimal")
    {
        unsigned decimal_precision;

        SECTION("4 digits: log₂(10^4) = 13.3")
        {
            decimal_precision = 4;
        }

        SECTION("5 digits: log₂(10^5) = 16.6")
        {
            decimal_precision = 5;
        }

        SECTION("9 digits: log₂(10^9) = 29.8")
        {
            decimal_precision = 9;
        }

        SECTION("10 digits: log₂(10^10) = 33.2")
        {
            decimal_precision = 10;
        }

        SECTION("19 digits: log₂(10^19) = 29.9")
        {
            decimal_precision = 19;
        }

        SECTION("20 digits: log₂(10^20) = 33.2")
        {
            decimal_precision = 20;
        }

        SECTION("38 digits: log₂(10^38) = 122.9")
        {
            decimal_precision = 38;
        }

        const Numeric *dec = Type::Get_Decimal(Type::TY_Scalar, decimal_precision, 2);
        CHECK(dec->kind == Numeric::N_Decimal);
        CHECK(dec->precision == decimal_precision);
        CHECK(dec->scale == 2);
    }
}

TEST_CASE("Type convert to scalar/vectorial", "[core][catalog][type]")
{
    const PrimitiveType *scalar;
    const PrimitiveType *vectorial;

    SECTION("Boolean")
    {
        scalar = Type::Get_Boolean(Type::TY_Scalar);
        vectorial = Type::Get_Boolean(Type::TY_Vector);
    }

    SECTION("CharacterSequence")
    {
        scalar = Type::Get_Char(Type::TY_Scalar, 42);
        vectorial = Type::Get_Char(Type::TY_Vector, 42);
    }

    SECTION("Numeric")
    {
        scalar = Type::Get_Integer(Type::TY_Scalar, 4);
        vectorial = Type::Get_Integer(Type::TY_Vector, 4);
    }

    REQUIRE(scalar->is_scalar());
    REQUIRE(vectorial->is_vectorial());

    const PrimitiveType *scalar_to_scalar = scalar->as_scalar();
    REQUIRE(scalar == scalar_to_scalar);

    const PrimitiveType *vec_to_vec = vectorial->as_vectorial();
    REQUIRE(vectorial == vec_to_vec);

    const PrimitiveType *scalar_to_vec = scalar->as_vectorial();
    CHECK(scalar_to_vec->is_vectorial());
    REQUIRE(scalar_to_vec == vectorial);

    const PrimitiveType *vec_to_scalar = vectorial->as_scalar();
    CHECK(vec_to_scalar->is_scalar());
    REQUIRE(vec_to_scalar == scalar);
}


TEST_CASE("Type/Numeric print()", "[core][catalog][type]")
{
    std::ostringstream oss;

    /* 8 byte integer */
    const Numeric *i8 = Type::Get_Integer(Type::TY_Scalar, 8);
    oss << *i8;
    CHECK(oss.str() == "INT(8)");
    oss.str("");

    /* 32 bit floating-point */
    const Numeric *f = Type::Get_Float(Type::TY_Scalar);
    oss << *f;
    CHECK(oss.str() == "FLOAT");
    oss.str("");

    /* 64 bit floating-point */
    const Numeric *d = Type::Get_Double(Type::TY_Scalar);
    oss << *d;
    CHECK(oss.str() == "DOUBLE");
    oss.str("");

    /* 4 byte decimal: log2(10^9) = 29.8 */
    const Numeric *dec_9_2 = Type::Get_Decimal(Type::TY_Scalar, 9, 2);
    oss << *dec_9_2;
    CHECK(oss.str() == "DECIMAL(9, 2)");
    oss.str("");

    /* 10 digits: log2(10^10) = 33.2 -> 64 bit */
    const Numeric *dec_10_0 = Type::Get_Decimal(Type::TY_Scalar, 10, 0);
    oss << *dec_10_0;
    CHECK(oss.str() == "DECIMAL(10, 0)");
    oss.str("");

    /* 16 byte decimal: log2(10^38) - 122.9 */
    const Numeric *dec_38_20 = Type::Get_Decimal(Type::TY_Scalar, 38, 20);
    oss << *dec_38_20;
    CHECK(oss.str() == "DECIMAL(38, 20)");
    oss.str("");
}

TEST_CASE("Type internalize", "[core][catalog][type]")
{
    SECTION("Boolean")
    {
        auto b = Type::Get_Boolean(Type::TY_Scalar);
        auto b_ = Type::Get_Boolean(Type::TY_Scalar);
        REQUIRE(b == b_);
    }

    SECTION("CharacterSequence")
    {
        auto vc42 = Type::Get_Varchar(Type::TY_Scalar, 42);
        auto vc42_ = Type::Get_Varchar(Type::TY_Scalar, 42);
        auto c42 = Type::Get_Char(Type::TY_Scalar, 42);

        REQUIRE(vc42 == vc42_);
        REQUIRE(vc42 != c42);
    }

    SECTION("Numeric")
    {
        auto i4 = Type::Get_Integer(Type::TY_Scalar, 4);
        auto i4_ = Type::Get_Integer(Type::TY_Scalar, 4);
        auto i8 = Type::Get_Integer(Type::TY_Scalar, 8);
        auto f = Type::Get_Float(Type::TY_Scalar);
        auto f_ = Type::Get_Float(Type::TY_Scalar);
        auto d = Type::Get_Double(Type::TY_Scalar);
        auto d_ = Type::Get_Double(Type::TY_Scalar);
        auto dec_9_2 = Type::Get_Decimal(Type::TY_Scalar, 9, 2);
        auto dec_9_2_ = Type::Get_Decimal(Type::TY_Scalar, 9, 2);
        auto dec_10_0 = Type::Get_Decimal(Type::TY_Scalar, 10, 0);

        REQUIRE(i4 == i4_);
        REQUIRE(i4 != i8);
        REQUIRE(f == f_);
        REQUIRE(f != d);
        REQUIRE(d == d_);
        REQUIRE(dec_9_2 == dec_9_2_);
        REQUIRE(dec_9_2 != dec_10_0);
    }
}

TEST_CASE("Type compatibility", "[core][catalog][type]")
{
    /* `PrimitiveType`s. */
    auto i2 = Type::Get_Integer(Type::TY_Scalar, 2);
    auto i4 = Type::Get_Integer(Type::TY_Scalar, 4);
    auto i4_ = Type::Get_Integer(Type::TY_Scalar, 4);
    auto f = Type::Get_Float(Type::TY_Scalar);
    auto f_ = Type::Get_Float(Type::TY_Scalar);
    auto d = Type::Get_Double(Type::TY_Scalar);
    auto d_ = Type::Get_Double(Type::TY_Scalar);
    auto dec_9_2 = Type::Get_Decimal(Type::TY_Scalar, 9, 2);
    auto dec_9_2_ = Type::Get_Decimal(Type::TY_Scalar, 9, 2);
    auto b = Type::Get_Boolean(Type::TY_Scalar);
    auto b_ = Type::Get_Boolean(Type::TY_Scalar);
    auto vc42 = Type::Get_Varchar(Type::TY_Scalar, 42);
    auto vc42_ = Type::Get_Varchar(Type::TY_Scalar, 42);
    auto c42 = Type::Get_Char(Type::TY_Scalar, 42);
    auto c42_ = Type::Get_Char(Type::TY_Scalar, 42);

    /* `ErrorType`. */
    auto e = Type::Get_Error();
    auto e_ = Type::Get_Error();

    /* `NoneType`. */
    auto n = Type::Get_None();
    auto n_ = Type::Get_None();

    /* `FnType`. */
    auto return_type = Type::Get_Integer(Type::TY_Scalar, 8);
    std::vector<const Type*> param_types;
    auto fn = Type::Get_Function(return_type, param_types);
    auto fn_ = Type::Get_Function(return_type, param_types);

    SECTION("Integer")
    {
        REQUIRE(is_comparable(i4, i4_));
        REQUIRE(is_comparable(i4, i2));
        REQUIRE(is_comparable(i4, f));
        REQUIRE(is_comparable(i4, d));
        REQUIRE(is_comparable(i4, dec_9_2));
        REQUIRE_FALSE(is_comparable(i4, b));
        REQUIRE_FALSE(is_comparable(i4, vc42));
        REQUIRE_FALSE(is_comparable(i4, c42));
        REQUIRE_FALSE(is_comparable(i4, e));
        REQUIRE_FALSE(is_comparable(i4, n));
        REQUIRE_FALSE(is_comparable(i4, fn));
    }

    SECTION("Float")
    {
        REQUIRE(is_comparable(f, f_));
        REQUIRE(is_comparable(f, d));
        REQUIRE(is_comparable(f, dec_9_2));
        REQUIRE_FALSE(is_comparable(f, b));
        REQUIRE_FALSE(is_comparable(f, vc42));
        REQUIRE_FALSE(is_comparable(f, c42));
        REQUIRE_FALSE(is_comparable(f, e));
        REQUIRE_FALSE(is_comparable(f, n));
        REQUIRE_FALSE(is_comparable(f, fn));
    }

    SECTION("Double")
    {
        REQUIRE(is_comparable(d, d_));
        REQUIRE(is_comparable(d, dec_9_2));
        REQUIRE_FALSE(is_comparable(d, b));
        REQUIRE_FALSE(is_comparable(d, vc42));
        REQUIRE_FALSE(is_comparable(d, c42));
        REQUIRE_FALSE(is_comparable(d, e));
        REQUIRE_FALSE(is_comparable(d, n));
        REQUIRE_FALSE(is_comparable(d, fn));
    }

    SECTION("Decimal")
    {
        REQUIRE(is_comparable(dec_9_2, dec_9_2_));
        REQUIRE_FALSE(is_comparable(dec_9_2, b));
        REQUIRE_FALSE(is_comparable(dec_9_2, vc42));
        REQUIRE_FALSE(is_comparable(dec_9_2, c42));
        REQUIRE_FALSE(is_comparable(dec_9_2, e));
        REQUIRE_FALSE(is_comparable(dec_9_2, n));
        REQUIRE_FALSE(is_comparable(dec_9_2, fn));
    }

    SECTION("Boolean")
    {
        REQUIRE(is_comparable(b, b_));
        REQUIRE_FALSE(is_comparable(b, vc42));
        REQUIRE_FALSE(is_comparable(b, c42));
        REQUIRE_FALSE(is_comparable(b, e));
        REQUIRE_FALSE(is_comparable(b, n));
        REQUIRE_FALSE(is_comparable(b, fn));
    }

    SECTION("Varchar")
    {
        REQUIRE(is_comparable(vc42, vc42_));
        REQUIRE(is_comparable(vc42, c42_));
        REQUIRE_FALSE(is_comparable(vc42, e));
        REQUIRE_FALSE(is_comparable(vc42, n));
        REQUIRE_FALSE(is_comparable(vc42, fn));
    }

    SECTION("CharacterSequence")
    {
        REQUIRE(is_comparable(c42, c42_));
        REQUIRE_FALSE(is_comparable(c42, e));
        REQUIRE_FALSE(is_comparable(c42, n));
        REQUIRE_FALSE(is_comparable(c42, fn));
    }

    SECTION("Error")
    {
        REQUIRE_FALSE(is_comparable(e, e_));
        REQUIRE_FALSE(is_comparable(e, n));
        REQUIRE_FALSE(is_comparable(e, fn));
    }

    SECTION("None")
    {
        REQUIRE_FALSE(is_comparable(n, n_));
        REQUIRE_FALSE(is_comparable(n, fn));
    }

    SECTION("Function")
    {
        REQUIRE_FALSE(is_comparable(fn, fn_));
    }
}

