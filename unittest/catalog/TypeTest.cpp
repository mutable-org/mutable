#include "catch.hpp"

#include "catalog/Type.hpp"
#include "util/fn.hpp"
#include <cmath>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <string>


using namespace db;


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
    }

    SECTION("VARCHAR(N)")
    {
        const CharacterSequence *chr42 = Type::Get_Varchar(Type::TY_Scalar, 42);
        CHECK(chr42->is_varying);
        CHECK(chr42->length == 42);
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

