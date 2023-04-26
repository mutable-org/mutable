#include "catch2/catch.hpp"

#include <mutable/catalog/Schema.hpp>
#include <mutable/IR/Tuple.hpp>


using namespace m;


/*======================================================================================================================
 * Value
 *====================================================================================================================*/

#ifdef M_ENABLE_SANITY_FIELDS
TEST_CASE("Value/default c'tor", "[core][storage][Value]")
{
    Value val;
    REQUIRE(val.type == Value::VNone);
}
#endif

TEST_CASE("Value/value c'tor", "[core][storage][Value]")
{
    SECTION("b")
    {
        Value val(true);
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(val.type == Value::Vb);
#endif
        REQUIRE(val.as_b() == true);
    }
    SECTION("i32")
    {
        Value val(42);
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(val.type == Value::Vi);
#endif
        REQUIRE(val.as_i() == 42);
    }
    SECTION("i64")
    {
        Value val(1337L);
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(val.type == Value::Vi);
#endif
        REQUIRE(val.as_i() == 1337L);
    }
    SECTION("u64")
    {
        Value val(1337UL);
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(val.type == Value::Vi);
#endif
        REQUIRE(val.as_i() == 1337L);
    }
    SECTION("f")
    {
        Value val(3.14f);
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(val.type == Value::Vf);
#endif
        REQUIRE(val.as_f() == 3.14f);
    }
    SECTION("d")
    {
        Value val(3.14159);
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(val.type == Value::Vd);
#endif
        REQUIRE(val.as_d() == 3.14159);
    }
    SECTION("p")
    {
        int x;
        Value val(&x);
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(val.type == Value::Vp);
#endif
        REQUIRE(val.as_p() == &x);
    }
}

TEST_CASE("Value/conversion", "[core][storage][Value]")
{
    SECTION("b")
    {
        Value val(true);
        REQUIRE(bool(val) == true);
    }
    SECTION("i32")
    {
        Value val(42);
        REQUIRE(int32_t(val) == 42);
    }
    SECTION("i64")
    {
        Value val(1337L);
        REQUIRE(int64_t(val) == 1337L);
    }
    SECTION("u64")
    {
        Value val(1337UL);
        REQUIRE(int64_t(val) == 1337UL);
    }
    SECTION("f")
    {
        Value val(3.14f);
        REQUIRE(float(val) == 3.14f);
    }
    SECTION("d")
    {
        Value val(3.14159);
        REQUIRE(double(val) == 3.14159);
    }
    SECTION("p")
    {
        int x;
        Value val(&x);
        REQUIRE((const int*)(val) == &x);
    }
}

TEST_CASE("Value/assignment", "[core][storage][Value]")
{
    Value src;
    Value dst;

    SECTION("b")
    {
        src = true;
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(src.type == Value::Vb);
#endif
        REQUIRE(src.as_b() == true);

        dst = src;
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(src.type == Value::Vb);
#endif
        REQUIRE(src.as_b() == true);
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(dst.type == Value::Vb);
#endif
        REQUIRE(dst.as_b() == true);
    }
    SECTION("i32")
    {
        src = 42;
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(src.type == Value::Vi);
#endif
        REQUIRE(src.as_i() == 42);

        dst = src;
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(src.type == Value::Vi);
#endif
        REQUIRE(src.as_i() == 42);
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(dst.type == Value::Vi);
#endif
        REQUIRE(dst.as_i() == 42);
    }
    SECTION("i64")
    {
        src = 1337L;
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(src.type == Value::Vi);
#endif
        REQUIRE(src.as_i() == 1337L);

        dst = src;
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(src.type == Value::Vi);
#endif
        REQUIRE(src.as_i() == 1337L);
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(dst.type == Value::Vi);
#endif
        REQUIRE(dst.as_i() == 1337L);
    }
    SECTION("p")
    {
        int x;
        src = &x;
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(src.type == Value::Vp);
#endif
        REQUIRE(src.as_p() == &x);

        dst = src;
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(src.type == Value::Vp);
#endif
        REQUIRE(src.as_p() == &x);
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(dst.type == Value::Vp);
#endif
        REQUIRE(dst.as_p() == &x);
    }
}

TEST_CASE("Value/conversion assignment", "[core][storage][Value]")
{
    Value val;

    SECTION("b")
    {
        val = true;
        val.as_b() = false;
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(val.type == Value::Vb);
#endif
        REQUIRE(val.as_b() == false);
    }
    SECTION("i32")
    {
        val = 1337;
        val.as_i() = 42;
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(val.type == Value::Vi);
#endif
        REQUIRE(val.as_i() == 42);
    }
    SECTION("i64")
    {
        val = 42L;
        val.as_i() = 1337L;
#ifdef M_ENABLE_SANITY_FIELDS
        REQUIRE(val.type == Value::Vi);
#endif
        REQUIRE(val.as_i() == 1337L);
    }
}

TEST_CASE("Value/comparison", "[core][storage][Value]")
{
    Value b0(true);
    Value b1(true);
    Value b2(false);

    Value i0(42);
    Value i1(42);
    Value i2(1337);

    Value f0(3.14f);
    Value f1(3.14f);
    Value f2(2.72f);

    Value d0(3.14159);
    Value d1(3.14159);
    Value d2(2.71828);

    int x;
    int y;
    Value p0(&x);
    Value p1(&x);
    Value p2(&y);

    REQUIRE(b0 == b1);
    REQUIRE(b0 != b2);

    REQUIRE(i0 == i1);
    REQUIRE(i0 != i2);

    REQUIRE(f0 == f1);
    REQUIRE(f0 != f2);

    REQUIRE(d0 == d1);
    REQUIRE(d0 != d2);

    REQUIRE(p0 == p1);
    REQUIRE(p0 != p2);
}

TEST_CASE("Value/initialization", "[core][storage][Value]")
{
    SECTION("from narrow to wide")
    {
        Value val(3.14);
        val = 2.7f;
        REQUIRE(val == 2.7f);
    }

    SECTION("from wide to narrow")
    {
        Value val(2.7f);
        val = 3.14;
        REQUIRE(val == 3.14);
    }
}


/*======================================================================================================================
 * Tuple
 *====================================================================================================================*/

TEST_CASE("Tuple/c'tor", "[core][storage][Tuple]")
{
    SECTION("default")
    {
        Tuple tup;
    }

    SECTION("schema")
    {
        Schema S;
        S.add({"0"}, Type::Get_Decimal(Type::TY_Vector, 10, 2));
        S.add({"1"}, Type::Get_Float(Type::TY_Vector));
        S.add({"2"}, Type::Get_Varchar(Type::TY_Vector, 42));
        S.add({"3"}, Type::Get_Char(Type::TY_Vector, 2));
        S.add({"4"}, Type::Get_Boolean(Type::TY_Vector));
        S.add({"5"}, Type::Get_Char(Type::TY_Vector, 3));

        Tuple tup(S);
        CHECK(tup.is_null(0));
        CHECK(tup.is_null(1));
        CHECK(tup.is_null(2));
        CHECK(tup.is_null(3));
        CHECK(tup.is_null(4));

        CHECK(tup[2].as_p() == (reinterpret_cast<uint8_t*>(&tup[0]) + 6 * sizeof(Value)));
        CHECK(tup[3].as_p() == (reinterpret_cast<uint8_t*>(&tup[0]) + 6 * sizeof(Value) + 43));
        CHECK(tup[5].as_p() == (reinterpret_cast<uint8_t*>(&tup[0]) + 6 * sizeof(Value) + 43 + 3));
    }
}

TEST_CASE("Tuple::set()/get()", "[core][storage][Tuple]")
{
    Schema S;
    S.add({"0"}, Type::Get_Integer(Type::TY_Vector, 4));
    S.add({"1"}, Type::Get_Integer(Type::TY_Vector, 4));
    S.add({"2"}, Type::Get_Integer(Type::TY_Vector, 4));
    S.add({"3"}, Type::Get_Integer(Type::TY_Vector, 4));
    S.add({"4"}, Type::Get_Integer(Type::TY_Vector, 4));

    Tuple tup(S);
    Value val;

    SECTION("set value, clear NULL bit")
    {
        tup.set(0, 42);
        REQUIRE(not tup.is_null(0));
        REQUIRE(tup[0].as_i() == 42);
        REQUIRE(tup.get(0).as_i() == 42);
    }

    SECTION("set value w/ NULL bit")
    {
        tup.set(0, 42, true);
        REQUIRE(tup.is_null(0));
    }

    SECTION("set value w/o NULL bit")
    {
        tup[0] = 42;
        REQUIRE(tup.is_null(0));
    }
}

TEST_CASE("Tuple::clear()", "[core][storage][Tuple]")
{
    Schema S;
    S.add({"0"}, Type::Get_Integer(Type::TY_Vector, 4));
    S.add({"1"}, Type::Get_Integer(Type::TY_Vector, 4));
    S.add({"2"}, Type::Get_Integer(Type::TY_Vector, 4));
    S.add({"3"}, Type::Get_Integer(Type::TY_Vector, 4));
    S.add({"4"}, Type::Get_Integer(Type::TY_Vector, 4));

    Tuple tup(S);
    tup.set(0, 42);
    tup.set(1, true);
    tup.set(2, 3.14f);

    REQUIRE(not tup.is_null(0));
    REQUIRE(not tup.is_null(1));
    REQUIRE(not tup.is_null(2));
    REQUIRE(tup.is_null(3));
    REQUIRE(tup.is_null(4));

    tup.clear();
    REQUIRE(tup.is_null(0));
    REQUIRE(tup.is_null(1));
    REQUIRE(tup.is_null(2));
    REQUIRE(tup.is_null(3));
    REQUIRE(tup.is_null(4));
}

TEST_CASE("Tuple::insert()", "[core][storage][Tuple]")
{
    Schema S;
    S.add({"0"}, Type::Get_Integer(Type::TY_Vector, 4));
    S.add({"1"}, Type::Get_Integer(Type::TY_Vector, 4));
    S.add({"2"}, Type::Get_Integer(Type::TY_Vector, 4));
    S.add({"3"}, Type::Get_Integer(Type::TY_Vector, 4));
    S.add({"4"}, Type::Get_Integer(Type::TY_Vector, 4));

    Tuple dst(S);
    Tuple src(S);
    src.set(0, 42);
    src.set(2, 3.14f);

    dst.insert(src, 1, 3);
    CHECK(dst.is_null(0));
    CHECK(not dst.is_null(1));
    CHECK(dst[1] == 42);
    CHECK(dst.is_null(2));
    CHECK(not dst.is_null(3));
    CHECK(dst[3] == 3.14f);
    CHECK(dst.is_null(4));
}

TEST_CASE("Tuple/comparison", "[core][storage][Tuple]")
{
    Schema S;
    S.add({"0"}, Type::Get_Integer(Type::TY_Vector, 4));
    S.add({"1"}, Type::Get_Integer(Type::TY_Vector, 4));
    S.add({"2"}, Type::Get_Integer(Type::TY_Vector, 4));
    S.add({"3"}, Type::Get_Integer(Type::TY_Vector, 4));
    S.add({"4"}, Type::Get_Integer(Type::TY_Vector, 4));

    Tuple first(S);
    first.set(1, 42);
    first.set(2, true);
    first.set(3, 3.14f);

    Tuple second(S);
    second.set(1, 42);
    second.set(2, true);
    second.set(3, 3.14f);

    Tuple third(S);
    third.set(0, false);
    third.set(1, 42);
    third.set(2, true);
    third.set(3, 3.14f);

    Tuple fourth(S);
    fourth.set(1, 42);
    fourth.set(2, true);

    REQUIRE(first == second);
    REQUIRE(first != third);
    REQUIRE(first != fourth);
    REQUIRE(third != fourth);
}
