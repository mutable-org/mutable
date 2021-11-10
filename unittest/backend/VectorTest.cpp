#include "catch2/catch.hpp"

#include "backend/Vector.hpp"


using namespace m;


TEST_CASE("Vector", "[core][backend][vector]")
{
    using Vec = Vector<64>;
    std::string str = "The quick brown fox jumps over the fence";

    Table schema;
    schema.push_back("i32", Type::Get_Integer(Type::TY_Vector, 32));
    schema.push_back("null", Type::Get_None(Type::TY_Vector));
    schema.push_back("f", Type::Get_Float(Type::TY_Vector));
    schema.push_back("s", Type::Get_Char(Type::TY_Vector, 20));
    schema.push_back("b", Type::Get_Boolean(Type::TY_Vector));

    SECTION("c'tor with empty schema")
    {
        Table empty("empty");
        Vec vec(empty);
        REQUIRE(vec.size() == 0);
        REQUIRE(vec.mask() == 0);
        REQUIRE(vec.capacity() == 64);
    }

    SECTION("c'tor with attr schema")
    {
        Vec vec(schema);
        REQUIRE(vec.size() == 0);
        REQUIRE(vec.mask() == 0);
        REQUIRE(vec.capacity() == 64);
    }

    SECTION("fill()")
    {
        Vec vec(schema);
        vec.fill();
        REQUIRE(vec.size() == vec.capacity());
        REQUIRE(vec.mask() == 0xFFFFFFFFFFFFFFFFUL);
    }

    SECTION("mask()")
    {
        Vec vec(schema);
        vec.mask(0b100101);
        REQUIRE(vec.size() == 3);
        REQUIRE(vec.mask() == 0b100101);
    }

    SECTION("iterator")
    {
        Vec vec(schema);
        vec.mask(0b100101);

        SECTION("begin()/end()")
        {
            auto it = vec.begin();
            REQUIRE(it.index() == 0);
            ++it;
            REQUIRE(it.index() == 2);
            ++it;
            REQUIRE(it.index() == 5);
            ++it;
            REQUIRE(it == vec.end());
        }

        SECTION("at()")
        {
            auto it = vec.at(0);
            REQUIRE(it.index() == 0);
            it = vec.at(2);
            REQUIRE(it.index() == 2);
            it = vec.at(5);
            REQUIRE(it.index() == 5);
            it = vec.at(1);
            REQUIRE(it.index() == 1); // iterator exactly where we asked for
            ++it;
            REQUIRE(it.index() == 2); // iterator advanced to next live value
            ++it;
            REQUIRE(it.index() == 5); // iterator advanced to next live value
            it = vec.at(3);
            REQUIRE(it.index() == 3); // iterator exactly where we asked for
            ++it;
            REQUIRE(it.index() == 5); // iterator advanced to next live value
        }

        SECTION("is_null()/set_null()")
        {
            auto it = vec.begin();
            it.set_null(0, true);
            it.set_null(1, true);
            it.set_null(2, true);
            it.set_null(3, true);
            REQUIRE(it.is_null(0));
            REQUIRE(it.is_null(1));
            REQUIRE(it.is_null(2));
            REQUIRE(it.is_null(3));
        }

        SECTION("set()/get()")
        {
            auto it = vec.begin();

            it.set_i32(0, 42);
            REQUIRE(not it.is_null(0));
            REQUIRE(it.get_i32(0) == 42);

            REQUIRE(it.is_null(1));

            it.set_f(2, 3.14f);
            REQUIRE(not it.is_null(2));
            REQUIRE(it.get_f(2) == 3.14f);

            it.set_s(3, str);
            REQUIRE(not it.is_null(3));
            REQUIRE(it.get_s(3) == str.substr(0, 20));

            it.set_b(4, true);
            REQUIRE(not it.is_null(4));
            REQUIRE(it.get_b(4) == true);
        }
    }
}
