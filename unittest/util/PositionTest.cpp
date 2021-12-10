#include "catch2/catch.hpp"

#include <mutable/util/Position.hpp>
#include <cstring>


using namespace m;


TEST_CASE("Position c'tor", "[core][util][Position]")
{
    SECTION("c'tor by name")
    {
        Position start("the_file");

        REQUIRE(strcmp(start.name, "the_file") == 0);
        REQUIRE(start.line == 0);
        REQUIRE(start.column == 0);
    }

    SECTION("c'tor by name, line, and column")
    {
        Position start("the_file", 13, 42);

        REQUIRE(strcmp(start.name, "the_file") == 0);
        REQUIRE(start.line == 13);
        REQUIRE(start.column == 42);
    }
}

TEST_CASE("Position compare","[core][util][Position]")
{
    SECTION("compare check name, line, and column using == and != operator")
    {
        Position p1("the_file", 18, 50);
        Position p2("the_file", 18, 50);

        REQUIRE(p1 == p2);
        REQUIRE_FALSE(p1 != p2);
    }

    SECTION("compare check name using == and != operator")
    {
        Position p1("the_file", 18, 50);
        Position p2("the_files", 19, 51);

        REQUIRE_FALSE(p1 == p2);
        REQUIRE(p1 != p2);
    }

    SECTION("compare check name, line, and column using == and != operator")
    {
        Position p1("the_file");
        Position p2("the_file");

        REQUIRE(p1 == p2);
        REQUIRE_FALSE(p1 != p2);
    }

    SECTION("compare check name using == and != operator")
    {
        Position p1("the_file");
        Position p2("the_files");

        REQUIRE_FALSE(p1 == p2);
        REQUIRE(p1 != p2);
    }
}
