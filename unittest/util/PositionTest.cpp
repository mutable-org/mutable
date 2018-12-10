#include "catch.hpp"

#include "util/Position.hpp"
#include <cstring>


using namespace db;



TEST_CASE("Position c'tor", "[unit][util]")
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
