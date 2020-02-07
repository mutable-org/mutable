#include "catch.hpp"

#include "util/StringPool.hpp"


using namespace db;


TEST_CASE("StringPool c'tor", "[core][util][stringpool]")
{
    StringPool pool(42);
    REQUIRE(pool.size() == 0);
}

TEST_CASE("StringPool internalize", "[core][util][stringpool]")
{
    StringPool pool;

    REQUIRE(pool.size() == 0);

    auto s0 = pool("Hello");
    REQUIRE(pool.size() == 1);

    auto s1 = pool("hello");
    REQUIRE(pool.size() == 2);
    REQUIRE(s0 != s1);

    auto s2 = pool("hello");
    REQUIRE(pool.size() == 2);
    REQUIRE(s1 == s2);
}
