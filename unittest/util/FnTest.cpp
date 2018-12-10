#include "catch.hpp"

#include "util/fn.hpp"
#include <cstdlib>
#include <cstring>


TEST_CASE("streq", "[unit][util]")
{
    const char *s0 = "Hello, World";
    const char *s1 = strdup(s0);
    const char *s2 = "The quick brown fox";
    const char *s3 = "The quick brown";

    REQUIRE(streq(s0, s0));
    REQUIRE(streq(s0, s1));
    REQUIRE(streq(s1, s0));
    REQUIRE(not streq(s0, s2));
    REQUIRE(not streq(s2, s0));

    REQUIRE(not streq(s2, s3));
    REQUIRE(strneq(s2, s3, strlen(s3)));
    REQUIRE(not strneq(s2, s3, strlen(s2)));
    REQUIRE(not strneq(s2, s3, strlen(s3) + 42));

    free((void*) s1);
}
