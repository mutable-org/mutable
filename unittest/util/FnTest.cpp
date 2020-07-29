#include "catch.hpp"

#include "util/fn.hpp"
#include <cstdlib>
#include <cstring>


TEST_CASE("streq", "[core][util][fn]")
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

TEST_CASE("ceil_to_pow_2", "[core][util][fn]")
{
    uint32_t u31 = 1U << 31;
    uint64_t u63 = 1UL << 63;

    CHECK(1 == ceil_to_pow_2(1U));
    CHECK(2 == ceil_to_pow_2(2U));
    CHECK(4 == ceil_to_pow_2(3U));
    CHECK(4 == ceil_to_pow_2(4U));
    CHECK(8 == ceil_to_pow_2(5U));
    CHECK(u31 == ceil_to_pow_2(u31 - 1U));
    CHECK(u31 == ceil_to_pow_2(u31));
    CHECK(u63 == ceil_to_pow_2(u63 - 1UL));
    CHECK(u63 == ceil_to_pow_2(u63));
}

TEST_CASE("round_up_to_multiple", "[core][util][fn]")
{
    CHECK(0 == round_up_to_multiple(0U, 1U));
    CHECK(1 == round_up_to_multiple(1U, 1U));
    CHECK(2 == round_up_to_multiple(2U, 1U));
    CHECK(0 == round_up_to_multiple(0U, 2U));
    CHECK(2 == round_up_to_multiple(1U, 2U));
    CHECK(2 == round_up_to_multiple(2U, 2U));
    CHECK(4 == round_up_to_multiple(3U, 2U));
}

TEST_CASE("log2", "[core][util][fn]")
{
    SECTION("floor")
    {
        CHECK( 0 == log2_floor(1U));
        CHECK( 1 == log2_floor(2U));
        CHECK( 1 == log2_floor(3U));
        CHECK( 2 == log2_floor(4U));
        CHECK( 9 == log2_floor(1023U));
        CHECK(10 == log2_floor(1024U));
        CHECK(10 == log2_floor(1025U));
    }

    SECTION("ceil")
    {
        CHECK( 0 == log2_ceil(1U));
        CHECK( 1 == log2_ceil(2U));
        CHECK( 2 == log2_ceil(3U));
        CHECK( 2 == log2_ceil(4U));
        CHECK(10 == log2_ceil(1023U));
        CHECK(10 == log2_ceil(1024U));
        CHECK(11 == log2_ceil(1025U));
    }
}

TEST_CASE("powi", "[core][util][fn]")
{
    REQUIRE(powi(4, 0) == 1);
    REQUIRE(powi(4, 1) == 4);
    REQUIRE(powi(4, 2) == 16);
    REQUIRE(powi(4, 3) == 64);
    REQUIRE(powi(4, 4) == 256);
    REQUIRE(powi(4, 5) == 1024);
}

TEST_CASE("subsets/least_subset, next_subset", "[core][util][fn]")
{
    uint64_t set = 10;

    REQUIRE(least_subset(set) == 2);
    REQUIRE(next_subset(2, set) == 8);
    REQUIRE(next_subset(8, set) == set);
    REQUIRE(next_subset(10, set) == 0);
}

TEST_CASE("sum_wo_overflow", "[core][util][fn]")
{
    uint64_t UL_MAX = std::numeric_limits<uint64_t>::max();
    uint64_t U_MAX = std::numeric_limits<uint64_t>::max();

    REQUIRE(sum_wo_overflow(5U, 10U) == 15U);
    REQUIRE(sum_wo_overflow(UL_MAX, 10U) == UL_MAX);
    REQUIRE(sum_wo_overflow((1UL << 63), (1UL << 63)) == UL_MAX);
    REQUIRE(sum_wo_overflow((1UL << 62), (1UL << 62)) == (1UL << 63));
    REQUIRE(sum_wo_overflow((1UL << 63), (1UL << 63), 5U) == UL_MAX);
    REQUIRE(sum_wo_overflow((1UL << 63), 5U, (1UL << 63), 1U) == UL_MAX);
    REQUIRE(sum_wo_overflow(UL_MAX, U_MAX) == UL_MAX);
    REQUIRE(sum_wo_overflow(UL_MAX - 1, 1U) == UL_MAX);
}

TEST_CASE("pattern_to_regex", "[core][util][fn]")
{
    std::string s1 = "abcd";
    std::string s2 = "defg";
    std::string s3 = "\"+";

    auto r1 = std::regex("abcd");
    auto r1_ = pattern_to_regex("abcd");

    REQUIRE(std::regex_match(s1, r1) == std::regex_match(s1, r1_));
    REQUIRE(std::regex_match(s2, r1) == std::regex_match(s2, r1_));

    auto r2 = std::regex("...d");
    auto r2_ = pattern_to_regex("___d");

    REQUIRE(std::regex_match(s1, r2) == std::regex_match(s1, r2_));
    REQUIRE(std::regex_match(s2, r2) == std::regex_match(s2, r2_));

    auto r3 = std::regex("(.*)d(.*)");
    auto r3_ = pattern_to_regex("%d%");

    REQUIRE(std::regex_match(s1, r3) == std::regex_match(s1, r3_));
    REQUIRE(std::regex_match(s2, r3) == std::regex_match(s2, r3_));

    auto r4 = std::regex("\".");
    auto r4_ = pattern_to_regex("\"_");

    REQUIRE(std::regex_match(s2, r4) == std::regex_match(s2, r4_));
    REQUIRE(std::regex_match(s3, r4) == std::regex_match(s3, r4_));
}
