#include "catch.hpp"

#include "util/ADT.hpp"

TEST_CASE("SmallBitset", "[unit]")
{
    SmallBitset S;
    REQUIRE(S.empty());
    REQUIRE(S.size() == 0);
    REQUIRE(S.capacity() == 64);

    SECTION("setting and checking bits")
    {
        S.set(0);
        REQUIRE(S == 1);
        REQUIRE(S.size() == 1);
        S.set(2);
        REQUIRE(S == 5);
        S.set(2);
        REQUIRE(S == 5);
        REQUIRE(S.size() == 2);
        REQUIRE(not S.empty());
        REQUIRE(S.contains(0));
        REQUIRE(S(2));
        REQUIRE(not S.contains(1));
    }

    SECTION("bitwise operations")
    {
        SmallBitset S1(14);
        SmallBitset S2(10);

        REQUIRE((S1 | S2) == S1);
        REQUIRE((S1 & S2) == S2);
        REQUIRE((S1 - S2) == 4);
        REQUIRE((S - S2) == S);
    }

    SECTION("is_subset")
    {
        SmallBitset S1(14);
        SmallBitset S2(10);

        REQUIRE(S2.is_subset(S1));
        REQUIRE(not S1.is_subset(S2));
    }
}

TEST_CASE("GospersHack", "[unit]")
{
    uint64_t n = 5;
    uint64_t k = 3;

    SECTION("factory methods")
    {
        GospersHack S1 = GospersHack::enumerate_all(k, n);
        REQUIRE(*S1 == 7);
        REQUIRE(S1);

        GospersHack S2 = GospersHack::enumerate_from(SmallBitset(14), 5);
        REQUIRE(*S2 == 14);
        REQUIRE(S2);
    }

    SECTION("enumerating subsets")
    {
        n = 4;
        k = 3;
        GospersHack S = GospersHack::enumerate_all(k, n);
        REQUIRE(*S == 7);
        REQUIRE(*(++S) == 11);
        REQUIRE(*(++S) == 13);
        REQUIRE(*(++S) == 14);
        REQUIRE(not ++S);
    }

}
