#include "catch.hpp"

#include "util/ADT.hpp"


using namespace m;


TEST_CASE("SmallBitset", "[unit]")
{
    SmallBitset S;
    REQUIRE(S.empty());
    REQUIRE(S.size() == 0);
    REQUIRE(S.capacity() == 64);

    SECTION("setting and checking bits")
    {
        S(0) = true;
        REQUIRE(S == SmallBitset(1UL));
        REQUIRE(S.size() == 1);
        S[2] = true;
        REQUIRE(S == SmallBitset(5UL));
        S[2] = true;
        REQUIRE(S == SmallBitset(5UL));
        REQUIRE(S.size() == 2);
        REQUIRE(not S.empty());
        REQUIRE(S(0));
        REQUIRE(S(2));
        REQUIRE(not S(1));
    }

    SECTION("bitwise operations")
    {
        SmallBitset S1(14);
        SmallBitset S2(10);

        REQUIRE((S1 | S2) == S1);
        REQUIRE((S1 & S2) == S2);
        REQUIRE((S1 - S2) == SmallBitset(4UL));
        REQUIRE((S - S2) == S);
    }

    SECTION("is_subset")
    {
        SmallBitset S1(14);
        SmallBitset S2(10);

        REQUIRE(S2.is_subset(S1));
        REQUIRE(not S1.is_subset(S2));
    }

    SECTION("out of range")
    {
        SmallBitset S;
        REQUIRE_THROWS_AS(S.at(64), m::out_of_range);
    }
}

TEST_CASE("GospersHack", "[unit]")
{
    SECTION("factory methods")
    {
        GospersHack S1 = GospersHack::enumerate_all(3UL, 5UL); // 3 of 5
        REQUIRE(*S1 == SmallBitset(7UL));
        REQUIRE(S1);

        GospersHack S2 = GospersHack::enumerate_from(SmallBitset(14UL), 5UL); // 14 = 0b01110
        REQUIRE(*S2 == SmallBitset(14UL));
        REQUIRE(S2);
    }

    SECTION("enumerating subsets")
    {
        GospersHack S = GospersHack::enumerate_all(3UL, 4UL); // 3 of 4
        REQUIRE(*S == SmallBitset(7UL));        // 0b0111
        REQUIRE(*(++S) == SmallBitset(11UL));   // 0b1011
        REQUIRE(*(++S) == SmallBitset(13UL));   // 0b1101
        REQUIRE(*(++S) == SmallBitset(14UL));   // 0b1110
        REQUIRE(not ++S);
    }
}

TEST_CASE("SmallBitset/least_subset", "[core][util][fn]")
{
    SmallBitset set(10UL); // 0b1010 <=> { 2, 8 }
    REQUIRE(least_subset(set) == SmallBitset(2UL));
}


TEST_CASE("SmallBitset/next_subset", "[core][util][fn]")
{
    SmallBitset set(10UL); // 0b1010 <=> { 2, 8 }

    REQUIRE(next_subset(SmallBitset(0UL), set) == SmallBitset(2UL));
    REQUIRE(next_subset(SmallBitset(2UL), set) == SmallBitset(8UL));
    REQUIRE(next_subset(SmallBitset(8UL), set) == set);
    REQUIRE(next_subset(SmallBitset(10UL), set) == SmallBitset(0UL));
}

