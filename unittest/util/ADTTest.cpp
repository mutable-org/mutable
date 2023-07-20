#include "catch2/catch.hpp"

#include <mutable/util/ADT.hpp>
#include <utility>
#include <vector>

using namespace m;


TEST_CASE("SmallBitset", "[core][util]")
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

        S2 &= S;
        REQUIRE(S2.empty());

        S2 |= S1;
        REQUIRE(S2 == S1);

        S2 -= SmallBitset(4UL);
        REQUIRE(S2 == SmallBitset(10UL));

        S2++; S1--;
        REQUIRE(--S1 == ++S2);
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

        const SmallBitset CS;
        REQUIRE_THROWS_AS(CS.at(64), m::out_of_range);
    }

    SECTION("singleton")
    {
        size_t num_shifts[] = { 0, 1, 62, 63 };
        for (auto ns : num_shifts) {
            SmallBitset SB(1UL << ns);
            REQUIRE(SB.singleton());
            REQUIRE(SB.singleton_to_lo_mask().size() == ns);
        }
    }

    SECTION("Forward Iterator")
    {
        SECTION("Empty Bitset")
        {
            SmallBitset SB;
            CHECK(SB.begin() == SB.end());
        }

        SECTION("Singleton Bitset")
        {
            SmallBitset SB(0b1);
            CHECK(SB.begin() != SB.end());
            CHECK(*(SB.begin()) == *(SB.rbegin()));
        }

        SECTION("Non-Singleton Bitset")
        {
            SmallBitset SB(0b1001011101);
            auto it = SB.begin();
            CHECK(it++ == SB.begin());
            CHECK(it != SB.begin());

            for (auto it = SB.cbegin(); it != SB.cend(); it++) {
                SmallBitset sub_SB = it.as_set();
                CHECK(sub_SB == SmallBitset(1ULL << (*it)));
                CHECK(sub_SB.is_subset(SB));
                CHECK(SB.at(*it));
            }
        }
    }

    SECTION("reverse_iterator")
    {
        SmallBitset S;
        S[0] = S[4] = S[63] = true;
        auto it = S.rbegin();
        CHECK(*it == 63);
        ++it;
        CHECK(*it == 4);
        ++it;
        CHECK(*it == 0);
        ++it;
        CHECK(it == S.rend());

        const SmallBitset SB(0b1001011101);
        auto rit = SB.rbegin();
        CHECK(rit++ == SB.rbegin());
        CHECK(rit != SB.rbegin());

        for (auto rit = SB.crbegin(); rit != SB.crend(); rit++) {
            SmallBitset sub_SB = rit.as_set();
            CHECK(sub_SB == SmallBitset(1ULL << (*rit)));
            CHECK(sub_SB.is_subset(SB));
            CHECK(SB.at(*rit));
        }
    }
}

TEST_CASE("GospersHack", "[core][util]")
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

TEST_CASE("SubsetEnumerator", "[core][util]")
{
    SmallBitset S(0b1101UL);

    SECTION("single")
    {
        SubsetEnumerator SE(S, 1);
        CHECK(bool(SE));
        CHECK(*SE == SmallBitset(0b0001UL));
        ++SE;
        CHECK(bool(SE));
        CHECK(*SE == SmallBitset(0b0100UL));
        ++SE;
        CHECK(bool(SE));
        CHECK(*SE == SmallBitset(0b1000UL));
        ++SE;
        CHECK_FALSE(bool(SE));
    }

    SECTION("two")
    {
        SubsetEnumerator SE(S, 2);
        CHECK(bool(SE));
        CHECK(*SE == SmallBitset(0b0101UL));
        ++SE;
        CHECK(bool(SE));
        CHECK(*SE == SmallBitset(0b1001UL));
        ++SE;
        CHECK(bool(SE));
        CHECK(*SE == SmallBitset(0b1100UL));
        ++SE;
        CHECK_FALSE(bool(SE));
    }

    SECTION("three")
    {
        SubsetEnumerator SE(S, 3);
        CHECK(bool(SE));
        CHECK(*SE == SmallBitset(0b1101UL));
        ++SE;
        CHECK_FALSE(bool(SE));
    }
}

TEST_CASE("doubly_linked_list", "[core][util]")
{
    auto CHECK_LIST = []<typename T>(const doubly_linked_list<T> &L, std::initializer_list<T> values) -> void {
        using std::begin, std::end, std::rbegin, std::rend;
        REQUIRE(L.size() == values.size());

        /* Forward iteration. */
        {
            auto list_it = L.cbegin();
            for (auto value_it = begin(values); value_it != end(values); ++value_it, list_it++) {
                REQUIRE(list_it != L.cend());
                CAPTURE(*list_it);
                CHECK(*list_it == *value_it);
            }
            CHECK(list_it == L.cend());
        }

        /* Reverse iteration. */
        {
            auto list_rit = L.crbegin();
            for (auto value_it = rbegin(values); value_it != rend(values); ++value_it, ++list_rit) {
                REQUIRE(list_rit != L.crend());
                CAPTURE(*list_rit);
                CHECK(*list_rit == *value_it);
            }
            CHECK(list_rit == L.crend());
        }
    };

    doubly_linked_list<int> L;

    REQUIRE(L.size() == 0);
    REQUIRE(L.begin() == L.end());
    REQUIRE(L.empty());
    CHECK_LIST(L, { });

    SECTION("emplace")
    {
        SECTION("empty")
        {
            REQUIRE(L.begin() == L.end());
            auto pos = L.emplace(L.begin(), 42);
            REQUIRE(L.size() == 1);
            CHECK(*pos == 42);
            CHECK(pos == L.begin());
            CHECK_LIST(L, { 42 });
        }

        L.push_back(42);
        L.push_back(13);

        SECTION("front")
        {
            auto pos = L.emplace(L.begin(), 73);
            REQUIRE(L.size() == 3);
            CHECK(*pos == 73);
            CHECK(pos == L.begin());
            CHECK_LIST(L, { 73, 42, 13 });
            ++pos;
            REQUIRE(pos != L.end());
            CHECK(*pos == 42);
            ++pos;
            REQUIRE(pos != L.end());
            CHECK(*pos == 13);
            ++pos;
            CHECK(pos == L.end());
        }

        SECTION("mid")
        {
            auto pos = L.emplace(std::next(L.begin()), 73);
            REQUIRE(L.size() == 3);
            CHECK(*pos == 73);
            CHECK(pos == std::next(L.begin()));
            CHECK_LIST(L, { 42, 73, 13 });
            ++pos;
            REQUIRE(pos != L.end());
            CHECK(*pos == 13);
            ++pos;
            CHECK(pos == L.end());
        }

        SECTION("back")
        {
            auto pos = L.emplace(L.end(), 73);
            REQUIRE(L.size() == 3);
            CHECK(*pos == 73);
            CHECK(pos == std::next(std::next(L.begin())));
            CHECK_LIST(L, { 42, 13, 73 });
            ++pos;
            CHECK(pos == L.end());
        }
    }

    SECTION("push_front/emplace_front")
    {
        {
            auto &ref = L.emplace_front(42);
            REQUIRE_FALSE(L.empty());
            REQUIRE(L.size() == 1);
            CHECK(ref == 42);
            CHECK(L.front() == 42);
            CHECK(L.back() == 42);
            CHECK_LIST(L, { 42 });
        }

        L.clear();
        L.push_front(42);

        {
            auto &ref = L.emplace_front(13);
            REQUIRE_FALSE(L.empty());
            REQUIRE(L.size() == 2);
            CHECK(ref == 13);
            CHECK(L.front() == 13);
            CHECK(L.back() == 42);
            CHECK_LIST(L, { 13, 42 });
        }

        L.clear();
        L.push_front(42);
        L.push_front(13);
        const auto &L_ref = L;

        {
            auto &ref = L.emplace_front(73);
            REQUIRE_FALSE(L.empty());
            REQUIRE(L.size() == 3);
            CHECK(ref == 73);
            CHECK(L_ref.front() == 73);
            CHECK(L_ref.back() == 42);
            CHECK_LIST(L, { 73, 13, 42 });
        }
    }

    SECTION("emplace_back")
    {
        {
            auto &ref = L.emplace_back(42);
            REQUIRE_FALSE(L.empty());
            REQUIRE(L.size() == 1);
            CHECK(ref == 42);
            CHECK(L.front() == 42);
            CHECK(L.back() == 42);
            CHECK_LIST(L, { 42 });
        }

        {
            auto &ref = L.emplace_back(13);
            REQUIRE_FALSE(L.empty());
            REQUIRE(L.size() == 2);
            CHECK(ref == 13);
            CHECK(L.front() == 42);
            CHECK(L.back() == 13);
            CHECK_LIST(L, { 42, 13 });
        }

        {
            auto &ref = L.emplace_back(73);
            REQUIRE_FALSE(L.empty());
            REQUIRE(L.size() == 3);
            CHECK(ref == 73);
            CHECK(L.front() == 42);
            CHECK(L.back() == 73);
            CHECK_LIST(L, { 42, 13, 73 });
        }
    }

    SECTION("insert")
    {
        SECTION("multiple")
        {
            auto it = L.insert(L.begin(), 3, 42);
            CHECK_LIST(L, { 42, 42, 42 });
            REQUIRE(it != L.end());
            CHECK(*it == 42);
            ++it;
            REQUIRE(it != L.end());
            CHECK(*it == 42);
            ++it;
            REQUIRE(it != L.end());
            CHECK(*it == 42);
            ++it;
            CHECK(it == L.end());
        }

        SECTION("range")
        {
            std::vector<int> vec{{ 42, 13, 73 }};
            auto it = L.insert(L.begin(), vec.cbegin(), vec.cend());
            CHECK_LIST(L, { 42, 13, 73 });
            REQUIRE(it != L.end());
            CHECK(*it == 42);
            ++it;
            REQUIRE(it != L.end());
            CHECK(*it == 13);
            ++it;
            REQUIRE(it != L.end());
            CHECK(*it == 73);
            ++it;
            CHECK(it == L.end());
        }

        SECTION("initializer list")
        {
            auto it = L.insert(L.begin(), { 42, 13, 73 });
            CHECK_LIST(L, { 42, 13, 73 });
            REQUIRE(it != L.end());
            CHECK(*it == 42);
            ++it;
            REQUIRE(it != L.end());
            CHECK(*it == 13);
            ++it;
            REQUIRE(it != L.end());
            CHECK(*it == 73);
            ++it;
            CHECK(it == L.end());
        }
    }

    SECTION("iterators")
    {
        L.push_back(42);
        L.push_back(13);
        L.push_back(73);
        CHECK_LIST(L, { 42, 13, 73 });

        decltype(L)::iterator it = L.begin();
        decltype(L)::const_iterator cit = it; // implicit conversion
        (void) cit;
        // decltype(L)::iterator ncit = cit; // illegal conversion
    }

    SECTION("clear")
    {
        L.push_back(42);
        L.push_back(13);
        L.push_back(73);
        REQUIRE(L.size() == 3);

        L.clear();
        CHECK(L.size() == 0);
        CHECK(L.begin() == L.end());
        CHECK(L.rbegin() == L.rend());
    }

    SECTION("erase")
    {
        L.push_back(42);

        SECTION("last")
        {
            auto to_erase = L.begin();
            auto ref = L.erase(to_erase);
            CHECK(L.size() == 0);
            REQUIRE(ref == L.end());
            CHECK_LIST(L, { });
       }

        L.push_back(13);
        L.push_back(73);

        SECTION("front")
        {
            auto to_erase = L.begin();
            auto ref = L.erase(to_erase); // erase 42
            CHECK(L.size() == 2);
            REQUIRE(ref != L.end());
            CHECK(*ref == 13);
            ++ref;
            CHECK(*ref == 73);
            CHECK_LIST(L, { 13, 73 });
        }

        SECTION("mid")
        {
            auto to_erase = L.begin();
            ++to_erase;
            auto ref = L.erase(to_erase); // erase 13
            CHECK(L.size() == 2);
            REQUIRE(ref != L.end());
            CHECK(*ref == 73);
            ++ref;
            CHECK(ref == L.end());
            CHECK_LIST(L, { 42, 73 });
        }

        SECTION("back")
        {
            auto to_erase = L.begin();
            ++to_erase;
            ++to_erase;
            auto ref = L.erase(to_erase); // erase 73
            CHECK(L.size() == 2);
            REQUIRE(ref == L.end());
            --ref;
            CHECK(*ref == 13);
            CHECK_LIST(L, { 42, 13 });
            CHECK(*ref-- == 13);
            CHECK(*ref.operator->() == 42);
        }
    }

    SECTION("pop_front")
    {
        L.push_back(42);

        SECTION("last")
        {
            auto val = L.pop_front();
            CHECK(val == 42);
            CHECK_LIST(L, { });
        }

        L.push_back(13);
        L.push_back(73);

        SECTION("multiple")
        {
            auto val = L.pop_front();
            CHECK(val == 42);
            CHECK_LIST(L, { 13, 73 });
        }
    }

    SECTION("pop_back")
    {
        L.push_back(42);

        SECTION("last")
        {
            auto val = L.pop_back();
            CHECK(val == 42);
            CHECK_LIST(L, { });
        }

        L.push_back(13);
        L.push_back(73);

        SECTION("multiple")
        {
            auto val = L.pop_back();
            CHECK(val == 73);
            CHECK_LIST(L, { 42, 13 });
        }
    }

    SECTION("reverse")
    {
        L.push_back(42);
        L.push_back(13);
        L.push_back(73);
        L.reverse();
        CHECK_LIST(L, { 73, 13, 42 });
    }

    SECTION("Constructor / Assignemnt")
    {
        using dlint = doubly_linked_list<int>;
        const std::vector<int> vec { { 42, 13, 73 } };

        SECTION("range c'tor")
        {
            dlint L(vec.begin(), vec.end());
            CHECK_LIST(L, { 42, 13, 73 });
        }

        SECTION("copy c'tor")
        {
            dlint L1(vec.begin(), vec.end());
            dlint& L1_ref = L1;

            dlint L2(L1);
            dlint L3 = dlint(L1_ref);

            CHECK_LIST(L1, { 42, 13, 73 });
            CHECK_LIST(L2, { 42, 13, 73 });
            CHECK_LIST(L3, { 42, 13, 73 });
        }

        SECTION("move c'tor")
        {
            dlint L1(vec.begin(), vec.end());

            dlint L2(std::move(L1));
            CHECK_LIST(L2, { 42, 13, 73 });
            CHECK(L1.empty());

            dlint L3 = std::move(L2);
            CHECK_LIST(L3, { 42, 13, 73 });
            CHECK(L2.empty());
        }

        SECTION("= operator")
        {
            dlint L1(vec.begin(), vec.end());
            dlint L2;

            L2 = dlint(L1);
            CHECK_LIST(L1, { 42, 13, 73 });
            CHECK_LIST(L2, { 42, 13, 73 });
        }

        SECTION("move assginment")
        {
            dlint L1(vec.begin(), vec.end());
            dlint L2;

            L2 = std::move(L1);
            CHECK_LIST(L2, { 42, 13, 73 });
            CHECK(L1.empty());
        }
    }
}
