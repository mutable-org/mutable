#include "catch2/catch.hpp"

#include "util/container/RefCountingHashMap.hpp"
#include <set>


using namespace m;


TEST_CASE("RefCountingHashMap", "[core][util][container]")
{
    using map_type = RefCountingHashMap<int32_t, int32_t>;

    SECTION("c'tor")
    {
        SECTION("capacity is zero")
        {
            REQUIRE_THROWS_AS(map_type(0), std::invalid_argument);
        }

        SECTION("capacity is one")
        {
            map_type map(1);
            CHECK(map.capacity() == 1);
            CHECK(map.size() == 0);
        }

        SECTION("capacity is 1024")
        {
            map_type map(1024);
            CHECK(map.capacity() == 1024);
            CHECK(map.size() == 0);
        }

        SECTION("capacity rounded to 16")
        {
            map_type map(9);
            CHECK(map.capacity() == 16);
            CHECK(map.size() == 0);
        }
    }

    SECTION("insert")
    {
        map_type map(16);

        SECTION("with duplicates")
        {
            {
                auto it = map.insert_with_duplicates(0, 0);
                CHECK(it->first == 0);
                CHECK(it->second == 0);
                CHECK(map.size() == 1);
            }

            {
                auto it = map.insert_with_duplicates(0, 1);
                CHECK(it->first == 0);
                CHECK(it->second == 1);
                CHECK(map.size() == 2);
            }

            {
                auto it = map.insert_with_duplicates(0, 2);
                CHECK(it->first == 0);
                CHECK(it->second == 2);
                CHECK(map.size() == 3);
            }

            {
                auto it = map.insert_with_duplicates(1, 0);
                CHECK(it->first == 1);
                CHECK(it->second == 0);
                CHECK(map.size() == 4);
            }

            {
                auto it = map.insert_with_duplicates(3, 0);
                CHECK(it->first == 3);
                CHECK(it->second == 0);
                CHECK(map.size() == 5);
            }

            {
                auto it = map.insert_with_duplicates(0, 3);
                CHECK(it->first == 0);
                CHECK(it->second == 3);
                CHECK(map.size() == 6);
            }

            {
                auto it = map.insert_with_duplicates(1, 1);
                CHECK(it->first == 1);
                CHECK(it->second == 1);
                CHECK(map.size() == 7);
            }

            {
                auto it = map.insert_with_duplicates(1, 2);
                CHECK(it->first == 1);
                CHECK(it->second == 2);
                CHECK(map.size() == 8);
            }
        }

        SECTION("without duplicates")
        {
            {
                auto [it, succ] = map.insert_without_duplicates(0, 0);
                REQUIRE(succ);
                CHECK(it->first == 0);
                CHECK(it->second == 0);
                CHECK(map.size() == 1);
            }

            {
                auto [it, succ] = map.insert_without_duplicates(0, 1);
                REQUIRE_FALSE(succ);
                CHECK(it->first == 0);
                CHECK(it->second == 0);
                CHECK(map.size() == 1);
            }

            {
                auto [it, succ] = map.insert_without_duplicates(0, 2);
                REQUIRE_FALSE(succ);
                CHECK(it->first == 0);
                CHECK(it->second == 0);
                CHECK(map.size() == 1);
            }

            {
                auto [it, succ] = map.insert_without_duplicates(1, 0);
                REQUIRE(succ);
                CHECK(it->first == 1);
                CHECK(it->second == 0);
                CHECK(map.size() == 2);
            }

            {
                auto [it, succ] = map.insert_without_duplicates(3, 0);
                REQUIRE(succ);
                CHECK(it->first == 3);
                CHECK(it->second == 0);
                CHECK(map.size() == 3);
            }

            {
                auto [it, succ] = map.insert_without_duplicates(0, 3);
                REQUIRE_FALSE(succ);
                CHECK(it->first == 0);
                CHECK(it->second == 0);
                CHECK(map.size() == 3);
            }

            {
                auto [it, succ] = map.insert_without_duplicates(1, 1);
                REQUIRE_FALSE(succ);
                CHECK(it->first == 1);
                CHECK(it->second == 0);
                CHECK(map.size() == 3);
            }

            {
                auto [it, succ] = map.insert_without_duplicates(1, 2);
                REQUIRE_FALSE(succ);
                CHECK(it->first == 1);
                CHECK(it->second == 0);
                CHECK(map.size() == 3);
            }
        }
    }

    SECTION("bucket_iterator")
    {
        map_type map(8);

        auto it = map.bucket(1);
        REQUIRE(it.probe_length() == 0);
        REQUIRE(it.probe_distance() == 0);
        REQUIRE(it.current_index() == 1);
        REQUIRE(it.bucket_index() == 1);

        ++it;
        REQUIRE(it.probe_length() == 1);
        REQUIRE(it.probe_distance() == 1);
        REQUIRE(it.current_index() == 2);
        REQUIRE(it.bucket_index() == 1);

        ++it;
        REQUIRE(it.probe_length() == 2);
        REQUIRE(it.probe_distance() == 3);
        REQUIRE(it.current_index() == 4);
        REQUIRE(it.bucket_index() == 1);

        ++it;
        REQUIRE(it.probe_length() == 3);
        REQUIRE(it.probe_distance() == 6);
        REQUIRE(it.current_index() == 7);
        REQUIRE(it.bucket_index() == 1);

        ++it;
        REQUIRE(it.probe_length() == 4);
        REQUIRE(it.probe_distance() == 10);
        REQUIRE(it.current_index() == 3);
        REQUIRE(it.bucket_index() == 1);

        ++it;
        REQUIRE(it.probe_length() == 5);
        REQUIRE(it.probe_distance() == 15);
        REQUIRE(it.current_index() == 0);
        REQUIRE(it.bucket_index() == 1);

        ++it;
        REQUIRE(it.probe_length() == 6);
        REQUIRE(it.probe_distance() == 21);
        REQUIRE(it.current_index() == 6);
        REQUIRE(it.bucket_index() == 1);

        ++it;
        REQUIRE(it.probe_length() == 7);
        REQUIRE(it.probe_distance() == 28);
        REQUIRE(it.current_index() == 5);
        REQUIRE(it.bucket_index() == 1);
    }

    SECTION("find")
    {
        map_type map(16);

        map.insert_without_duplicates(0, 0);
        map.insert_without_duplicates(8, 0);
        map.insert_without_duplicates(16, 0);
        map.insert_without_duplicates(1, 1);
        map.insert_without_duplicates(3, 3);
        map.insert_without_duplicates(24, 0);
        map.insert_without_duplicates(9, 1);
        map.insert_without_duplicates(17, 1);

        SECTION("existent")
        {
            {
                auto it = map.find(0);
                REQUIRE(it != map.end());
                REQUIRE(it->first == 0);
                REQUIRE(it->second == 0);
            }

            {
                auto it = map.find(8);
                REQUIRE(it != map.end());
                REQUIRE(it->first == 8);
                REQUIRE(it->second == 0);
            }

            {
                auto it = map.find(16);
                REQUIRE(it != map.end());
                REQUIRE(it->first == 16);
                REQUIRE(it->second == 0);
            }

            {
                auto it = map.find(1);
                REQUIRE(it != map.end());
                REQUIRE(it->first == 1);
                REQUIRE(it->second == 1);
            }

            {
                auto it = map.find(3);
                REQUIRE(it != map.end());
                REQUIRE(it->first == 3);
                REQUIRE(it->second == 3);
            }

            {
                auto it = map.find(24);
                REQUIRE(it != map.end());
                REQUIRE(it->first == 24);
                REQUIRE(it->second == 0);
            }

            {
                auto it = map.find(9);
                REQUIRE(it != map.end());
                REQUIRE(it->first == 9);
                REQUIRE(it->second == 1);
            }

            {
                auto it = map.find(17);
                REQUIRE(it != map.end());
                REQUIRE(it->first == 17);
                REQUIRE(it->second == 1);
            }
        }

        SECTION("non-existent")
        {
            {
                auto it = map.find(2);
                REQUIRE(it == map.end());
            }

            {
                auto it = map.find(4);
                REQUIRE(it == map.end());
            }

            {
                auto it = map.find(5);
                REQUIRE(it == map.end());
            }

            {
                auto it = map.find(6);
                REQUIRE(it == map.end());
            }

            {
                auto it = map.find(7);
                REQUIRE(it == map.end());
            }
        }
    }

    SECTION("for_all")
    {
        map_type map(8);

        map.insert_with_duplicates(0, 0);
        map.insert_with_duplicates(0, 1);
        map.insert_with_duplicates(0, 2);
        map.insert_with_duplicates(1, 0);
        map.insert_with_duplicates(3, 0);
        map.insert_with_duplicates(0, 3);
        map.insert_with_duplicates(1, 1);
        map.insert_with_duplicates(1, 2);

        {
            std::set<typename map_type::value_type> matches;
            map.for_all(0, [&](map_type::value_type &v) {
                matches.insert(v);
            });
            REQUIRE(matches.size() == 4);
            REQUIRE(matches.count({0, 0}) == 1);
            REQUIRE(matches.count({0, 1}) == 1);
            REQUIRE(matches.count({0, 2}) == 1);
            REQUIRE(matches.count({0, 3}) == 1);
        }

        {
            std::set<typename map_type::value_type> matches;
            map.for_all(1, [&](map_type::value_type &v) {
                matches.insert(v);
            });
            REQUIRE(matches.size() == 3);
            REQUIRE(matches.count({1, 0}) == 1);
            REQUIRE(matches.count({1, 1}) == 1);
            REQUIRE(matches.count({1, 2}) == 1);
        }

        {
            std::set<typename map_type::value_type> matches;
            map.for_all(2, [&](map_type::value_type &v) {
                matches.insert(v);
            });
            REQUIRE(matches.size() == 0);
        }

        {
            std::set<typename map_type::value_type> matches;
            map.for_all(3, [&](map_type::value_type &v) {
                matches.insert(v);
            });
            REQUIRE(matches.size() == 1);
            REQUIRE(matches.count({3, 0}) == 1);
        }
    }

    SECTION("count")
    {
        map_type map(8);

        map.insert_with_duplicates(0, 0);
        map.insert_with_duplicates(0, 1);
        map.insert_with_duplicates(0, 2);
        map.insert_with_duplicates(1, 0);
        map.insert_with_duplicates(3, 0);
        map.insert_with_duplicates(0, 3);
        map.insert_with_duplicates(1, 1);
        map.insert_with_duplicates(1, 2);

        CHECK(map.count(0) == 4);
        CHECK(map.count(1) == 3);
        CHECK(map.count(2) == 0);
        CHECK(map.count(3) == 1);
        CHECK(map.count(4) == 0);
        CHECK(map.count(5) == 0);
        CHECK(map.count(6) == 0);
        CHECK(map.count(7) == 0);
    }

    SECTION("rehash")
    {
        map_type map(8);
        map.max_load_factor(1.f);

        map.insert_with_duplicates(0, 0);
        map.insert_with_duplicates(0, 1);
        map.insert_with_duplicates(0, 2);
        map.insert_with_duplicates(1, 0);
        map.insert_with_duplicates(3, 0);
        map.insert_with_duplicates(1, 1);
        map.insert_with_duplicates(1, 2);
        REQUIRE(map.size() == 7);
        REQUIRE(map.capacity() == 8);

        map.rehash();
        REQUIRE(map.size() == 7);
        REQUIRE(map.capacity() == 8); // rehashing does not change capacity

        CHECK(map.count(0) == 3);
        CHECK(map.count(1) == 3);
        CHECK(map.count(2) == 0);
        CHECK(map.count(3) == 1);
        CHECK(map.count(4) == 0);
        CHECK(map.count(5) == 0);
        CHECK(map.count(6) == 0);
        CHECK(map.count(7) == 0);
    }

    SECTION("resize")
    {
        map_type map(8);
        map.max_load_factor(1.f);

        map.insert_with_duplicates(0, 0);
        map.insert_with_duplicates(0, 1);
        map.insert_with_duplicates(0, 2);
        map.insert_with_duplicates(1, 0);
        map.insert_with_duplicates(3, 0);
        map.insert_with_duplicates(1, 1);
        map.insert_with_duplicates(1, 2);

        map.resize(16);
        REQUIRE(map.size() == 7);
        REQUIRE(map.capacity() == 16);

        CHECK(map.count(0) == 3);
        CHECK(map.count(1) == 3);
        CHECK(map.count(2) == 0);
        CHECK(map.count(3) == 1);
        CHECK(map.count(4) == 0);
        CHECK(map.count(5) == 0);
        CHECK(map.count(6) == 0);
        CHECK(map.count(7) == 0);
    }

    SECTION("insert_with_duplicates with resize")
    {
        map_type map(4);
        map.max_load_factor(.7f);

        map.insert_with_duplicates(0, 0);
        map.insert_with_duplicates(0, 1);
        map.insert_with_duplicates(0, 2);
        map.insert_with_duplicates(1, 0);
        map.insert_with_duplicates(3, 0);
        map.insert_with_duplicates(1, 1);
        map.insert_with_duplicates(1, 2);

        REQUIRE(map.size() == 7);
        REQUIRE(map.capacity() == 16);

        CHECK(map.count(0) == 3);
        CHECK(map.count(1) == 3);
        CHECK(map.count(2) == 0);
        CHECK(map.count(3) == 1);
        CHECK(map.count(4) == 0);
        CHECK(map.count(5) == 0);
        CHECK(map.count(6) == 0);
        CHECK(map.count(7) == 0);
    }

    SECTION("insert_without_duplicates with resize")
    {
        map_type map(4);
        map.max_load_factor(.7f);

        map.insert_without_duplicates(0, 0);
        map.insert_without_duplicates(8, 0);
        map.insert_without_duplicates(16, 0);
        map.insert_without_duplicates(1, 1);
        map.insert_without_duplicates(3, 3);
        map.insert_without_duplicates(24, 0);
        map.insert_without_duplicates(9, 1);
        map.insert_without_duplicates(17, 1);

        REQUIRE(map.size() == 8);
        REQUIRE(map.capacity() == 16);

        CHECK(map.count(0) == 1);
        CHECK(map.count(1) == 1);
        CHECK(map.count(3) == 1);
        CHECK(map.count(8) == 1);
        CHECK(map.count(9) == 1);
        CHECK(map.count(16) == 1);
        CHECK(map.count(17) == 1);
        CHECK(map.count(24) == 1);

        CHECK(map.count(2) == 0);
        CHECK(map.count(4) == 0);
        CHECK(map.count(5) == 0);
        CHECK(map.count(6) == 0);
        CHECK(map.count(7) == 0);
        CHECK(map.count(10) == 0);
        CHECK(map.count(11) == 0);
        CHECK(map.count(12) == 0);
    }

    SECTION("resize")
    {
        map_type map(16);
        map.max_load_factor(.7f);

        map.insert_without_duplicates(8, 0);
        map.insert_without_duplicates(16, 0);
        map.insert_without_duplicates(1, 1);
        map.insert_without_duplicates(3, 3);
        map.insert_without_duplicates(24, 0);

        SECTION("less than size")
        {
            map.resize(4);
            REQUIRE(map.size() == 5);
            REQUIRE(map.capacity() == 8);
            CHECK(map.count(1) == 1);
            CHECK(map.count(3) == 1);
            CHECK(map.count(8) == 1);
            CHECK(map.count(16) == 1);
            CHECK(map.count(24) == 1);
        }

        SECTION("exceeding max load factor")
        {
            map.insert_without_duplicates(17, 0);
            map.resize(8); // desired capacity 8 would not satisfy max load factor
            REQUIRE(map.size() == 6);
            REQUIRE(map.capacity() == 16);
            CHECK(map.count(1) == 1);
            CHECK(map.count(3) == 1);
            CHECK(map.count(8) == 1);
            CHECK(map.count(16) == 1);
            CHECK(map.count(24) == 1);
            CHECK(map.count(17) == 1);
        }

        SECTION("less than capacity")
        {
            map.resize(8);
            REQUIRE(map.size() == 5);
            REQUIRE(map.capacity() == 8);
            CHECK(map.count(1) == 1);
            CHECK(map.count(3) == 1);
            CHECK(map.count(8) == 1);
            CHECK(map.count(16) == 1);
            CHECK(map.count(24) == 1);
        }

        SECTION("equals to capacity")
        {
            map.resize(16);
            REQUIRE(map.size() == 5);
            REQUIRE(map.capacity() == 16);
            CHECK(map.count(1) == 1);
            CHECK(map.count(3) == 1);
            CHECK(map.count(8) == 1);
            CHECK(map.count(16) == 1);
            CHECK(map.count(24) == 1);
        }

        SECTION("equals greater than capacity")
        {
            map.resize(17);
            REQUIRE(map.size() == 5);
            REQUIRE(map.capacity() == 32);
            CHECK(map.count(1) == 1);
            CHECK(map.count(3) == 1);
            CHECK(map.count(8) == 1);
            CHECK(map.count(16) == 1);
            CHECK(map.count(24) == 1);
        }
    }

    SECTION("insert into potential hole after resize")
    {
        map_type map(8);
        map.max_load_factor(1.f);

        map.insert_without_duplicates(0, 0);    // slot 0

        map.insert_without_duplicates(15, 0);   // slot 7
        map.insert_without_duplicates(7, 0);    // slot 2, slot 7 and slot 0 are occupied

        map.resize(16);

        map.insert_without_duplicates(7, 1);    // slot 7
        map.insert_without_duplicates(23, 0);   // slot 10, slot 7 and slot 8 are occupied

        CHECK(map.count(7) == 1);
    }
}
