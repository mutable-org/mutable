#include "catch2/catch.hpp"

#include "util/algorithms.hpp"
#include <algorithm>
#include <array>
#include <random>


TEST_CASE("verify_partition", "[core][util][algorithms]")
{
    using std::prev, std::next;

    SECTION("empty")
    {
        std::array<int, 0> data;
        REQUIRE(m::verify_partition(data.begin(), data.begin(), data.end()));
    }

    SECTION("single element")
    {
        std::array<int, 1> data = { {42} };
        REQUIRE(m::verify_partition(data.begin(), data.begin(), data.end()));
        REQUIRE(m::verify_partition(data.begin(), next(data.begin()), data.end()));
    }

    SECTION("sorted")
    {
        std::array<int, 2> data = { {13, 42} };
        REQUIRE(m::verify_partition(data.begin(), data.begin(), data.end()));
        REQUIRE(m::verify_partition(data.begin(), next(data.begin()), data.end()));
        REQUIRE(m::verify_partition(data.begin(), next(data.begin(), 2), data.end()));
    }

    SECTION("unsorted")
    {
        std::array<int, 2> data = { {42, 13} };
        REQUIRE(m::verify_partition(data.begin(), data.begin(), data.end()));
        REQUIRE_FALSE(m::verify_partition(data.begin(), next(data.begin()), data.end()));
        REQUIRE(m::verify_partition(data.begin(), next(data.begin(), 2), data.end()));
    }
}

TEST_CASE("partition_predicated_naive", "[core][util][algorithms]")
{
    using std::distance;

    m::partition_predicated_naive p;

    SECTION("empty")
    {
        std::array<int, 0> data;
        auto part = p(42, data.begin(), data.end());
        REQUIRE(part == data.end());
    }

    SECTION("one element")
    {
        std::array<int, 1> data = { {42} };

        SECTION("less than pivot")
        {
            auto part = p(43, data.begin(), data.end());
            REQUIRE(distance(part, data.end()) == 0);
            REQUIRE(m::verify_partition(data.begin(), part, data.end()));
        }

        SECTION("equals to pivot")
        {
            auto part = p(42, data.begin(), data.end());
            REQUIRE(m::verify_partition(data.begin(), part, data.end()));
        }

        SECTION("greater than pivot")
        {
            auto part = p(41, data.begin(), data.end());
            REQUIRE(distance(data.begin(), part) == 0);
            REQUIRE(m::verify_partition(data.begin(), part, data.end()));
        }
    }

    SECTION("two elements/sorted")
    {
        std::array<int, 2> data = { {13, 42} };

        SECTION("all less than pivot")
        {
            auto part = p(43, data.begin(), data.end());
            REQUIRE(distance(data.begin(), part) == 2);
            REQUIRE(m::verify_partition(data.begin(), part, data.end()));
        }

        SECTION("one less than pivot")
        {
            auto part = p(41, data.begin(), data.end());
            REQUIRE(distance(data.begin(), part) == 1);
            REQUIRE(m::verify_partition(data.begin(), part, data.end()));
            REQUIRE(data[0] == 13);
            REQUIRE(data[1] == 42);
        }

        SECTION("none less than pivot")
        {
            auto part = p(12, data.begin(), data.end());
            REQUIRE(distance(data.begin(), part) == 0);
            REQUIRE(m::verify_partition(data.begin(), part, data.end()));
        }
    }

    SECTION("two elements/unsorted")
    {
        std::array<int, 2> data = { {42, 13} };

        SECTION("all less than pivot")
        {
            auto part = p(43, data.begin(), data.end());
            REQUIRE(distance(data.begin(), part) == 2);
            REQUIRE(m::verify_partition(data.begin(), part, data.end()));
        }

        SECTION("one less than pivot")
        {
            auto part = p(41, data.begin(), data.end());
            REQUIRE(distance(data.begin(), part) == 1);
            REQUIRE(m::verify_partition(data.begin(), part, data.end()));
            REQUIRE(data[0] == 13);
            REQUIRE(data[1] == 42);
        }

        SECTION("none less than pivot")
        {
            auto part = p(12, data.begin(), data.end());
            REQUIRE(distance(data.begin(), part) == 0);
            REQUIRE(m::verify_partition(data.begin(), part, data.end()));
        }
    }

    SECTION("three elements/sorted")
    {
        std::array<int, 3> data = { {13, 42, 73} };

        SECTION("all less than pivot")
        {
            auto part = p(74, data.begin(), data.end());
            REQUIRE(distance(data.begin(), part) == 3);
            REQUIRE(m::verify_partition(data.begin(), part, data.end()));
        }

        SECTION("two less than pivot")
        {
            auto part = p(72, data.begin(), data.end());
            REQUIRE(distance(data.begin(), part) == 2);
            REQUIRE(m::verify_partition(data.begin(), part, data.end()));
            REQUIRE(data[2] == 73);
        }

        SECTION("one less than pivot")
        {
            auto part = p(41, data.begin(), data.end());
            REQUIRE(distance(data.begin(), part) == 1);
            REQUIRE(m::verify_partition(data.begin(), part, data.end()));
            REQUIRE(data[0] == 13);
        }

        SECTION("none less than pivot")
        {
            auto part = p(12, data.begin(), data.end());
            REQUIRE(distance(data.begin(), part) == 0);
            REQUIRE(m::verify_partition(data.begin(), part, data.end()));
        }
    }

    SECTION("three elements/unsorted")
    {
        std::array<int, 3> data = { {42, 73, 13} };

        SECTION("all less than pivot")
        {
            auto part = p(74, data.begin(), data.end());
            REQUIRE(distance(data.begin(), part) == 3);
            REQUIRE(m::verify_partition(data.begin(), part, data.end()));
        }

        SECTION("two less than pivot")
        {
            auto part = p(72, data.begin(), data.end());
            REQUIRE(distance(data.begin(), part) == 2);
            REQUIRE(m::verify_partition(data.begin(), part, data.end()));
            REQUIRE(data[2] == 73);
        }

        SECTION("one less than pivot")
        {
            auto part = p(41, data.begin(), data.end());
            REQUIRE(distance(data.begin(), part) == 1);
            REQUIRE(m::verify_partition(data.begin(), part, data.end()));
            REQUIRE(data[0] == 13);
        }

        SECTION("none less than pivot")
        {
            auto part = p(12, data.begin(), data.end());
            REQUIRE(distance(data.begin(), part) == 0);
            REQUIRE(m::verify_partition(data.begin(), part, data.end()));
        }
    }
}

TEST_CASE("qsort/partition_predicated_naive", "[core][util][algorithms]")
{
    m::partition_predicated_naive p;

    SECTION("empty")
    {
        std::array<int, 0> data;
        qsort(data.begin(), data.end(), p);
        REQUIRE(std::is_sorted(data.begin(), data.end()));
    }

    SECTION("one element")
    {
        std::array<int, 1> data = { {0} };
        qsort(data.begin(), data.end(), p);
        REQUIRE(std::is_sorted(data.begin(), data.end()));
    }

    SECTION("two elements/sorted")
    {
        std::array<int, 2> data = { {0, 1} };
        qsort(data.begin(), data.end(), p);
        REQUIRE(std::is_sorted(data.begin(), data.end()));
    }

    SECTION("two elements/sorted descending")
    {
        std::array<int, 2> data = { {1, 0} };
        qsort(data.begin(), data.end(), p);
        REQUIRE(std::is_sorted(data.begin(), data.end()));
    }

    SECTION("three elements/sorted")
    {
        std::array<int, 3> data = { {0, 1, 2} };
        qsort(data.begin(), data.end(), p);
        REQUIRE(std::is_sorted(data.begin(), data.end()));
    }

    SECTION("three elements/sorted descending")
    {
        std::array<int, 3> data = { {0, 1, 2} };
        qsort(data.begin(), data.end(), p);
        REQUIRE(std::is_sorted(data.begin(), data.end()));
    }

    SECTION("three elements/unsorted")
    {
        std::array<int, 3> data = { {1, 2, 0} };
        qsort(data.begin(), data.end(), p);
        REQUIRE(std::is_sorted(data.begin(), data.end()));
    }

    SECTION("four elements/sorted")
    {
        std::array<int, 4> data = { {0, 1, 2, 3} };
        qsort(data.begin(), data.end(), p);
        REQUIRE(std::is_sorted(data.begin(), data.end()));
    }

    SECTION("four elements/sorted descending")
    {
        std::array<int, 4> data = { {3, 2, 1, 0} };
        qsort(data.begin(), data.end(), p);
        REQUIRE(std::is_sorted(data.begin(), data.end()));
    }

    SECTION("four elements/unsorted")
    {
        std::array<int, 4> data = { {2, 1, 3, 0} };
        qsort(data.begin(), data.end(), p);
        REQUIRE(std::is_sorted(data.begin(), data.end()));
    }

    SECTION("two elements/equal")
    {
        std::array<int, 2> data = { {42, 42} };
        qsort(data.begin(), data.end(), p);
        REQUIRE(std::is_sorted(data.begin(), data.end()));
    }

    SECTION("three elements/equal")
    {
        std::array<int, 3> data = { {42, 42, 42} };
        qsort(data.begin(), data.end(), p);
        REQUIRE(std::is_sorted(data.begin(), data.end()));
    }

    SECTION("four elements/equal")
    {
        std::array<int, 4> data = { {42, 42, 42, 42} };
        qsort(data.begin(), data.end(), p);
        REQUIRE(std::is_sorted(data.begin(), data.end()));
    }

    SECTION("128 elements/sorted")
    {
        std::array<int, 128> data;
        for (int i = 0; i != 128; ++i)
            data[i] = i;
        qsort(data.begin(), data.end(), p);
        REQUIRE(std::is_sorted(data.begin(), data.end()));
    }

    SECTION("128 elements/sorted descending")
    {
        std::array<int, 128> data;
        for (int i = 0; i != 128; ++i)
            data[i] = 128 - i;
        qsort(data.begin(), data.end(), p);
        REQUIRE(std::is_sorted(data.begin(), data.end()));
    }

    SECTION("128 elements/unsorted")
    {
        std::array<int, 128> data;
        for (int i = 0; i != 128; ++i)
            data[i] = i;

        std::mt19937_64 rng(42L);
        std::shuffle(data.begin(), data.end(), rng);

        qsort(data.begin(), data.end(), p);
        REQUIRE(std::is_sorted(data.begin(), data.end()));
    }

    SECTION("10,000 elements/unsorted")
    {
        constexpr std::size_t N = 1e4;
        std::array<int, N> data;
        for (int i = 0; i != N; ++i)
            data[i] = i;

        std::mt19937_64 rng(42L);
        std::shuffle(data.begin(), data.end(), rng);

        qsort(data.begin(), data.end(), p);
        REQUIRE(std::is_sorted(data.begin(), data.end()));
    }
}
