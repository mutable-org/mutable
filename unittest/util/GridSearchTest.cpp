#include "catch2/catch.hpp"

#include "util/GridSearch.hpp"


using namespace m;
using namespace m::gs;


TEST_CASE("LinearSpace", "[core][util]")
{
    SECTION("int, 0 to 1, 1 step")
    {
        LinearSpace<int> S(0, 1, 1);
        CHECK(S.lo() == 0);
        CHECK(S.hi() == 1);
        CHECK(S.num_steps() == 1);
        CHECK(S.step() == Approx(1));
        CHECK(S.ascending());
        CHECK_FALSE(S.descending());

        SECTION("at()/operator()")
        {
            CHECK(S.at(0) == 0);
            CHECK(S.at(1) == 1);
        }

        SECTION("sequence()")
        {
            CHECK(S.sequence() == std::vector<int>{0, 1});
        }
    }

    SECTION("unsigned, 0 to 10, 5 steps")
    {
        LinearSpace<unsigned> S(0, 10, 5);
        CHECK(S.lo() == 0);
        CHECK(S.hi() == 10);
        CHECK(S.num_steps() == 5);
        CHECK(S.step() == Approx(2));
        CHECK(S.ascending());
        CHECK_FALSE(S.descending());

        SECTION("at()/operator()")
        {
            CHECK(S.at(0) == 0);
            CHECK(S.at(1) == 2);
            CHECK(S.at(2) == 4);
            CHECK(S.at(3) == 6);
            CHECK(S.at(4) == 8);
            CHECK(S.at(5) == 10);
        }

        SECTION("sequence()")
        {
            CHECK(S.sequence() == std::vector<unsigned>{0, 2, 4, 6, 8, 10});
        }
    }

    SECTION("unsigned, 0 to 10, 5 steps, ascending (factory method)")
    {
        auto S = LinearSpace<unsigned>::Ascending(0, 10, 5);
        CHECK(S.lo() == 0);
        CHECK(S.hi() == 10);
        CHECK(S.num_steps() == 5);
        CHECK(S.step() == Approx(2));
        CHECK(S.ascending());
        CHECK_FALSE(S.descending());

        SECTION("at()/operator()")
        {
            CHECK(S.at(0) == 0);
            CHECK(S.at(1) == 2);
            CHECK(S.at(2) == 4);
            CHECK(S.at(3) == 6);
            CHECK(S.at(4) == 8);
            CHECK(S.at(5) == 10);
        }

        SECTION("sequence()")
        {
            CHECK(S.sequence() == std::vector<unsigned>{0, 2, 4, 6, 8, 10});
        }
    }

    SECTION("int, -10 to 10, 4 steps")
    {
        LinearSpace<int> S(-10, 10, 4);
        CHECK(S.lo() == -10);
        CHECK(S.hi() == 10);
        CHECK(S.num_steps() == 4);
        CHECK(S.step() == Approx(5));
        CHECK(S.ascending());
        CHECK_FALSE(S.descending());

        SECTION("at()/operator()")
        {
            CHECK(S.at(0) == -10);
            CHECK(S.at(1) == -5);
            CHECK(S.at(2) == 0);
            CHECK(S.at(3) == 5);
            CHECK(S.at(4) == 10);
        }

        SECTION("sequence()")
        {
            CHECK(S.sequence() == std::vector<int>{-10, -5, 0, 5, 10});
        }
    }

    SECTION("int, -5 to 5, 3 steps")
    {
        LinearSpace<int> S(-5, 5, 3);
        CHECK(S.lo() == -5);
        CHECK(S.hi() == 5);
        CHECK(S.num_steps() == 3);
        CHECK(S.step() == Approx(10./3));
        CHECK(S.ascending());
        CHECK_FALSE(S.descending());

        SECTION("at()/operator()")
        {
            CHECK(S.at(0) == -5);
            CHECK(S.at(1) == -2);
            CHECK(S.at(2) == 2);
            CHECK(S.at(3) == 5);
        }

        SECTION("sequence()")
        {
            CHECK(S.sequence() == std::vector<int>{-5, -2, 2, 5});
        }
    }

    SECTION("float, 0 to 1, 10 steps")
    {
        LinearSpace<float> S(0, 1, 10);
        CHECK(S.lo() == 0.f);
        CHECK(S.hi() == 1.f);
        CHECK(S.num_steps() == 10);
        CHECK(S.step() == Approx(.1f));
        CHECK(S.ascending());
        CHECK_FALSE(S.descending());

        SECTION("at()/operator()")
        {
            CHECK(S.at(0) == Approx(.0f));
            CHECK(S.at(1) == Approx(.1f));
            CHECK(S.at(2) == Approx(.2f));
        }

        SECTION("sequence()")
        {
            CHECK(S.sequence() == std::vector<float>{.0f, .1f, .2f, .3f, .4f, .5f, .6f, .7f, .8f, .9f, 1.f});
        }
    }

    SECTION("double, -1 to 1, 4 steps")
    {
        LinearSpace<double> S(-1., 1., 4);
        CHECK(S.lo() == -1.);
        CHECK(S.hi() == 1.);
        CHECK(S.num_steps() == 4);
        CHECK(S.step() == Approx(.5));
        CHECK(S.ascending());
        CHECK_FALSE(S.descending());

        SECTION("at()/operator()")
        {
            CHECK(S.at(0) == Approx(-1.));
            CHECK(S.at(1) == Approx(-.5));
            CHECK(S.at(2) == Approx(0.));
            CHECK(S.at(3) == Approx(.5));
            CHECK(S.at(4) == Approx(1.));
        }

        SECTION("sequence()")
        {
            CHECK(S.sequence() == std::vector<double>{-1., -.5, 0., .5, 1.});
        }
    }

    SECTION("unsigned, 0 to 10, 5 steps, descending")
    {
        LinearSpace<unsigned> S(0, 10, 5, /* ascending= */ false);
        CHECK(S.lo() == 0);
        CHECK(S.hi() == 10);
        CHECK(S.num_steps() == 5);
        CHECK(S.step() == Approx(2));
        CHECK(S.descending());
        CHECK_FALSE(S.ascending());

        SECTION("at()/operator()")
        {
            CHECK(S.at(0) == 10);
            CHECK(S.at(1) == 8);
            CHECK(S.at(2) == 6);
            CHECK(S.at(3) == 4);
            CHECK(S.at(4) == 2);
            CHECK(S.at(5) == 0);
        }

        SECTION("sequence()")
        {
            CHECK(S.sequence() == std::vector<unsigned>{10, 8, 6, 4, 2, 0});
        }
    }

    SECTION("unsigned, 0 to 10, 5 steps, descending (factory method)")
    {
        auto S = LinearSpace<unsigned>::Descending(0, 10, 5);
        CHECK(S.lo() == 0);
        CHECK(S.hi() == 10);
        CHECK(S.num_steps() == 5);
        CHECK(S.step() == Approx(2));
        CHECK(S.descending());
        CHECK_FALSE(S.ascending());

        SECTION("at()/operator()")
        {
            CHECK(S.at(0) == 10);
            CHECK(S.at(1) == 8);
            CHECK(S.at(2) == 6);
            CHECK(S.at(3) == 4);
            CHECK(S.at(4) == 2);
            CHECK(S.at(5) == 0);
        }

        SECTION("sequence()")
        {
            CHECK(S.sequence() == std::vector<unsigned>{10, 8, 6, 4, 2, 0});
        }
    }

    SECTION("int32_t, -2^31 to 2^31 - 1, 4 steps")
    {
        LinearSpace<int32_t> S(std::numeric_limits<int32_t>::lowest(), std::numeric_limits<int32_t>::max(), 4);
        CHECK(S.lo() == std::numeric_limits<int32_t>::lowest());
        CHECK(S.hi() == std::numeric_limits<int32_t>::max());
        CHECK(S.num_steps() == 4);
        CHECK(S.step() == Approx(std::numeric_limits<uint32_t>::max()/4));
        CHECK(S.ascending());
        CHECK_FALSE(S.descending());

        SECTION("at()/operator()")
        {
            CHECK(S.at(0) == std::numeric_limits<int32_t>::lowest());
            CHECK(S.at(1) == -1073741824);
            CHECK(S.at(2) == 0);
            CHECK(S.at(3) == 1073741823);
            CHECK(S.at(4) == std::numeric_limits<int32_t>::max());
        }

        SECTION("sequence()")
        {
            CHECK(S.sequence() == std::vector<int32_t>{
                std::numeric_limits<int32_t>::lowest(),
                -1073741824,
                0,
                1073741823,
                std::numeric_limits<int32_t>::max()
            });
        }
    }

    SECTION("int64_t, -2^63 to 2^63 - 1, 4 steps")
    {
        LinearSpace<int64_t> S(std::numeric_limits<int64_t>::lowest(), std::numeric_limits<int64_t>::max(), 4);
        CHECK(S.lo() == std::numeric_limits<int64_t>::lowest());
        CHECK(S.hi() == std::numeric_limits<int64_t>::max());
        CHECK(S.num_steps() == 4);
        CHECK(S.step() == Approx(std::numeric_limits<uint64_t>::max()/4));
        CHECK(S.ascending());
        CHECK_FALSE(S.descending());

        /* an overflow was caused when casting double(2^64) to uint64_t because of a rounding issue with double.
         * To prevent this and allow for a close to accurate result LinearSpace rounds down the step distance,
         * causing inaccuracies when dealing with a large range of type int64_t. */
        SECTION("at()/operator()")
        {
            CHECK(S.at(0) == std::numeric_limits<int64_t>::lowest());
            CHECK(S.at(1) == -4611686018427388416);
            CHECK(S.at(2) == -1024);
            CHECK(S.at(3) == 4611686018427385856);
            CHECK(S.at(4) == 9223372036854773760);
        }

        SECTION("sequence()")
        {
            CHECK(S.sequence() == std::vector<int64_t>{
                std::numeric_limits<int64_t>::lowest(),
                -4611686018427388416,
                -1024,
                4611686018427385856,
                9223372036854773760});
        }
    }

    SECTION("int32_t, -2^31 to 2^31 - 1, 4 steps, descending")
    {
        LinearSpace<int32_t> S(std::numeric_limits<int32_t>::lowest(), std::numeric_limits<int32_t>::max(), 4, false);
        CHECK(S.lo() == std::numeric_limits<int32_t>::lowest());
        CHECK(S.hi() == std::numeric_limits<int32_t>::max());
        CHECK(S.num_steps() == 4);
        CHECK(S.step() == Approx(std::numeric_limits<uint32_t>::max()/4));
        CHECK_FALSE(S.ascending());
        CHECK(S.descending());

        SECTION("at()/operator()")
        {
            CHECK(S.at(0) == std::numeric_limits<int32_t>::max());
            CHECK(S.at(1) == 1073741823);
            CHECK(S.at(2) == -1);
            CHECK(S.at(3) == -1073741824);
            CHECK(S.at(4) == std::numeric_limits<int32_t>::lowest());
        }

        SECTION("sequence()")
        {
            CHECK(S.sequence() == std::vector<int32_t>{
                std::numeric_limits<int32_t>::max(),
                1073741823,
                -1,
                -1073741824,
                std::numeric_limits<int32_t>::lowest()
            });
        }
    }

    SECTION("int64_t, -2^63 to 2^63 - 1, 4 steps, descending")
    {
        LinearSpace<int64_t> S(std::numeric_limits<int64_t>::lowest(), std::numeric_limits<int64_t>::max(), 4, false);
        CHECK(S.lo() == std::numeric_limits<int64_t>::lowest());
        CHECK(S.hi() == std::numeric_limits<int64_t>::max());
        CHECK(S.num_steps() == 4);
        CHECK(S.step() == Approx(std::numeric_limits<uint64_t>::max()/4));
        CHECK_FALSE(S.ascending());
        CHECK(S.descending());

        /* an overflow was caused when casting double(2^64) to uint64_t because of a rounding issue with double.
         * To prevent this and allow for a close to accurate result LinearSpace rounds down the step distance,
         * causing inaccuracies when dealing with a large range of type int64_t. */
        SECTION("at()/operator()")
        {
            CHECK(S.at(0) == std::numeric_limits<int64_t>::max());
            CHECK(S.at(1) == 4611686018427388415);
            CHECK(S.at(2) == 1023);
            CHECK(S.at(3) == -4611686018427385857);
            CHECK(S.at(4) == -9223372036854773761);
        }

        SECTION("sequence()")
        {
            CHECK(S.sequence() == std::vector<int64_t>{
                std::numeric_limits<int64_t>::max(),
                4611686018427388415,
                1023,
                -4611686018427385857,
                -9223372036854773761});
        }
    }
}

TEST_CASE("LinearSpace sanity", "[core][util]")
{
    SECTION("int, 1 to 0, 1 steps")
    {
        REQUIRE_THROWS_AS(LinearSpace<int>(1, 0, 1), std::invalid_argument);
    }

    SECTION("int, 0 to 1, 0 steps")
    {
        REQUIRE_THROWS_AS(LinearSpace<int>(0, 1, 0), std::invalid_argument);
    }

    SECTION("int, 0 to 1, 1 steps")
    {
        LinearSpace<int> S(0, 1, 1);
        REQUIRE(S(0) == 0);
        REQUIRE(S(1) == 1);
        REQUIRE_THROWS_AS(S(2), std::out_of_range);
    }
}

TEST_CASE("Space", "[core][util]")
{
    LinearSpace<int> LS(0, 10, 2);
    Space<int, LinearSpace> &S = LS;

    CHECK(S.lo() == 0);
    CHECK(S.hi() == 10);
    CHECK(S.num_steps() == 2);
    CHECK(S.step() == Approx(5));
}

TEST_CASE("GridSearch", "[core][util]")
{
    LinearSpace<int> A(0, 1, 1);
    LinearSpace<int> B(-10, 10, 4);

    GridSearch GS(A, B);
    CHECK(GS.num_spaces() == 2);
    CHECK(GS.num_points() == 10);
    std::vector<std::pair<int, int>> points;
    GS.search([&points](int a, int b) { points.emplace_back(a, b); });
    CHECK(points == std::vector<std::pair<int, int>>{
        {0, -10}, {0, -5}, {0, 0}, {0, 5}, {0, 10},
        {1, -10}, {1, -5}, {1, 0}, {1, 5}, {1, 10},
    });
}
