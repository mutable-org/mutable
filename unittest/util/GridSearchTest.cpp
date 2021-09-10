#include "catch.hpp"

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
}

TEST_CASE("LinearSpace sanity", "[core][util]")
{
    SECTION("int, 1 to 0, 1 step")
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
