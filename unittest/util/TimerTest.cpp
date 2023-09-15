#include "catch2/catch.hpp"
#include <chrono>
#include <mutable/util/Timer.hpp>

using namespace m;

TEST_CASE("Timer::Measurement", "[core][util]")
{
    Timer::Measurement M("test measurement");
    using duration = Timer::duration;
    using timepoint = Timer::time_point;

    SECTION("unused Measurement")
    {
        REQUIRE(not M.has_started());
        REQUIRE(not M.has_ended());
        REQUIRE(not M.is_active());
        REQUIRE(not M.is_finished());
        REQUIRE(M.is_unused());
    }

    SECTION("used Measurement")
    {
        timepoint start = Timer::clock::now();
        M.start();
        REQUIRE(M.has_started());
        REQUIRE(not M.has_ended());
        REQUIRE(M.is_active());
        REQUIRE(not M.is_finished());

        M.stop();
        timepoint end = Timer::clock::now();
        REQUIRE(M.has_started());
        REQUIRE(M.has_ended());
        REQUIRE(not M.is_active());
        REQUIRE(M.is_finished());

        auto elapsed = std::chrono::duration<double, std::milli>(end - start);
        REQUIRE(M.duration() > duration(0));
        REQUIRE(M.duration() <= elapsed);

        M.clear();
        REQUIRE(not M.has_started());
        REQUIRE(not M.has_ended());
        REQUIRE(not M.is_active());
        REQUIRE(not M.is_finished());
        REQUIRE(M.is_unused());
    }
}

TEST_CASE("Timer", "[core][util][timer]")
{
    Timer T;

    auto tp0 = new Timer::TimingProcess(T.create_timing("m0"));
    auto tp1 = new Timer::TimingProcess(T.create_timing("m1"));
    REQUIRE(T.measurements().size() == 2);

    auto & m0 = T.get("m0");
    auto & m1 = T.get(1);

    REQUIRE(m0.is_active());
    REQUIRE(m1.is_active());

    {
        REQUIRE_THROWS_AS(T.create_timing("m0"), m::invalid_argument);
        REQUIRE_THROWS_AS(T.get(2), m::out_of_range);
        REQUIRE_THROWS_AS(T.get("m2"), m::out_of_range);
    }

    // Timing process destructor will stop its corresponding measurement
    delete tp0;
    delete tp1;

    REQUIRE(m0.has_ended());
    REQUIRE(m1.has_ended());
    REQUIRE(T.total() >= m0.duration() + m1.duration());

    T.clear();
    REQUIRE(T.begin() == T.end());

    {
        REQUIRE_THROWS_AS(T.get(0), m::out_of_range);
        REQUIRE_THROWS_AS(T.get("m0"), m::out_of_range);
    }
}
