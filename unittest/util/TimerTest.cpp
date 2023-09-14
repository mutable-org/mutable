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
