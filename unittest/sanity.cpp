#include <catch2/catch.hpp>

#include <cstdlib>
#include <mutable/util/fn.hpp>


/* These tests are used for sanity testing our unit testing pipeline itself.  Some of them are expected to fail. */
TEST_CASE("success", "[test-sanity]") { REQUIRE(true); }
TEST_CASE("failure", "[test-sanity]") { REQUIRE(false); }
TEST_CASE("abort", "[test-sanity]") { abort(); }
TEST_CASE("segv", "[test-sanity]") { const char *p = nullptr;  REQUIRE(m::streq(p, "segv")); }
