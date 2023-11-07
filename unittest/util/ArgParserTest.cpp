#include "catch2/catch.hpp"
#include <mutable/util/ArgParser.hpp>

using namespace m;

#define ADD(TYPE, SHORT, LONG, DESCR, CALLBACK)         \
    {                                                   \
        AP.add<TYPE>(SHORT, LONG, DESCR, CALLBACK);     \
    }


TEST_CASE("ArgParser", "[core][util][ArgParser]") {
    ArgParser AP;
    bool callback_called = false;

    SECTION("Boolean option with short name only")
    {
        ADD(bool, "-h", nullptr, "help description",
            [&](bool) { callback_called = true; });

        const char *argv[] = { "program", "-h", nullptr };
        AP(1, argv);
    }

    SECTION("Boolean option with long name only")
    {
        ADD(bool, nullptr, "--help", "help description",
            [&](bool) { callback_called = true; });

        const char *argv[] = { "program", "--help", nullptr };
        AP(1, argv);
    }

    SECTION("String option with both short and long name")
    {
        ADD(const char *, "-s", "--string", "string option description",
            [&](const char * str) {
                REQUIRE(streq(str, "char * value"));
                callback_called = true;
            });

        const char *argv[] = { "program", "-s", "char * value", nullptr };
        AP(2, argv);
    }

    SECTION("Positional arguments only")
    {
        const char *argv[] = { "program", "pos_arg1", "--", "pos_arg2", "pos_arg3", nullptr };
        AP(2, argv);

        auto &args = AP.args();

        REQUIRE(args.size() == 3);
        REQUIRE(streq(args[0] ,"pos_arg1"));
        REQUIRE(streq(args[1], "pos_arg2"));
        REQUIRE(streq(args[2], "pos_arg3"));
        callback_called = true; // to silence the test
    }

    SECTION("Combination of Options & Positional arguments")
    {
        ADD(bool, "-h", "--help", "help description",
            [&](bool) { callback_called = true; });

        const char *argv[] = { "program", "-h", "pos_arg1", "pos_arg2", nullptr };
        AP(3, argv);

        auto &args = AP.args();

        REQUIRE(args.size() == 2);
        REQUIRE(streq(args[0] ,"pos_arg1"));
        REQUIRE(streq(args[1], "pos_arg2"));
    }

    REQUIRE(callback_called);
}

TEMPLATE_TEST_CASE("ArgParser with Integral types", "[core][util][ArgParser]",
                    int, long, long long, unsigned, unsigned long, unsigned long long)
{
    ArgParser AP;
    bool callback_called = false;

    ADD(TestType, "-i", "--integral", "integral description",
        [&](TestType integral) {
            REQUIRE(integral == TestType(42));
            callback_called = true;
        });

    const char *argv[] = { "program", "-i", "42", nullptr };
    AP(2, argv);

    REQUIRE(callback_called);
}

TEMPLATE_TEST_CASE("ArgParser with Floating-Point types", "[core][util][ArgParser]",
                    float, double, long double)
{
    ArgParser AP;
    bool callback_called = false;

    ADD(TestType, "-f", "--float", "float description",
        [&](TestType floating_point) {
            REQUIRE(floating_point == TestType(42.0));
            callback_called = true;
        });

    const char *argv[] = { "program", "-f", "42.0", nullptr };
    AP(2, argv);

    REQUIRE(callback_called);
}

TEST_CASE("ArgParser with List-of-Strings", "[core][util][ArgParser]")
{
    ArgParser AP;
    bool callback_called = false;

    SECTION("Empty list")
    {
        ADD(std::vector<std::string_view>, "-l", "--list-of-strings", "list description",
            [&](std::vector<std::string_view> list) {
                REQUIRE(list.empty());
                callback_called = true;
            });

        const char *argv[] = { "program", "-l", "", nullptr };
        AP(2, argv);
    }

    SECTION("Non-Empty list")
    {
        ADD(std::vector<std::string_view>, "-l", "--list-of-strings", "list description",
            [&](std::vector<std::string_view> list) {
                REQUIRE(list.size() == 4);
                REQUIRE(list[0] == "yellow");
                REQUIRE(list[1] == "green");
                REQUIRE(list[2] == "navy blue");
                REQUIRE(list[3] == "pink");

                callback_called = true;
            });

        const char *argv[] = { "program", "-l", "yellow,green,navy blue,pink", nullptr };
        AP(3, argv);
    }

    REQUIRE(callback_called);
}


#undef ADD
