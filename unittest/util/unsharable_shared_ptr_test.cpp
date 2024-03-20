#include "catch2/catch.hpp"

#include <mutable/util/fn.hpp>
#include <mutable/util/unsharable_shared_ptr.hpp>
#include <string>


using namespace m;


TEST_CASE("unsharable_shared_ptr", "[core][util]")
{
    unsharable_shared_ptr<int> sp1(new int(42));
    CHECK(sp1.use_count() == 1);

    SECTION("unsharable_shared_ptr<int> to unsharable_shared_ptr<int>")
    {
        unsharable_shared_ptr<int> sp2 = sp1;  // copy
        CHECK(sp1.use_count() == 2);
        CHECK(sp2.use_count() == 2);

        CHECK_THROWS_AS(sp1.exclusive_shared_to_unique(), m::invalid_state);
        CHECK_THROWS_AS(sp2.exclusive_shared_to_unique(), m::invalid_state);
        CHECK(*sp1 == 42);
        CHECK(*sp2 == 42);
        // sp2 goes out of scope
    }

    SECTION("unsharable_shared_ptr<int> to unsharable_shared_ptr<const int>")
    {
        unsharable_shared_ptr<const int> sp2 = sp1;  // copy and add constness
        CHECK(sp1.use_count() == 2);
        CHECK(sp2.use_count() == 2);

        CHECK_THROWS_AS(sp1.exclusive_shared_to_unique(), m::invalid_state);
        CHECK_THROWS_AS(sp2.exclusive_shared_to_unique(), m::invalid_state);
        CHECK(*sp1 == 42);
        CHECK(*sp2 == 42);
        // sp2 goes out of scope
    }

    SECTION("unsharable_shared_ptr<int> to std::shared_ptr<int>")
    {
        std::shared_ptr<int> sp2 = sp1;  // copy to `std::shared_ptr`
        CHECK(sp1.use_count() == 2);
        CHECK(sp2.use_count() == 2);

        CHECK_THROWS_AS(sp1.exclusive_shared_to_unique(), m::invalid_state);
        CHECK(*sp1 == 42);
        CHECK(*sp2 == 42);
        // sp2 goes out of scope
    }

    SECTION("unsharable_shared_ptr<int> to std::shared_ptr<const int>")
    {
        std::shared_ptr<const int> sp2 = sp1;  // copy to `std::shared_ptr` and add constness
        CHECK(sp1.use_count() == 2);
        CHECK(sp2.use_count() == 2);

        CHECK_THROWS_AS(sp1.exclusive_shared_to_unique(), m::invalid_state);
        CHECK(*sp1 == 42);
        CHECK(*sp2 == 42);
        // sp2 goes out of scope
    }

    CHECK(sp1.use_count() == 1);
    auto uptr = sp1.exclusive_shared_to_unique();
    CHECK(*uptr == 42);
    CHECK(sp1.use_count() == 0);
    CHECK(sp1.get() == nullptr);
}

TEST_CASE("unsharable_shared_ptr/nullptr", "[core][util]")
{
    unsharable_shared_ptr<int> Null(nullptr);
    CHECK(Null.use_count() == 0);
    auto null_uptr = Null.exclusive_shared_to_unique();
    CHECK(null_uptr.get() == nullptr);
}

TEST_CASE("unsharable_shared_ptr/assignment operator", "[core][util]")
{
    SECTION("unsharable_shared_ptr<int> to unsharable_shared_ptr<int>")
    {
        unsharable_shared_ptr<int> sp1;
        CHECK(sp1.use_count() == 0);
        sp1 = unsharable_shared_ptr<int>(new int(42)); // assign
        CHECK(sp1.use_count() == 1);
        auto uptr = sp1.exclusive_shared_to_unique();
        CHECK(*uptr == 42);
        CHECK(sp1.use_count() == 0);
        CHECK(sp1.get() == nullptr);
    }

    SECTION("unsharable_shared_ptr<int> to unsharable_shared_ptr<const int>")
    {
        unsharable_shared_ptr<const int> sp1;
        CHECK(sp1.use_count() == 0);
        sp1 = unsharable_shared_ptr<int>(new int(42)); // assign and add constness
        CHECK(sp1.use_count() == 1);
        auto uptr = sp1.exclusive_shared_to_unique();
        CHECK(*uptr == 42);
        CHECK(sp1.use_count() == 0);
        CHECK(sp1.get() == nullptr);
    }

    SECTION("unsharable_shared_ptr<int> to std::shared_ptr<int>")
    {
        std::shared_ptr<int> sp1;
        CHECK(sp1.use_count() == 0);
        sp1 = unsharable_shared_ptr<int>(new int(42)); // assign to `std::shared_ptr`
        CHECK(sp1.use_count() == 1);
        CHECK(*sp1 == 42);
    }

    SECTION("unsharable_shared_ptr<int> to std::shared_ptr<const int>")
    {
        std::shared_ptr<const int> sp1;
        CHECK(sp1.use_count() == 0);
        sp1 = unsharable_shared_ptr<int>(new int(42)); // assign to `std::shared_ptr` and add constness
        CHECK(sp1.use_count() == 1);
        CHECK(*sp1 == 42);
    }
}

TEST_CASE("make_unsharable_shared", "[core][util]")
{
    SECTION("int")
    {
        auto sp1 = make_unsharable_shared<int>(42);
        CHECK(sp1.use_count() == 1);
        CHECK(*sp1 == 42);

        auto uptr = sp1.exclusive_shared_to_unique();
        CHECK(*uptr == 42);
        CHECK(sp1.use_count() == 0);
        CHECK(sp1.get() == nullptr);
    }

    SECTION("POD")
    {
        using namespace std::string_literals;
        struct POD { int i; float f; const char *str; };

        auto sp1 = make_unsharable_shared<POD>(42, 3.14f, "Hello, World!");
        CHECK(sp1.use_count() == 1);
        CHECK(sp1->i == 42);
        CHECK(sp1->f == 3.14f);
        CHECK(sp1->str == "Hello, World!"s);

        auto uptr = sp1.exclusive_shared_to_unique();
        CHECK(uptr->i == 42);
        CHECK(uptr->f == 3.14f);
        CHECK(uptr->str == "Hello, World!"s);
        CHECK(sp1.use_count() == 0);
        CHECK(sp1.get() == nullptr);
    }
}

TEST_CASE("unsharable_shared_ptr/cast", "[core][util]")
{
    struct Foo
    {
        virtual ~Foo() { }
    };
    struct Bar : Foo { };

    SECTION("is<Bar>(unsharable_shared_ptr<Foo>)")
    {
        unsharable_shared_ptr<Foo> sp1 = make_unsharable_shared<Bar>();
        CHECK(sp1.use_count() == 1);
        CHECK(is<Bar>(sp1));
    }

    SECTION("cast<Bar>(unsharable_shared_ptr<Foo>)")
    {
        unsharable_shared_ptr<Foo> sp1 = make_unsharable_shared<Bar>();
        CHECK(sp1.use_count() == 1);
        auto sp2 = cast<Bar>(sp1);
        CHECK(bool(sp2));
        CHECK(sp1.use_count() == 2);
        CHECK(sp2.use_count() == 2);
    }

    SECTION("as<Bar>(unsharable_shared_ptr<Foo>)")
    {
        unsharable_shared_ptr<Foo> sp1 = make_unsharable_shared<Bar>();
        CHECK(sp1.use_count() == 1);
        auto sp2 = as<Bar>(sp1);
        CHECK(bool(sp2));
        CHECK(sp1.use_count() == 2);
        CHECK(sp2.use_count() == 2);
    }

    SECTION("as<Bar>(unsharable_shared_ptr<Foo>) and convert to unique pointer")
    {
        unsharable_shared_ptr<Foo> sp1 = make_unsharable_shared<Bar>();
        CHECK(sp1.use_count() == 1);
        auto sp2 = as<Bar>(std::move(sp1));
        CHECK(bool(sp2));
        CHECK(sp2.use_count() == 1);

        auto uptr = sp2.exclusive_shared_to_unique();
        CHECK(sp2.use_count() == 0);
        CHECK(sp2.get() == nullptr);
    }

    SECTION("unsharable_shared_ptr<Foo>(unsharable_shared_ptr<Bar>) and convert to unique pointer")
    {
        unsharable_shared_ptr<Foo> sp1 = make_unsharable_shared<Bar>();
        CHECK(sp1.use_count() == 1);

        auto uptr = sp1.exclusive_shared_to_unique();
        CHECK(sp1.use_count() == 0);
        CHECK(sp1.get() == nullptr);
    }
}
