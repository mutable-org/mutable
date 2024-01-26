#include "catch2/catch.hpp"

#include <mutable/util/Pool.hpp>


using namespace m;


TEST_CASE("Pool c'tor", "[core][util][pool]")
{
    PODPool<int> pool(42);
    REQUIRE(pool.size() == 0);
}

TEST_CASE("Pool internalize simple", "[core][util][pool]")
{
    PODPool<int> pool;

    auto i0 = pool(42);
    REQUIRE(pool.size() == 1);
    REQUIRE(*i0 == 42);

    auto i1 = pool(13);
    REQUIRE(pool.size() == 2);
    REQUIRE(*i1 == 13);

    auto i2 = pool(42);
    REQUIRE(pool.size() == 2);
    REQUIRE(*i2 == 42);
    REQUIRE(i0 == i2);
}

TEST_CASE("Pool internalize object", "[core][util][pool]")
{
    struct Object
    {
        int *p = nullptr;

        Object() : p(nullptr) { }
        Object(int n) : p(new int(n)) { }
        Object(const Object &other) = delete;
        Object(Object &&other) : Object() { std::swap(this->p, other.p); }
        ~Object() { delete p; }

        bool operator==(const Object &other) const { return *this->p == *other.p; }
    };
    struct hash
    {
        auto operator()(const Object &o) const { return std::hash<int>{}(*o.p); }
    };

    Object o42(42);
    Object o13(13);

    Pool<Object, hash> pool;

    auto i0 = pool(Object(42));
    REQUIRE(pool.size() == 1);
    REQUIRE(*i0->p == 42);

    auto i1 = pool(Object(13));
    REQUIRE(pool.size() == 2);
    REQUIRE(*i1->p == 13);

    auto i2 = pool(Object(42));
    REQUIRE(pool.size() == 2);
    REQUIRE(*i2->p == 42);
    REQUIRE(i0 == i2);
}

TEST_CASE("StringPool c'tor", "[core][util][pool]")
{
    StringPool pool(42);
    REQUIRE(pool.size() == 0);
}

TEST_CASE("StringPool internalize", "[core][util][pool]")
{
    StringPool pool;

    REQUIRE(pool.size() == 0);

    auto s0 = pool("Hello");
    REQUIRE(pool.size() == 1);

    auto s1 = pool("hello");
    REQUIRE(pool.size() == 2);
    REQUIRE(s0 != s1);

    auto s2 = pool(std::string_view("hello"));
    REQUIRE(pool.size() == 2);
    REQUIRE(s1 == s2);

    auto s3 = pool("");
    REQUIRE(pool.size() == 3);

    auto s4 = pool(std::string_view(""));
    REQUIRE(s3 == s4);
}


TEST_CASE("PooledOptionalString Utilization", "[core][util][pool]")
{
    StringPool pool;

    SECTION("empty PooledOptionalString")
    {
        // default constructing an empty PooledOptionalString should not affect the pool
        PooledOptionalString ps0;
        REQUIRE(pool.size() == 0);
        REQUIRE(ps0.has_value() == false);
        REQUIRE(ps0.count() == 0);

        // copy constructing an empty PooledOptionalString should not affect the pool
        PooledOptionalString ps1{ ps0 };
        REQUIRE(pool.size() == 0);
        REQUIRE(ps1.has_value() == false);
        REQUIRE(ps1.count() == 0);

        // move constructing an empty PooledOptionalString should not affect the pool
        PooledOptionalString ps2{ std::move(ps0) };
        REQUIRE(pool.size() == 0);
        REQUIRE(ps2.has_value() == false);
        REQUIRE(ps2.count() == 0);
        REQUIRE(ps0.count() == 0);
    }

    SECTION("PooledOptionalString with value")
    {
        // constructing a PooledOptionalString with a string should affect the pool
        PooledOptionalString ps0{ pool("ps0") };
        REQUIRE(pool.size() == 1);
        REQUIRE(ps0.has_value());
        REQUIRE(ps0.count() == 1);

        // copy constructing a non-empty PooledOptionalString should not affect the pool
        PooledOptionalString ps1{ ps0 };
        REQUIRE(pool.size() == 1);
        REQUIRE(ps0.has_value());
        REQUIRE(ps1.has_value());
        REQUIRE(ps0 == ps1);
        REQUIRE(ps0.count() == 2);
        REQUIRE(ps1.count() == 2);

        // move constructing a non-empty PooledOptionalString should not affect the pool
        PooledOptionalString ps2{ std::move(ps0) };
        REQUIRE(pool.size() == 1);
        REQUIRE(ps2.has_value());
        REQUIRE(ps0.has_value() == false);
        REQUIRE(ps0.count() == 0);

        REQUIRE(ps1 == ps2);
        REQUIRE(streq(*ps2, "ps0"));
        REQUIRE(ps1.count() == 2);
        REQUIRE(ps2.count() == 2);
    }

    SECTION("nested scope assignment and garbage collection")
    {
        REQUIRE(pool.size() == 0);
        {
            PooledOptionalString ps0;
            {
                ps0 = pool("ps0");
            }
            REQUIRE(pool.size() == 1);
            REQUIRE(ps0.has_value());
            REQUIRE(streq(*ps0, "ps0"));
        }
        REQUIRE(pool.size() == 1); // XXX: TODO: size() should return 0 after garbage collection is implemented
    }
}

TEST_CASE("Interaction of optional & non-optional", "[core][util][pool]")
{
    StringPool pool;

    REQUIRE(pool.size() == 0);
    {
        PooledOptionalString pos0{ pool("pos") };
        PooledString ps0{ pool("ps") };
        REQUIRE(pool.size() == 2);

        SECTION("conversion c'tor: non-optional -> optional")
        {
            PooledOptionalString pos1{ ps0 };
            REQUIRE(pool.size() == 2);
            REQUIRE(pos1.has_value());
            REQUIRE(pos1.count() == 2);
            REQUIRE(streq(*pos1, "ps"));
        }

        SECTION("conversion c'tor: optional -> non-optional")
        {
            PooledString ps1{ PooledString(pos0) };
            REQUIRE(pool.size() == 2);
            REQUIRE(pos0.has_value());
            REQUIRE(ps1.count() == 2);
            REQUIRE(streq(*ps1, "pos"));
        }

        SECTION("conversion move c'tor: non-optional -> optional")
        {
            PooledOptionalString pos2 { std::move(ps0) };
            REQUIRE(pool.size() == 2);
            REQUIRE(pos2.has_value());
            REQUIRE(pos2.count() == 1);
            REQUIRE(ps0.count() == 0);
            REQUIRE(streq(*pos2, "ps"));
        }

        SECTION("conversion move c'tor: optional -> non-optional")
        {
            PooledString ps2{ std::move(pos0) };
            REQUIRE(pool.size() == 2);
            REQUIRE(ps2.count() == 1);
            REQUIRE(pos0.count() == 0);
            REQUIRE(streq(*ps2, "pos"));
        }
    }

    // Ensure no leftovers are left in the pool
    REQUIRE(pool.size() == 2); // XXX: TODO: size() should return 0 after garbage collection is implemented
}
