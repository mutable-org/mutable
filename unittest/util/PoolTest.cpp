#include "catch2/catch.hpp"

#include <mutable/util/Pool.hpp>


using namespace m;


TEST_CASE("Pool c'tor", "[core][util][pool]")
{
    Pool<int> pool(42);
    REQUIRE(pool.size() == 0);
}

TEST_CASE("Pool internalize simple", "[core][util][pool]")
{
    Pool<int> pool;

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
        Object() : p(nullptr) { }
        Object(int n) : p(new int(n)) { }
        Object(const Object &other) = delete;
        Object(Object &&other) { std::swap(this->p, other.p); }
        ~Object() { delete p; }
        int *p = nullptr;

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
