#include "catch.hpp"

#include "util/Pool.hpp"


using namespace db;


TEST_CASE("Pool c'tor", "[unit][util]")
{
    Pool<int> pool(42);
    REQUIRE(pool.size() == 0);
}

TEST_CASE("Pool internalize simple", "[unit][util]")
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

TEST_CASE("Pool internalize object", "[unit][util]")
{
    struct Object
    {
        Object() : p(nullptr) { }
        Object(int n) : p(new int(n)) { }
        Object(const Object &other) : p(new int(*other.p)) { }
        Object(Object &&other) { std::swap(this->p, other.p); }
        ~Object() { delete p; }
        int *p;

        bool operator==(const Object &other) const { return *this->p == *other.p; }
    };
    struct hash
    {
        auto operator()(const Object &o) const { return std::hash<int>{}(*o.p); }
    };

    Object o42(42);
    Object o13(13);
    Object o42_(42);

    Pool<Object, hash> pool;

    auto i0 = pool(o42);
    REQUIRE(pool.size() == 1);
    REQUIRE(*i0->p == 42);

    auto i1 = pool(o13);
    REQUIRE(pool.size() == 2);
    REQUIRE(*i1->p == 13);

    auto i2 = pool(o42_);
    REQUIRE(pool.size() == 2);
    REQUIRE(*i2->p == 42);
    REQUIRE(i0 == i2);
}
