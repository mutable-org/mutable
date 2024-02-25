#include "catch2/catch.hpp"

#include <functional>
#include <mutable/util/Pool.hpp>
#include <string_view>
#include <thread>


using namespace m;
using namespace std::string_view_literals;


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
    struct Base
    {
        int *p = nullptr;

        Base() : p(nullptr) { }
        Base(int n) : p(new int(n)) { }
        Base(const Base &other) = delete;
        Base(Base &&other) : Base() { std::swap(this->p, other.p); }
        virtual ~Base() { delete p; }

        bool operator==(const Base &other) const { return *this->p == *other.p; }
    };
    struct hash
    {
        auto operator()(const Base &o) const { return std::hash<int>{}(*o.p); }
    };

    struct Derived : Base
    {
        std::string name;
        Derived(std::string name, int value) : Base(value), name(name) { }
    };

    Pool<Base, hash> pool;

    auto i0 = pool(Base(42));
    REQUIRE(pool.size() == 1);
    REQUIRE(*i0->p == 42);

    auto i1 = pool(Base(13));
    REQUIRE(pool.size() == 2);
    REQUIRE(*i1->p == 13);

    auto i2 = pool(Base(42));
    REQUIRE(pool.size() == 2);
    REQUIRE(*i2->p == 42);
    REQUIRE(i0 == i2);

    auto i3 = pool(Derived("a", 37));
    REQUIRE(pool.size() == 3);
    REQUIRE(*i3->p == 37);
    REQUIRE(i3->name == "a");

    auto derived = i3.as<Derived>();
    REQUIRE(pool.size() == 3);
    REQUIRE(*derived->p == 37);
    REQUIRE(i3->name == "a");

    auto base = derived.as<Base>();
    REQUIRE(pool.size() == 3);
    REQUIRE(*base->p == 37);
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
        CHECK(*ps0.assert_not_none() == "ps0"sv);
        REQUIRE(ps0.count() == 1);

        // copy constructing a non-empty PooledOptionalString should not affect the pool
        PooledOptionalString ps1{ ps0 };
        REQUIRE(pool.size() == 1);
        REQUIRE(ps0.has_value());
        REQUIRE(ps1.has_value());
        CHECK(*ps1.assert_not_none() == "ps0"sv);
        REQUIRE(ps0 == ps1);
        REQUIRE(ps0.count() == 2);
        REQUIRE(ps1.count() == 2);

        // move constructing a non-empty PooledOptionalString should not affect the pool
        PooledOptionalString ps2{ std::move(ps0) };
        REQUIRE(pool.size() == 1);
        REQUIRE(ps2.has_value());
        REQUIRE(ps0.has_value() == false);
        CHECK(*ps2.assert_not_none() == "ps0"sv);
        REQUIRE(ps0.count() == 0);

        REQUIRE(ps1 == ps2);
        REQUIRE(*ps2 == "ps0"sv);
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
            REQUIRE(*ps0 == "ps0"sv);
        }
        REQUIRE(pool.size() == 1); // TODO: size() should return 0 after garbage collection is implemented
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
            REQUIRE(*pos1 == "ps"sv);
        }

        SECTION("conversion c'tor: optional -> non-optional")
        {
            PooledString ps1{ PooledString(pos0) };
            REQUIRE(pool.size() == 2);
            REQUIRE(pos0.has_value());
            REQUIRE(ps1.count() == 2);
            REQUIRE(*ps1 == "pos"sv);
        }

        SECTION("conversion move c'tor: non-optional -> optional")
        {
            PooledOptionalString pos2 { std::move(ps0) };
            REQUIRE(pool.size() == 2);
            REQUIRE(pos2.has_value());
            REQUIRE(pos2.count() == 1);
            REQUIRE(ps0.count() == 0);
            REQUIRE(*pos2 == "ps"sv);
        }

        SECTION("conversion move c'tor: optional -> non-optional")
        {
            PooledString ps2{ std::move(pos0) };
            REQUIRE(pool.size() == 2);
            REQUIRE(ps2.count() == 1);
            REQUIRE(pos0.count() == 0);
            REQUIRE(*ps2 == "pos"sv);
        }
    }

    // Ensure no leftovers are left in the pool
    REQUIRE(pool.size() == 2); // TODO: size() should return 0 after garbage collection is implemented
}

TEST_CASE("Thread-safe concurrent PODPool", "[core][util][pool]")
{
    ThreadSafePODPool<int> pool;

    constexpr int NUM_VALUES = 1000;
    std::vector<int> values(NUM_VALUES);
    std::iota(values.begin(), values.end(), 0);
    std::mt19937_64 gen{42};

    std::shuffle(values.begin(), values.end(), gen);
    const auto values_t1 = values;

    std::shuffle(values.begin(), values.end(), gen);
    const auto values_t2 = values;

    std::shuffle(values.begin(), values.end(), gen);
    const auto values_t3 = values;

    using refs_vector_t = std::vector<decltype(pool)::proxy_type>;
    refs_vector_t refs_t1, refs_t2, refs_t3;
    refs_t1.reserve(NUM_VALUES);
    refs_t2.reserve(NUM_VALUES);
    refs_t3.reserve(NUM_VALUES);

    auto internalize = [&pool](const std::vector<int> &values, refs_vector_t &refs) -> void {
        for (auto v : values)
            refs.emplace_back(pool(v));
    };

    std::thread t1(internalize, std::cref(values_t1), std::ref(refs_t1));
    std::thread t2(internalize, std::cref(values_t2), std::ref(refs_t2));
    std::thread t3(internalize, std::cref(values_t3), std::ref(refs_t3));

    t1.join();
    t2.join();
    t3.join();

    auto validate = [](const std::vector<int> &values, const refs_vector_t &refs) {
        REQUIRE(values.size() == refs.size());
        for (std::size_t idx = 0; idx != values.size(); ++idx)
            CHECK(values[idx] == *refs[idx]);
    };

    validate(values_t1, refs_t1);
    validate(values_t2, refs_t2);
    validate(values_t3, refs_t3);
}

TEST_CASE("Thread-safe concurrent Pool", "[core][util][pool]")
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

    ThreadSafePool<Object, hash> pool;

    constexpr int NUM_VALUES = 1000;
    std::vector<int> values(NUM_VALUES);
    std::iota(values.begin(), values.end(), 0);
    std::mt19937_64 gen{42};

    std::shuffle(values.begin(), values.end(), gen);
    const auto values_t1 = values;

    std::shuffle(values.begin(), values.end(), gen);
    const auto values_t2 = values;

    std::shuffle(values.begin(), values.end(), gen);
    const auto values_t3 = values;

    using refs_vector_t = std::vector<decltype(pool)::proxy_type<Object>>;
    refs_vector_t refs_t1, refs_t2, refs_t3;
    refs_t1.reserve(NUM_VALUES);
    refs_t2.reserve(NUM_VALUES);
    refs_t3.reserve(NUM_VALUES);

    auto internalize = [&pool](const std::vector<int> &values, refs_vector_t &refs) -> void {
        for (auto v : values)
            refs.emplace_back(pool(Object{v}));
    };

    std::thread t1(internalize, std::cref(values_t1), std::ref(refs_t1));
    std::thread t2(internalize, std::cref(values_t2), std::ref(refs_t2));
    std::thread t3(internalize, std::cref(values_t3), std::ref(refs_t3));

    t1.join();
    t2.join();
    t3.join();

    auto validate = [](const std::vector<int> &values, const refs_vector_t &refs) {
        REQUIRE(values.size() == refs.size());
        for (std::size_t idx = 0; idx != values.size(); ++idx)
            CHECK(values[idx] == *refs[idx]);
    };

    validate(values_t1, refs_t1);
    validate(values_t2, refs_t2);
    validate(values_t3, refs_t3);
}
