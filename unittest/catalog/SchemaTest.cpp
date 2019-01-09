#include "catch.hpp"

#include "catalog/Schema.hpp"
#include "util/fn.hpp"
#include <cmath>
#include <sstream>
#include <stdexcept>


using namespace db;


TEST_CASE("Type/CharacterSequence", "[unit]")
{
    SECTION("CHAR(N)")
    {
        const CharacterSequence *chr42 = Type::Get_Char(42);
        CHECK(not chr42->is_varying);
        CHECK(chr42->length == 42);
    }

    SECTION("VARCHAR(N)")
    {
        const CharacterSequence *chr42 = Type::Get_Varchar(42);
        CHECK(chr42->is_varying);
        CHECK(chr42->length == 42);
    }
}

TEST_CASE("Type/Numeric c'tor", "[unit]")
{
    using std::ceil;
    using std::log2;
    using std::pow;

    SECTION("8 byte integer")
    {
        const Numeric *i8 = Type::Get_Integer(8);
        CHECK(i8->kind == Numeric::N_Int);
        CHECK(i8->precision == 8);
        CHECK(i8->scale == 0);
    }

    SECTION("32 bit floating-point")
    {
        const Numeric *f = Type::Get_Float();
        CHECK(f->kind == Numeric::N_Float);
        CHECK(f->precision == 32);
    }

    SECTION("64 bit floating-point")
    {
        const Numeric *d = Type::Get_Double();
        CHECK(d->kind == Numeric::N_Float);
        CHECK(d->precision == 64);
    }

    SECTION("decimal")
    {
        unsigned decimal_precision;

        SECTION("4 digits: log₂(10^4) = 13.3")
        {
            decimal_precision = 4;
        }

        SECTION("5 digits: log₂(10^5) = 16.6")
        {
            decimal_precision = 5;
        }

        SECTION("9 digits: log₂(10^9) = 29.8")
        {
            decimal_precision = 9;
        }

        SECTION("10 digits: log₂(10^10) = 33.2")
        {
            decimal_precision = 10;
        }

        SECTION("19 digits: log₂(10^19) = 29.9")
        {
            decimal_precision = 19;
        }

        SECTION("20 digits: log₂(10^20) = 33.2")
        {
            decimal_precision = 20;
        }

        SECTION("38 digits: log₂(10^38) = 122.9")
        {
            decimal_precision = 38;
        }

        const Numeric *dec = Type::Get_Decimal(decimal_precision, 2);
        CHECK(dec->kind == Numeric::N_Decimal);
        CHECK(dec->precision == decimal_precision);
        CHECK(dec->scale == 2);
    }
}

TEST_CASE("Type/Numeric print()", "[unit]")
{
    std::ostringstream oss;

    /* 8 byte integer */
    const Numeric *i8 = Type::Get_Integer(8);
    oss << *i8;
    CHECK(oss.str() == "INT(8)");
    oss.str("");

    /* 32 bit floating-point */
    const Numeric *f = Type::Get_Float();
    oss << *f;
    CHECK(oss.str() == "FLOAT");
    oss.str("");

    /* 64 bit floating-point */
    const Numeric *d = Type::Get_Double();
    oss << *d;
    CHECK(oss.str() == "DOUBLE");
    oss.str("");

    /* 4 byte decimal: log2(10^9) = 29.8 */
    const Numeric *dec_9_2 = Type::Get_Decimal(9, 2);
    oss << *dec_9_2;
    CHECK(oss.str() == "DECIMAL(9, 2)");
    oss.str("");

    /* 10 digits: log2(10^10) = 33.2 -> 64 bit */
    const Numeric *dec_10_0 = Type::Get_Decimal(10, 0);
    oss << *dec_10_0;
    CHECK(oss.str() == "DECIMAL(10, 0)");
    oss.str("");

    /* 16 byte decimal: log2(10^38) - 122.9 */
    const Numeric *dec_38_20 = Type::Get_Decimal(38, 20);
    oss << *dec_38_20;
    CHECK(oss.str() == "DECIMAL(38, 20)");
    oss.str("");
}

TEST_CASE("Type internalize", "[unit]")
{
    SECTION("Boolean")
    {
        auto b = Type::Get_Boolean();
        auto b_ = Type::Get_Boolean();
        REQUIRE(b == b_);
    }

    SECTION("CharacterSequence")
    {
        auto vc42 = Type::Get_Varchar(42);
        auto vc42_ = Type::Get_Varchar(42);
        auto c42 = Type::Get_Char(42);

        REQUIRE(vc42 == vc42_);
        REQUIRE(vc42 != c42);
    }

    SECTION("Numeric")
    {
        auto i4 = Type::Get_Integer(4);
        auto i4_ = Type::Get_Integer(4);
        auto i8 = Type::Get_Integer(8);
        auto f = Type::Get_Float();
        auto f_ = Type::Get_Float();
        auto d = Type::Get_Double();
        auto d_ = Type::Get_Double();
        auto dec_9_2 = Type::Get_Decimal(9, 2);
        auto dec_9_2_ = Type::Get_Decimal(9, 2);
        auto dec_10_0 = Type::Get_Decimal(10, 0);

        REQUIRE(i4 == i4_);
        REQUIRE(i4 != i8);
        REQUIRE(f == f_);
        REQUIRE(f != d);
        REQUIRE(d == d_);
        REQUIRE(dec_9_2 == dec_9_2_);
        REQUIRE(dec_9_2 != dec_10_0);
    }
}

TEST_CASE("Relation c'tor")
{
    Relation r("myrelation");

    CHECK(streq(r.name, "myrelation"));
    CHECK(r.size() == 0);
}

TEST_CASE("Relation empty access")
{
    Relation r("myrelation");

    REQUIRE_THROWS_AS(r[42].id, std::out_of_range);
    REQUIRE_THROWS_AS(r["attribute"].id, std::out_of_range);

    for (auto it = r.cbegin(), end = r.cend(); it != end; ++it)
        REQUIRE(((void) "this code must be dead or the relation is not empty", false));
}

TEST_CASE("Relation::push_back()")
{
    Relation r("myrelation");

    const Type *i4 = Type::Get_Integer(4);
    const Type *vc = Type::Get_Varchar(42);
    const Type *b = Type::Get_Boolean();

    r.push_back(i4, "n");
    r.push_back(vc, "comment");
    r.push_back(b, "condition");

    REQUIRE(r.size() == 3);

    auto &attr = r[1];
    REQUIRE(&attr == &r[attr.id]);
    REQUIRE(&attr.relation == &r);
    REQUIRE(attr.type == vc);
    REQUIRE(streq(attr.name, "comment"));
}

TEST_CASE("Relation iterators")
{
    Relation r("myrelation");
    const Type *i4 = Type::Get_Integer(4);

    r.push_back(i4, "a");
    r.push_back(i4, "b");
    r.push_back(i4, "c");
    REQUIRE(r.size() == 3);

    auto it = r.cbegin();
    REQUIRE(streq(it->name, "a"));
    ++it;
    REQUIRE(streq(it->name, "b"));
    ++it;
    REQUIRE(streq(it->name, "c"));
    ++it;
    REQUIRE(it == r.cend());
}

TEST_CASE("Relation get attribute by name")
{
    Relation r("myrelation");
    const Type *i4 = Type::Get_Integer(4);

    r.push_back(i4, "a");
    r.push_back(i4, "b");
    r.push_back(i4, "c");
    REQUIRE(r.size() == 3);

    {
        auto &attr = r["a"];
        REQUIRE(streq(attr.name, "a"));
    {
        auto &attr = r["b"];
        REQUIRE(streq(attr.name, "b"));
    }
    {
        auto &attr = r["c"];
        REQUIRE(streq(attr.name, "c"));
    }
    }
}

TEST_CASE("Relation::push_back() error if name alreay taken")
{
    Relation r("myrelation");
    const Type *i4 = Type::Get_Integer(4);

    r.push_back(i4, "a");
    REQUIRE_THROWS_AS(r.push_back(i4, "a"), std::invalid_argument);
}

TEST_CASE("Catalog singleton c'tor")
{
    Catalog &C = Catalog::Get();
    Catalog &C2 = Catalog::Get();
    REQUIRE(&C == &C2);
    REQUIRE(C.num_schemas() == 0);
    REQUIRE(not C.has_database_in_use());
}

TEST_CASE("Catalog Schema creation")
{
    Catalog &C = Catalog::Get();
    Schema &S = C.get_or_add_database("myschema");
    Schema &S2 = C.get_or_add_database("myschema");
    REQUIRE(&S == &S2);
    REQUIRE(streq(S.name, "myschema"));
}

TEST_CASE("Catalog use database")
{
    Catalog &C = Catalog::Get();
    Schema &S1 = C.get_or_add_database("myschema");
    REQUIRE(not C.has_database_in_use());
    C.set_database_in_use(S1);
    REQUIRE(C.has_database_in_use());
    auto &in_use = C.get_database_in_use();
    REQUIRE(&S1 == &in_use);
    C.unset_database_in_use();
    REQUIRE(not C.has_database_in_use());
}

TEST_CASE("Schema c'tor")
{
    Catalog &C = Catalog::Get();
    Schema &S = C.get_or_add_database("myschema");
    REQUIRE(S.size() == 0);
}

TEST_CASE("Schema/add relation error if name already taken")
{
    Catalog &C = Catalog::Get();
    const char *name = "myrelation";
    Schema &S = C.get_or_add_database("myschema");

    S.get_or_add_relation(name);
    Relation *R = new Relation(name);
    REQUIRE_THROWS_AS(S.add(R), std::invalid_argument);
    delete R;
}
