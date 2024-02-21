#include "catch2/catch.hpp"

#include <cmath>
#include <cstring>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/util/fn.hpp>
#include <sstream>
#include <stdexcept>
#include <string>


using namespace m;


namespace {

std::string get_unique_id()
{
    static unsigned id = 0;
    return std::to_string(id++);
}

}

TEST_CASE("ConcreteTable c'tor", "[core][catalog][schema]")
{
    Catalog &C = Catalog::Get();
    ConcreteTable r(C.pool("mytable"));

    CHECK(streq(*r.name(), "mytable"));
    CHECK(r.num_attrs() == 0);
    CHECK(r.num_hidden_attrs() == 0);
    CHECK(r.num_all_attrs() == 0);
}

TEST_CASE("ConcreteTable empty access", "[core][catalog][schema]")
{
    Catalog &C = Catalog::Get();
    ConcreteTable r(C.pool("mytable"));

    REQUIRE_THROWS_AS(r.at(C.pool("attribute")), std::out_of_range);

    for (auto it = r.cbegin(), end = r.cend(); it != end; ++it)
        REQUIRE(((void) "this code must be dead or the table is not empty", false));
}

TEST_CASE("ConcreteTable::push_back()", "[core][catalog][schema]")
{
    Catalog &C = Catalog::Get();
    ConcreteTable r(C.pool("mytable"));

    const PrimitiveType *i4 = Type::Get_Integer(Type::TY_Vector, 4);
    const PrimitiveType *vc = Type::Get_Varchar(Type::TY_Vector, 42);
    const PrimitiveType *b = Type::Get_Boolean(Type::TY_Vector);

    r.push_back(C.pool("n"), i4);
    r.push_back(C.pool("comment"), vc);
    r.push_back(C.pool("condition"), b);

    REQUIRE(r.num_attrs() == 3);
    REQUIRE(r.num_hidden_attrs() == 0);
    REQUIRE(r.num_all_attrs() == 3);

    auto &attr = r[1];
    REQUIRE(&attr == &r[attr.id]);
    REQUIRE(&attr.table == &r);
    REQUIRE(attr.type == vc);
    REQUIRE(streq(*attr.name, "comment"));
}

TEST_CASE("ConcreteTable iterators", "[core][catalog][schema]")
{
    Catalog &C = Catalog::Get();
    ConcreteTable r(C.pool("mytable"));
    const PrimitiveType *i4 = Type::Get_Integer(Type::TY_Vector, 4);

    r.push_back(C.pool("a"), i4);
    r.push_back(C.pool("b"), i4);
    r.push_back(C.pool("c"), i4);
    CHECK(r.num_attrs() == 3);
    CHECK(r.num_hidden_attrs() == 0);
    CHECK(r.num_all_attrs() == 3);

    SECTION("iterator") {
        auto it = r.cbegin();
        REQUIRE(streq(*it->name, "a"));
        ++it;
        REQUIRE(streq(*it->name, "b"));
        ++it;
        REQUIRE(streq(*it->name, "c"));
        ++it;
        REQUIRE(it == r.cend());
    }

    SECTION("hidden iterator") {
        auto it = r.cbegin_hidden();
        REQUIRE(it == r.cend_hidden());
    }

    SECTION("all iterator") {
        auto it = r.cbegin_all();
        REQUIRE(streq(*it->name, "a"));
        ++it;
        REQUIRE(streq(*it->name, "b"));
        ++it;
        REQUIRE(streq(*it->name, "c"));
        ++it;
        REQUIRE(it == r.cend_all());
    }
}

TEST_CASE("ConcreteTable get attribute by name", "[core][catalog][schema]")
{
    Catalog &C = Catalog::Get();
    ConcreteTable r(C.pool("mytable"));
    const PrimitiveType *i4 = Type::Get_Integer(Type::TY_Vector, 4);

    r.push_back(C.pool("a"), i4);
    r.push_back(C.pool("b"), i4);
    r.push_back(C.pool("c"), i4);
    REQUIRE(r.num_attrs() == 3);
    REQUIRE(r.num_hidden_attrs() == 0);
    REQUIRE(r.num_all_attrs() == 3);

    {
        auto &attr = r[C.pool("a")];
        REQUIRE(streq(*attr.name, "a"));
    }
    {
        auto &attr = r[C.pool("b")];
        REQUIRE(streq(*attr.name, "b"));
    }
    {
        auto &attr = r[C.pool("c")];
        REQUIRE(streq(*attr.name, "c"));
    }
}

TEST_CASE("ConcreteTable::push_back() duplicate name", "[core][catalog][schema]")
{
    Catalog &C = Catalog::Get();
    ConcreteTable r(C.pool("mytable"));
    const PrimitiveType *i4 = Type::Get_Integer(Type::TY_Vector, 4);

    ThreadSafePooledString attr_name = C.pool("a");

    r.push_back(attr_name, i4);
    REQUIRE_THROWS_AS(r.push_back(attr_name, i4), std::invalid_argument); // duplicate
}

TEST_CASE("MultiVersioningTable c'tor", "[core][catalog][schema]")
{
    auto &C = Catalog::Get();
    MultiVersioningTable r(std::make_unique<ConcreteTable>(C.pool("mytable")));

    CHECK(streq(*r.name(), "mytable"));
    CHECK(r.num_attrs() == 0);
    CHECK(r.num_hidden_attrs() == 2);
    CHECK(r.num_all_attrs() == 2);

    const PrimitiveType *i8 = Type::Get_Integer(Type::TY_Vector, 8);
    auto &attr0 = r[(int)0];
    REQUIRE(&attr0 == &r[attr0.id]);
    REQUIRE(attr0.table == r);
    REQUIRE(&attr0.table != &r);
    REQUIRE(*attr0.type == *i8);
    REQUIRE(streq(*attr0.name, "$ts_begin"));
    auto &attr1 = r[1];
    REQUIRE(&attr1 == &r[attr1.id]);
    REQUIRE(attr1.table == r);
    REQUIRE(&attr1.table != &r);
    REQUIRE(*attr1.type == *i8);
    REQUIRE(streq(*attr1.name, "$ts_end"));
}

TEST_CASE("MultiVersioningTable out of range access", "[core][catalog][schema]")
{
    auto &C = Catalog::Get();
    MultiVersioningTable r(std::make_unique<ConcreteTable>(C.pool("mytable")));

    REQUIRE_THROWS_AS(r.at(C.pool("attribute")), std::out_of_range);
    REQUIRE_THROWS_AS(r.at(2), std::out_of_range);
}

TEST_CASE("MultiVersioningTable::push_back()", "[core][catalog][schema]")
{
    auto &C = Catalog::Get();
    MultiVersioningTable r(std::make_unique<ConcreteTable>(C.pool("mytable")));

    const PrimitiveType *i4 = Type::Get_Integer(Type::TY_Vector, 4);
    const PrimitiveType *vc = Type::Get_Varchar(Type::TY_Vector, 42);
    const PrimitiveType *b = Type::Get_Boolean(Type::TY_Vector);

    r.push_back(C.pool("n"), i4);
    r.push_back(C.pool("comment"), vc);
    r.push_back(C.pool("condition"), b);

    CHECK(r.num_attrs() == 3);
    CHECK(r.num_hidden_attrs() == 2);
    CHECK(r.num_all_attrs() == 5);

    auto &attr = r[3];
    REQUIRE(&attr == &r[attr.id]);
    REQUIRE(&attr.table != &r);
    REQUIRE(attr.table == r);
    REQUIRE(attr.type == vc);
    REQUIRE(streq(*attr.name, "comment"));
}

TEST_CASE("MultiVersioningTable iterators", "[core][catalog][schema]")
{
    auto &C = Catalog::Get();
    MultiVersioningTable r(std::make_unique<ConcreteTable>(C.pool("mytable")));
    const PrimitiveType *i4 = Type::Get_Integer(Type::TY_Vector, 4);

    r.push_back(C.pool("a"), i4);
    r.push_back(C.pool("b"), i4);
    r.push_back(C.pool("c"), i4);

    CHECK(r.num_attrs() == 3);
    CHECK(r.num_hidden_attrs() == 2);
    CHECK(r.num_all_attrs() == 5);

    SECTION("iterator") {
        auto it = r.cbegin();
        REQUIRE(streq(*it->name, "a"));
        ++it;
        REQUIRE(streq(*it->name, "b"));
        ++it;
        REQUIRE(streq(*it->name, "c"));
        ++it;
        REQUIRE(it == r.cend());
    }

    SECTION("hidden iterator") {
        auto it = r.cbegin_hidden();
        REQUIRE(streq(*it->name, "$ts_begin"));
        ++it;
        REQUIRE(streq(*it->name, "$ts_end"));
        ++it;
        REQUIRE(it == r.cend_hidden());
    }

    SECTION("all iterator") {
        auto it = r.cbegin_all();
        REQUIRE(streq(*it->name, "$ts_begin"));
        ++it;
        REQUIRE(streq(*it->name, "$ts_end"));
        ++it;
        REQUIRE(streq(*it->name, "a"));
        ++it;
        REQUIRE(streq(*it->name, "b"));
        ++it;
        REQUIRE(streq(*it->name, "c"));
        ++it;
        REQUIRE(it == r.cend_all());
    }
}

TEST_CASE("MultiVersioningTable get attribute by name", "[core][catalog][schema]")
{
    auto &C = Catalog::Get();
    MultiVersioningTable r(std::make_unique<ConcreteTable>(C.pool("mytable")));
    const PrimitiveType *i4 = Type::Get_Integer(Type::TY_Vector, 4);

    r.push_back(C.pool("a"), i4);
    r.push_back(C.pool("b"), i4);
    r.push_back(C.pool("c"), i4);

    CHECK(r.num_attrs() == 3);
    CHECK(r.num_hidden_attrs() == 2);
    CHECK(r.num_all_attrs() == 5);

    {
        auto & attr = r[C.pool("$ts_begin")];
        REQUIRE(streq(*attr.name, "$ts_begin"));
    }
    {
        auto & attr = r[C.pool("$ts_end")];
        REQUIRE(streq(*attr.name, "$ts_end"));
    }
    {
        auto & attr = r[C.pool("a")];
        REQUIRE(streq(*attr.name, "a"));
    }
    {
        auto &attr = r[C.pool("b")];
        REQUIRE(streq(*attr.name, "b"));
    }
    {
        auto &attr = r[C.pool("c")];
        REQUIRE(streq(*attr.name, "c"));
    }
}

TEST_CASE("MultiVersioningTable::push_back() duplicate name", "[core][catalog][schema]")
{
    auto &C = Catalog::Get();
    MultiVersioningTable r(std::make_unique<ConcreteTable>(C.pool("mytable")));
    const PrimitiveType *i8 = Type::Get_Integer(Type::TY_Vector, 8);

    REQUIRE_THROWS_AS(r.push_back(C.pool("$ts_begin"), i8), std::invalid_argument); // duplicate
    REQUIRE_THROWS_AS(r.push_back(C.pool("$ts_end"), i8), std::invalid_argument); // duplicate
}

TEST_CASE("MultiVersioningTable Datalayout", "[core][catalog][schema]")
{
    Catalog &C = Catalog::Get();
    MultiVersioningTable r(std::make_unique<ConcreteTable>(C.pool("mytable")));
    const PrimitiveType *i4 = Type::Get_Integer(Type::TY_Vector, 4);

    r.push_back(C.pool("a"), i4);
    r.push_back(C.pool("b"), i4);
    r.push_back(C.pool("c"), i4);

    r.layout(C.data_layout());
    auto &layout = r.layout();
    layout.for_sibling_leaves([&](const std::vector<m::storage::DataLayout::leaf_info_t> &leaves,
                                  const m::storage::DataLayout::level_info_stack_t &levels,
                                  uint64_t inode_offset_in_bits)
    {
        REQUIRE(leaves.size() == r.num_all_attrs() + 1 /* NULL bitmap */);
    });
}

TEST_CASE("Table Convert Non-Hidden Attribute ID", "[core][catalog][schema]")
{
    Catalog &C = Catalog::Get();
    ConcreteTable r(C.pool("mytable"));
    const PrimitiveType *i4 = Type::Get_Integer(Type::TY_Vector, 4);

    r.push_back(C.pool("a"), i4);
    r.push_back(C.pool("b"), i4);
    r.push_back(C.pool("c"), i4);
    r.push_back(C.pool("d"), i4);
    r.push_back(C.pool("e"), i4);
    r[C.pool("b")].is_hidden = true;
    r[C.pool("d")].is_hidden = true;

    REQUIRE(r.convert_id(0) == 0);
    REQUIRE(r.convert_id(1) == 2);
    REQUIRE(r.convert_id(2) == 4);
}

// XXX: This might not be the right place for the following tests.
TEST_CASE("Catalog singleton c'tor", "[core][catalog]")
{
    Catalog &C = Catalog::Get();
    Catalog &C2 = Catalog::Get();
    REQUIRE(&C == &C2);
    Catalog::Clear();
}

TEST_CASE("Catalog Database creation", "[core][catalog]")
{
    Catalog &C = Catalog::Get();
    ThreadSafePooledString db_name = C.pool(get_unique_id());
    Database &D = C.add_database(db_name);
    Database &D2 = C.get_database(db_name);
    REQUIRE(&D == &D2);
    REQUIRE(D.name == db_name);
    Catalog::Clear();
}

TEST_CASE("Catalog::drop_database() by name", "[core][catalog]")
{
    Catalog &C = Catalog::Get();
    ThreadSafePooledString db_name = C.pool(get_unique_id());
    C.add_database(db_name);

    REQUIRE_NOTHROW(C.get_database(db_name));
    REQUIRE_NOTHROW(C.drop_database(db_name)); // ok
    CHECK_THROWS_AS(C.get_database(db_name), std::out_of_range); // already deleted
    REQUIRE_THROWS_AS(C.drop_database(C.pool("nodb")), std::invalid_argument); // does not exist
    Catalog::Clear();
}

TEST_CASE("Catalog::drop_database() by reference", "[core][catalog]")
{
    Catalog &C = Catalog::Get();
    ThreadSafePooledString db_name = C.pool(get_unique_id());
    Database &D = C.add_database(db_name);

    C.set_database_in_use(D);
    REQUIRE_THROWS_AS(C.drop_database(db_name), std::invalid_argument); // db in use

    C.unset_database_in_use();
    REQUIRE_NOTHROW(C.get_database(db_name)); // ok
    REQUIRE_NOTHROW(C.drop_database(D)); // ok
    CHECK_THROWS_AS(C.get_database(db_name), std::out_of_range); // not found
    Catalog::Clear();
}

TEST_CASE("Catalog use database", "[core][catalog]")
{
    Catalog &C = Catalog::Get();
    ThreadSafePooledString db_name = C.pool(get_unique_id());

    C.unset_database_in_use();
    Database &D = C.add_database(db_name);
    REQUIRE(not C.has_database_in_use());
    C.set_database_in_use(D);
    REQUIRE(C.has_database_in_use());
    auto &in_use = C.get_database_in_use();
    REQUIRE(&D == &in_use);
    C.unset_database_in_use();
    REQUIRE(not C.has_database_in_use());
    Catalog::Clear();
}

TEST_CASE("Database c'tor", "[core][catalog][database]")
{
    Catalog &C = Catalog::Get();
    ThreadSafePooledString db_name = C.pool(get_unique_id());
    Database &D = C.add_database(db_name);
    REQUIRE(D.size() == 0);
    Catalog::Clear();
}

TEST_CASE("Database/add table error if name already taken", "[core][catalog][database]")
{
    Catalog &C = Catalog::Get();
    ThreadSafePooledString db_name = C.pool(get_unique_id());
    Database &D = C.add_database(db_name);

    auto tbl_name = C.pool("mytable");
    D.add_table(tbl_name);
    std::unique_ptr<Table> R = std::make_unique<ConcreteTable>(tbl_name);
    REQUIRE_THROWS_AS(D.add(std::move(R)), std::invalid_argument);
    Catalog::Clear();
}
