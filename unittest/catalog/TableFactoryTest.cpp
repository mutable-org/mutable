#include "catch2/catch.hpp"

#include <cmath>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/util/fn.hpp>


using namespace m;


TEST_CASE("ConcreteTableFactory make", "[core][catalog][tablefactory]")
{
    auto &C = Catalog::Get();
    std::unique_ptr<TableFactory> table_factory = std::make_unique<ConcreteTableFactory>();

    auto r = table_factory->make(C.pool("mytable"));

    CHECK(is<ConcreteTable>(r.get()));
    CHECK(streq(*r->name(), "mytable"));
    CHECK(r->num_attrs() == 0);
    CHECK(r->num_hidden_attrs() == 0);
    CHECK(r->num_all_attrs() == 0);
}

TEST_CASE("MultiVersioningTableFactory make", "[core][catalog][tablefactory]")
{
    auto &C = Catalog::Get();
    std::unique_ptr<TableFactory> table_factory = std::make_unique<ConcreteTableFactoryDecorator<MultiVersioningTable>>(std::make_unique<ConcreteTableFactory>());

    auto r = table_factory->make(C.pool("mytable"));

    CHECK(is<MultiVersioningTable>(r.get()));
    CHECK(streq(*r->name(), "mytable"));
    CHECK(r->num_attrs() == 0);
    CHECK(r->num_hidden_attrs() == 2);
    CHECK(r->num_all_attrs() == 2);

    const PrimitiveType *i8 = Type::Get_Integer(Type::TY_Vector, 8);
    auto &attr0 = (*r)[(int)0];
    REQUIRE(&attr0 == &(*r)[attr0.id]);
    REQUIRE(attr0.table == *r);
    REQUIRE(&attr0.table != r.get());
    REQUIRE(*attr0.type == *i8);
    REQUIRE(streq(*attr0.name, "$ts_begin"));
    auto &attr1 = (*r)[1];
    REQUIRE(&attr1 == &(*r)[attr1.id]);
    REQUIRE(attr1.table == *r);
    REQUIRE(&attr1.table != r.get());
    REQUIRE(*attr1.type == *i8);
    REQUIRE(streq(*attr1.name, "$ts_end"));
}
