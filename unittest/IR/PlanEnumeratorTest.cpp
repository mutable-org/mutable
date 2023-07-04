#include "catch2/catch.hpp"

#include <iostream>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/catalog/CostFunctionCout.hpp>
#include <mutable/catalog/Type.hpp>
#include <mutable/IR/PlanEnumerator.hpp>
#include <mutable/IR/PlanTable.hpp>
#include <mutable/mutable.hpp>
#include <mutable/storage/Store.hpp>
#include <mutable/util/ADT.hpp>
#include <parse/Parser.hpp>
#include <parse/Sema.hpp>
#include <testutil.hpp>


using namespace m;


/*======================================================================================================================
 * Helper funtctions for test setup.
 *====================================================================================================================*/

namespace pe_test {

template<typename PlanTable>
void init_PT_base_case(const QueryGraph &G, PlanTable &PT)
{
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();
    using Subproblem = SmallBitset;
    for (auto &ds : G.sources()) {
        Subproblem s(1UL << ds->id());
        auto bt = as<const BaseTable>(*ds);
        PT[s].cost = 0;
        PT[s].model = CE.estimate_scan(G, s);
    }
}

}


/*======================================================================================================================
 * Test Cost Function.
 *====================================================================================================================*/
TEST_CASE("PlanEnumerator", "[core][IR]")
{
    using Subproblem = SmallBitset;
    using PlanTable = PlanTableSmallOrDense;

    /* Get Catalog and create new database to use for unit testing. */
    Catalog::Clear();
    Catalog &Cat = Catalog::Get();
    auto &db = Cat.add_database("db");
    Cat.set_database_in_use(db);

    /* Create pooled strings. */
    const char *str_A    = Cat.pool("A");
    const char *str_B    = Cat.pool("B");
    const char *str_C    = Cat.pool("C");
    const char *str_D    = Cat.pool("D");

    const char *col_id = Cat.pool("id");
    const char *col_aid = Cat.pool("aid");
    const char *col_bid = Cat.pool("bid");
    const char *col_cid = Cat.pool("cid");

    /* Create tables. */
    Table &tbl_A = db.add_table(str_A);
    Table &tbl_B = db.add_table(str_B);
    Table &tbl_C = db.add_table(str_C);
    Table &tbl_D = db.add_table(str_D);

    /* Add columns to tables. */
    tbl_A.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_B.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_C.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_C.push_back(col_aid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_D.push_back(col_aid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_D.push_back(col_bid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_D.push_back(col_cid, Type::Get_Integer(Type::TY_Vector, 4));

    /* Add data to tables. */
    std::size_t num_rows_A = 5;
    std::size_t num_rows_B = 10;
    std::size_t num_rows_C = 8;
    std::size_t num_rows_D = 12;
    tbl_A.store(Cat.create_store(tbl_A));
    tbl_B.store(Cat.create_store(tbl_B));
    tbl_C.store(Cat.create_store(tbl_C));
    tbl_D.store(Cat.create_store(tbl_D));
    tbl_A.layout(Cat.data_layout());
    tbl_B.layout(Cat.data_layout());
    tbl_C.layout(Cat.data_layout());
    tbl_D.layout(Cat.data_layout());
    for (std::size_t i = 0; i < num_rows_A; ++i) { tbl_A.store().append(); }
    for (std::size_t i = 0; i < num_rows_B; ++i) { tbl_B.store().append(); }
    for (std::size_t i = 0; i < num_rows_C; ++i) { tbl_C.store().append(); }
    for (std::size_t i = 0; i < num_rows_D; ++i) { tbl_D.store().append(); }

    /* Define query:
     *
     *    C
     *   / \
     *  A---D---B
     */
    const std::string query = "\
SELECT * \
FROM A, B, C, D \
WHERE A.id = C.aid AND A.id = D.aid AND B.id = D.bid AND C.id = D.cid;";

    Diagnostic diag(false, std::cout, std::cerr);
    auto stmt = m::statement_from_string(diag, query);
    REQUIRE(not diag.num_errors());
    auto query_graph = QueryGraph::Build(*stmt);
    auto &G = *query_graph.get();

    const Subproblem A(1);
    const Subproblem B(2);
    const Subproblem C(4);
    const Subproblem D(8);
    CostFunctionCout C_out;
    CartesianProductEstimator CE;

    /* Initialize `PlanTable` for base case. */
    PlanTable plan_table(G);
    pe_test::init_PT_base_case(G, plan_table);

    PlanTable expected(G);
    pe_test::init_PT_base_case(G, expected);

    auto make_entry = [&](const Subproblem left, const Subproblem right) {
        cnf::CNF condition;
        auto &entry = expected[left|right];
        entry.left = left;
        entry.right = right;
        entry.model = CE.estimate_join(G, *expected[left].model, *expected[right].model, condition);
        entry.cost = C_out.calculate_join_cost(G, expected, CE, left, right, condition);
    };

    SECTION("DPsize")
    {
        make_entry(A, C);
        make_entry(A, D);
        make_entry(B, D);
        make_entry(B, A|D);
        make_entry(C, D);
        make_entry(D, A|C);
        make_entry(B, C|D);
        make_entry(A|C, B|D);

        auto &PE = Catalog::Get().plan_enumerator("DPsize");
        PE(G, C_out, plan_table);
        REQUIRE(expected == plan_table);
    }

    SECTION("DPsizeOpt")
    {
        make_entry(A, C);
        make_entry(A, D);
        make_entry(B, D);
        make_entry(B, A|D);
        make_entry(C, D);
        make_entry(D, A|C);
        make_entry(B, C|D);
        make_entry(A|C, B|D);

        auto &PE = Catalog::Get().plan_enumerator("DPsizeOpt");
        PE(G, C_out, plan_table);
        REQUIRE(expected == plan_table);
    }

    SECTION("DPsizeSub")
    {
        make_entry(A, C);
        make_entry(A, D);
        make_entry(B, D);
        make_entry(B, A|D);
        make_entry(C, D);
        make_entry(A|C, D);
        make_entry(B, C|D);
        make_entry(A|C, B|D);

        auto &PE = Catalog::Get().plan_enumerator("DPsizeSub");
        PE(G, C_out, plan_table);
        REQUIRE(expected == plan_table);
    }

    SECTION("DPsub")
    {
        make_entry(A, C);
        make_entry(A, D);
        make_entry(B, D);
        make_entry(B, A|D);
        make_entry(C, D);
        make_entry(A|C, D);
        make_entry(B, C|D);
        make_entry(A|C, B|D);

        auto &PE = Catalog::Get().plan_enumerator("DPsub");
        PE(G, C_out, plan_table);
        REQUIRE(expected == plan_table);
    }

    SECTION("DPsubOpt")
    {
        make_entry(A, C);
        make_entry(A, D);
        make_entry(B, D);
        make_entry(B, A|D);
        make_entry(C, D);
        make_entry(A|C, D);
        make_entry(B, C|D);
        make_entry(A|C, B|D);

        auto &PE = Catalog::Get().plan_enumerator("DPsubOpt");
        PE(G, C_out, plan_table);
        REQUIRE(expected == plan_table);
    }

    SECTION("DPccp")
    {
        make_entry(C, A);
        make_entry(D, A);
        make_entry(D, B);
        make_entry(D, C);
        make_entry(A|D, B);
        make_entry(D, A|C);
        make_entry(C|D, B);
        make_entry(B|D, A|C);

        auto &PE = Catalog::Get().plan_enumerator("DPccp");
        PE(G, C_out, plan_table);
        REQUIRE(expected == plan_table);
    }

    SECTION("TDbasic")
    {
        make_entry(A, C);
        make_entry(A, D);
        make_entry(B, D);
        make_entry(A|D, B);
        make_entry(C, D);
        make_entry(A|C, D);
        make_entry(B, C|D);
        make_entry(A|C, B|D);

        auto &PE = Catalog::Get().plan_enumerator("TDbasic");
        PE(G, C_out, plan_table);
        REQUIRE(expected == plan_table);
    }

    SECTION("TDMinCutAGaT")
    {
        make_entry(A, C);
        make_entry(A, D);
        make_entry(B, D);
        make_entry(A|D, B);
        make_entry(C, D);
        make_entry(A|C, D);
        make_entry(B, C|D);
        make_entry(A|C, B|D);

        auto &PE = Catalog::Get().plan_enumerator("TDMinCutAGaT");
        PE(G, C_out, plan_table);
        REQUIRE(expected == plan_table);
    }
}
