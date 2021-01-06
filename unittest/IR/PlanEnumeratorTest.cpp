#include "catch.hpp"

#include "mutable/catalog/CostFunction.hpp"
#include "catalog/Schema.hpp"
#include "mutable/catalog/Type.hpp"
#include "mutable/IR/PlanEnumerator.hpp"
#include "mutable/IR/PlanTable.hpp"
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "mutable/storage/Store.hpp"
#include "testutil.hpp"
#include "util/ADT.hpp"

using namespace m;


/*======================================================================================================================
 * Helper funtctions for test setup.
 *====================================================================================================================*/

namespace pe_test {
Stmt * get_Stmt(const char *sql)
{
    LEXER(sql);
    Parser parser(lexer);
    Sema sema(diag);
    auto stmt = parser.parse();
    sema(*stmt);
    if (diag.num_errors() != 0) {
        std::cout << out.str() << std::endl;
        std::cerr << err.str() << std::endl;
        delete stmt;
        REQUIRE(false); // abort test
    }
    return stmt;
}

void init_PT_base_case(const QueryGraph &G, PlanTable &PT)
{
    using Subproblem = SmallBitset;
    for (auto ds : G.sources()) {
        Subproblem s(1UL << ds->id());
        auto bt = cast<const BaseTable>(ds);
        auto &store = bt->table().store();
        PT[s].cost = 0;
        PT[s].size = store.num_rows();
    }
}
}


/*======================================================================================================================
 * Test Cost Function.
 *====================================================================================================================*/
TEST_CASE("PlanEnumerator", "[core][IR][planenumerator]")
{
    using Subproblem = SmallBitset;
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
    tbl_A.store(Store::CreateRowStore(tbl_A));
    tbl_B.store(Store::CreateRowStore(tbl_B));
    tbl_C.store(Store::CreateRowStore(tbl_C));
    tbl_D.store(Store::CreateRowStore(tbl_D));
    for (std::size_t i = 0; i < num_rows_A; ++i) { tbl_A.store().append(); }
    for (std::size_t i = 0; i < num_rows_B; ++i) { tbl_B.store().append(); }
    for (std::size_t i = 0; i < num_rows_C; ++i) { tbl_C.store().append(); }
    for (std::size_t i = 0; i < num_rows_D; ++i) { tbl_D.store().append(); }

    /* Create adjacency matrix. */
    const char *query = "SELECT * \
                         FROM A, B, C, D \
                         WHERE A.id = C.aid AND A.id = D.aid AND B.id = D.bid AND C.id = D.cid;";
    auto stmt = as<const SelectStmt>(pe_test::get_Stmt(query));
    auto query_graph = QueryGraph::Build(*stmt);
    auto &G = *query_graph.get();
    auto num_sources = query_graph->sources().size();

    CostFunction cf([](CostFunction::Subproblem left, CostFunction::Subproblem right, int, const PlanTable &T) {
        return sum_wo_overflow(T[left].cost, T[right].cost, T[left].size, T[right].size);
    });

    constexpr uint64_t MAX = std::numeric_limits<uint64_t>::max();

    SECTION("DPsize")
    {
        /* Initialize `PlanTable` for `DPsize`. */
        PlanTable expected_plan_table(num_sources);
        expected_plan_table.at(Subproblem(1))  = { Subproblem(0), Subproblem(0),     5,   0 };
        expected_plan_table.at(Subproblem(2))  = { Subproblem(0), Subproblem(0),    10,   0 };
        expected_plan_table.at(Subproblem(3))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(4))  = { Subproblem(0), Subproblem(0),     8,   0 };
        expected_plan_table.at(Subproblem(5))  = { Subproblem(1), Subproblem(4),    40,  13 };
        expected_plan_table.at(Subproblem(6))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(7))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(8))  = { Subproblem(0), Subproblem(0),    12,   0 };
        expected_plan_table.at(Subproblem(9))  = { Subproblem(1), Subproblem(8),    60,  17 };
        expected_plan_table.at(Subproblem(10)) = { Subproblem(2), Subproblem(8),   120,  22 };
        expected_plan_table.at(Subproblem(11)) = { Subproblem(2), Subproblem(9),   600,  87 };
        expected_plan_table.at(Subproblem(12)) = { Subproblem(4), Subproblem(8),    96,  20 };
        expected_plan_table.at(Subproblem(13)) = { Subproblem(8), Subproblem(5),   480,  65 };
        expected_plan_table.at(Subproblem(14)) = { Subproblem(2), Subproblem(12),  960, 126 };
        expected_plan_table.at(Subproblem(15)) = { Subproblem(5), Subproblem(10), 4800, 195 };

        auto dp_size = PlanEnumerator::CreateDPsize();
        PlanTable plan_table(num_sources);
        /* Initialize `PlanTable` for base case. */
        pe_test::init_PT_base_case(G, plan_table);

        (*dp_size)(G, cf, plan_table);
        REQUIRE(expected_plan_table == plan_table);
    }

    SECTION("DPsizeOpt")
    {
        /* Initialize `PlanTable` for `DPsize`. */
        PlanTable expected_plan_table(num_sources);
        expected_plan_table.at(Subproblem(1))  = { Subproblem(0), Subproblem(0),     5,   0 };
        expected_plan_table.at(Subproblem(2))  = { Subproblem(0), Subproblem(0),    10,   0 };
        expected_plan_table.at(Subproblem(3))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(4))  = { Subproblem(0), Subproblem(0),     8,   0 };
        expected_plan_table.at(Subproblem(5))  = { Subproblem(1), Subproblem(4),    40,  13 };
        expected_plan_table.at(Subproblem(6))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(7))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(8))  = { Subproblem(0), Subproblem(0),    12,   0 };
        expected_plan_table.at(Subproblem(9))  = { Subproblem(1), Subproblem(8),    60,  17 };
        expected_plan_table.at(Subproblem(10)) = { Subproblem(2), Subproblem(8),   120,  22 };
        expected_plan_table.at(Subproblem(11)) = { Subproblem(2), Subproblem(9),   600,  87 };
        expected_plan_table.at(Subproblem(12)) = { Subproblem(4), Subproblem(8),    96,  20 };
        expected_plan_table.at(Subproblem(13)) = { Subproblem(8), Subproblem(5),   480,  65 };
        expected_plan_table.at(Subproblem(14)) = { Subproblem(2), Subproblem(12),  960, 126 };
        expected_plan_table.at(Subproblem(15)) = { Subproblem(5), Subproblem(10), 4800, 195 };

        auto dp_size_opt = PlanEnumerator::CreateDPsizeOpt();
        PlanTable plan_table(num_sources);
        /* Initialize `PlanTable` for base case. */
        pe_test::init_PT_base_case(G, plan_table);

        (*dp_size_opt)(G, cf, plan_table);
        REQUIRE(expected_plan_table == plan_table);
    }

    SECTION("DPsizeSub")
    {
        /* Initialize `PlanTable` for `DPsizeSub`. */
        PlanTable expected_plan_table(num_sources);
        expected_plan_table.at(Subproblem(1))  = { Subproblem(0), Subproblem(0),     5,   0 };
        expected_plan_table.at(Subproblem(2))  = { Subproblem(0), Subproblem(0),    10,   0 };
        expected_plan_table.at(Subproblem(3))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(4))  = { Subproblem(0), Subproblem(0),     8,   0 };
        expected_plan_table.at(Subproblem(5))  = { Subproblem(1), Subproblem(4),    40,  13 };
        expected_plan_table.at(Subproblem(6))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(7))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(8))  = { Subproblem(0), Subproblem(0),    12,   0 };
        expected_plan_table.at(Subproblem(9))  = { Subproblem(1), Subproblem(8),    60,  17 };
        expected_plan_table.at(Subproblem(10)) = { Subproblem(2), Subproblem(8),   120,  22 };
        expected_plan_table.at(Subproblem(11)) = { Subproblem(2), Subproblem(9),   600,  87 };
        expected_plan_table.at(Subproblem(12)) = { Subproblem(4), Subproblem(8),    96,  20 };
        expected_plan_table.at(Subproblem(13)) = { Subproblem(5), Subproblem(8),   480,  65 };
        expected_plan_table.at(Subproblem(14)) = { Subproblem(2), Subproblem(12),  960, 126 };
        expected_plan_table.at(Subproblem(15)) = { Subproblem(5), Subproblem(10), 4800, 195 };

        auto dp_size_sub = PlanEnumerator::CreateDPsizeSub();
        PlanTable plan_table(num_sources);
        /* Initialize `PlanTable` for base case. */
        pe_test::init_PT_base_case(G, plan_table);

        (*dp_size_sub)(G, cf, plan_table);
        REQUIRE(expected_plan_table == plan_table);
    }

    SECTION("DPsub")
    {
        /* Initialize `PlanTable` for `DPsub`. */
        PlanTable expected_plan_table(num_sources);
        expected_plan_table.at(Subproblem(1))  = { Subproblem(0), Subproblem(0),     5,   0 };
        expected_plan_table.at(Subproblem(2))  = { Subproblem(0), Subproblem(0),    10,   0 };
        expected_plan_table.at(Subproblem(3))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(4))  = { Subproblem(0), Subproblem(0),     8,   0 };
        expected_plan_table.at(Subproblem(5))  = { Subproblem(1), Subproblem(4),    40,  13 };
        expected_plan_table.at(Subproblem(6))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(7))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(8))  = { Subproblem(0), Subproblem(0),    12,   0 };
        expected_plan_table.at(Subproblem(9))  = { Subproblem(1), Subproblem(8),    60,  17 };
        expected_plan_table.at(Subproblem(10)) = { Subproblem(2), Subproblem(8),   120,  22 };
        expected_plan_table.at(Subproblem(11)) = { Subproblem(2), Subproblem(9),   600,  87 };
        expected_plan_table.at(Subproblem(12)) = { Subproblem(4), Subproblem(8),    96,  20 };
        expected_plan_table.at(Subproblem(13)) = { Subproblem(5), Subproblem(8),   480,  65 };
        expected_plan_table.at(Subproblem(14)) = { Subproblem(2), Subproblem(12),  960, 126 };
        expected_plan_table.at(Subproblem(15)) = { Subproblem(5), Subproblem(10), 4800, 195 };

        auto dp_sub = PlanEnumerator::CreateDPsub();
        PlanTable plan_table(num_sources);
        /* Initialize `PlanTable` for base case. */
        pe_test::init_PT_base_case(G, plan_table);

        (*dp_sub)(G, cf, plan_table);
        REQUIRE(expected_plan_table == plan_table);
    }

    SECTION("DPsubOpt")
    {
        /* Initialize `PlanTable` for `DPsub`. */
        PlanTable expected_plan_table(num_sources);
        expected_plan_table.at(Subproblem(1))  = { Subproblem(0), Subproblem(0),     5,   0 };
        expected_plan_table.at(Subproblem(2))  = { Subproblem(0), Subproblem(0),    10,   0 };
        expected_plan_table.at(Subproblem(3))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(4))  = { Subproblem(0), Subproblem(0),     8,   0 };
        expected_plan_table.at(Subproblem(5))  = { Subproblem(1), Subproblem(4),    40,  13 };
        expected_plan_table.at(Subproblem(6))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(7))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(8))  = { Subproblem(0), Subproblem(0),    12,   0 };
        expected_plan_table.at(Subproblem(9))  = { Subproblem(1), Subproblem(8),    60,  17 };
        expected_plan_table.at(Subproblem(10)) = { Subproblem(2), Subproblem(8),   120,  22 };
        expected_plan_table.at(Subproblem(11)) = { Subproblem(2), Subproblem(9),   600,  87 };
        expected_plan_table.at(Subproblem(12)) = { Subproblem(4), Subproblem(8),    96,  20 };
        expected_plan_table.at(Subproblem(13)) = { Subproblem(5), Subproblem(8),   480,  65 };
        expected_plan_table.at(Subproblem(14)) = { Subproblem(2), Subproblem(12),  960, 126 };
        expected_plan_table.at(Subproblem(15)) = { Subproblem(5), Subproblem(10), 4800, 195 };

        auto dp_sub_opt = PlanEnumerator::CreateDPsubOpt();
        PlanTable plan_table(num_sources);
        /* Initialize `PlanTable` for base case. */
        pe_test::init_PT_base_case(G, plan_table);

        (*dp_sub_opt)(G, cf, plan_table);
        REQUIRE(expected_plan_table == plan_table);
    }

    SECTION("DPccp")
    {
        /* Initialize `PlanTable` for `DPccp`. */
        PlanTable expected_plan_table(num_sources);
        expected_plan_table.at(Subproblem(1))  = { Subproblem(0), Subproblem(0),     5,   0 };
        expected_plan_table.at(Subproblem(2))  = { Subproblem(0), Subproblem(0),    10,   0 };
        expected_plan_table.at(Subproblem(3))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(4))  = { Subproblem(0), Subproblem(0),     8,   0 };
        expected_plan_table.at(Subproblem(5))  = { Subproblem(1), Subproblem(4),    40,  13 };
        expected_plan_table.at(Subproblem(6))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(7))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(8))  = { Subproblem(0), Subproblem(0),    12,   0 };
        expected_plan_table.at(Subproblem(9))  = { Subproblem(1), Subproblem(8),    60,  17 };
        expected_plan_table.at(Subproblem(10)) = { Subproblem(2), Subproblem(8),   120,  22 };
        expected_plan_table.at(Subproblem(11)) = { Subproblem(9), Subproblem(2),   600,  87 };
        expected_plan_table.at(Subproblem(12)) = { Subproblem(4), Subproblem(8),    96,  20 };
        expected_plan_table.at(Subproblem(13)) = { Subproblem(5), Subproblem(8),   480,  65 };
        expected_plan_table.at(Subproblem(14)) = { Subproblem(2), Subproblem(12),  960, 126 };
        expected_plan_table.at(Subproblem(15)) = { Subproblem(5), Subproblem(10), 4800, 195 };

        auto dp_ccp = PlanEnumerator::CreateDPccp();
        PlanTable plan_table(num_sources);
        /* Initialize `PlanTable` for base case. */
        pe_test::init_PT_base_case(G, plan_table);

        (*dp_ccp)(G, cf, plan_table);
        REQUIRE(expected_plan_table == plan_table);
    }

    SECTION("TDbasic")
    {
        /* Initialize `PlanTable` for `TDbasic`. */
        PlanTable expected_plan_table(num_sources);
        expected_plan_table.at(Subproblem(1))  = { Subproblem(0), Subproblem(0),     5,   0 };
        expected_plan_table.at(Subproblem(2))  = { Subproblem(0), Subproblem(0),    10,   0 };
        expected_plan_table.at(Subproblem(3))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(4))  = { Subproblem(0), Subproblem(0),     8,   0 };
        expected_plan_table.at(Subproblem(5))  = { Subproblem(1), Subproblem(4),    40,  13 };
        expected_plan_table.at(Subproblem(6))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(7))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(8))  = { Subproblem(0), Subproblem(0),    12,   0 };
        expected_plan_table.at(Subproblem(9))  = { Subproblem(1), Subproblem(8),    60,  17 };
        expected_plan_table.at(Subproblem(10)) = { Subproblem(2), Subproblem(8),   120,  22 };
        expected_plan_table.at(Subproblem(11)) = { Subproblem(9), Subproblem(2),   600,  87 };
        expected_plan_table.at(Subproblem(12)) = { Subproblem(4), Subproblem(8),    96,  20 };
        expected_plan_table.at(Subproblem(13)) = { Subproblem(5), Subproblem(8),   480,  65 };
        expected_plan_table.at(Subproblem(14)) = { Subproblem(2), Subproblem(12),  960, 126 };
        expected_plan_table.at(Subproblem(15)) = { Subproblem(5), Subproblem(10), 4800, 195 };

        auto td_basic = PlanEnumerator::CreateTDbasic();
        PlanTable plan_table(num_sources);
        /* Initialize `PlanTable` for base case. */
        pe_test::init_PT_base_case(G, plan_table);

        (*td_basic)(G, cf, plan_table);
        REQUIRE(expected_plan_table == plan_table);
    }

    SECTION("TDMinCutAGaT")
    {
        /* Initialize `PlanTable` for `TDMinCutAGaT`. */
        PlanTable expected_plan_table(num_sources);
        expected_plan_table.at(Subproblem(1))  = { Subproblem(0), Subproblem(0),     5,   0 };
        expected_plan_table.at(Subproblem(2))  = { Subproblem(0), Subproblem(0),    10,   0 };
        expected_plan_table.at(Subproblem(3))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(4))  = { Subproblem(0), Subproblem(0),     8,   0 };
        expected_plan_table.at(Subproblem(5))  = { Subproblem(1), Subproblem(4),    40,  13 };
        expected_plan_table.at(Subproblem(6))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(7))  = { Subproblem(0), Subproblem(0),     0, MAX };
        expected_plan_table.at(Subproblem(8))  = { Subproblem(0), Subproblem(0),    12,   0 };
        expected_plan_table.at(Subproblem(9))  = { Subproblem(1), Subproblem(8),    60,  17 };
        expected_plan_table.at(Subproblem(10)) = { Subproblem(2), Subproblem(8),   120,  22 };
        expected_plan_table.at(Subproblem(11)) = { Subproblem(9), Subproblem(2),   600,  87 };
        expected_plan_table.at(Subproblem(12)) = { Subproblem(4), Subproblem(8),    96,  20 };
        expected_plan_table.at(Subproblem(13)) = { Subproblem(5), Subproblem(8),   480,  65 };
        expected_plan_table.at(Subproblem(14)) = { Subproblem(2), Subproblem(12),  960, 126 };
        expected_plan_table.at(Subproblem(15)) = { Subproblem(5), Subproblem(10), 4800, 195 };

        auto td_mincut_agat = PlanEnumerator::CreateTDMinCutAGaT();
        PlanTable plan_table(num_sources);
        /* Initialize `PlanTable` for base case. */
        pe_test::init_PT_base_case(G, plan_table);

        (*td_mincut_agat)(G, cf, plan_table);
        REQUIRE(expected_plan_table == plan_table);
    }
    delete stmt;
}
