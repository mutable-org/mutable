#include "catch2/catch.hpp"

#include <iostream>
#include <mutable/IR/Optimizer.hpp>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/catalog/Type.hpp>
#include <mutable/catalog/YannakakisHeuristic.hpp>
#include <mutable/IR/PlanTable.hpp>
#include <mutable/mutable.hpp>
#include <mutable/storage/Store.hpp>
#include <mutable/util/ADT.hpp>
#include <parse/Parser.hpp>
#include <parse/Sema.hpp>
#include <mutable/Options.hpp>


using namespace m;
TEST_CASE("Catalog/YannakakisHeuristic", "[catalog]")
{
    /* Get Catalog and create new database to use for unit testing. */
    Catalog::Clear();
    Catalog &Cat = Catalog::Get();
    auto &db = Cat.add_database(Cat.pool("db"));
    Cat.set_database_in_use(db);

    Diagnostic diag(false, std::cout, std::cerr);

    /* Create pooled strings. */
    ThreadSafePooledString str_A = Cat.pool("A");
    ThreadSafePooledString str_B = Cat.pool("B");
    ThreadSafePooledString str_C = Cat.pool("C");
    ThreadSafePooledString str_D = Cat.pool("D");

    ThreadSafePooledString col_id = Cat.pool("id");
    ThreadSafePooledString col_aid = Cat.pool("aid");
    ThreadSafePooledString col_bid = Cat.pool("bid");
    ThreadSafePooledString col_cid = Cat.pool("cid");
    ThreadSafePooledString col_did = Cat.pool("did");

    /* Create tables. */
    Table &tbl_A = db.add_table(str_A);
    Table &tbl_B = db.add_table(str_B);
    Table &tbl_C = db.add_table(str_C);
    Table &tbl_D = db.add_table(str_D);

    /* Add columns to tables. */
    tbl_A.push_back(col_bid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_B.push_back(col_cid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_B.push_back(col_aid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_B.push_back(col_did, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_C.push_back(col_bid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_C.push_back(col_did, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_D.push_back(col_bid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_D.push_back(col_cid, Type::Get_Integer(Type::TY_Vector, 4));

    /* Add data to tables. */
    tbl_A.store(Cat.create_store(tbl_A));
    tbl_B.store(Cat.create_store(tbl_B));
    tbl_C.store(Cat.create_store(tbl_C));
    tbl_D.store(Cat.create_store(tbl_D));

    tbl_A.layout(Cat.data_layout());
    tbl_B.layout(Cat.data_layout());
    tbl_C.layout(Cat.data_layout());
    tbl_D.layout(Cat.data_layout());


    const Subproblem A(1);
    const Subproblem B(2);
    const Subproblem C(4);
    const Subproblem D(8);

    /* Define query:
     *          C
     *        / |
     * A -- B   |
     *        \ |
     *          D
     *          */
    const std::string query = "\
    SELECT * \
    FROM A, B, C, D \
    WHERE A.bid = B.aid  AND B.did = D.bid AND C.did = D.cid  \
                              AND B.cid = C.bid;";

    /* Setup semi-join cardinalities */
    for (std::size_t i = 0; i < 100; ++i) { tbl_A.store().append(); }
    for (std::size_t i = 0; i < 200; ++i) { tbl_B.store().append(); }
    for (std::size_t i = 0; i < 1000; ++i) { tbl_C.store().append(); }
    for (std::size_t i = 0; i < 300; ++i) { tbl_D.store().append(); }

    std::istringstream json_input;
    json_input.str("{ \"db\": [ \
                            {\"relations\": [\"A\"], \"size\":100, \"reductions\": [{ \"right_relations\": [\"B\", \"C\", \"D\"],  \"size\":20}]}, \
                            {\"relations\": [\"B\"], \"size\":200, \"reductions\": [{ \"right_relations\": [\"C\", \"D\"],  \"size\":500},\
                                                                                { \"right_relations\": [\"A\"],  \"size\":800}, \
                                                                                { \"right_relations\": [\"A\", \"C\", \"D\"],  \"size\":400}]}, \
                            {\"relations\": [\"C\"], \"size\":1000, \"reductions\": [{ \"right_relations\": [\"A\", \"B\", \"D\"],  \"size\":800}]}, \
                            {\"relations\": [\"D\"], \"size\":300, \"reductions\": [{ \"right_relations\": [\"A\", \"B\", \"C\"],  \"size\":250}]}, \
                            {\"relations\": [\"B\", \"C\"], \"size\":400, \"reductions\": [{ \"right_relations\": [\"A\"],  \"size\":50},\
                                                                                    { \"right_relations\": [\"D\"],  \"size\":30},\
                                                                                    { \"right_relations\": [\"A\", \"D\"],  \"size\":25}]}, \
                            {\"relations\": [\"B\", \"D\"], \"size\":2000, \"reductions\": [{ \"right_relations\": [\"A\"],  \"size\":3000},\
                                                                                    { \"right_relations\": [\"C\"],  \"size\":2500},\
                                                                                    { \"right_relations\": [\"A\", \"C\"],  \"size\":2000}]}, \
                            {\"relations\": [\"C\", \"D\"], \"size\":3000, \"reductions\": [{ \"right_relations\": [\"A\", \"B\"],  \"size\":4000}]}, \
                            {\"relations\": [\"B\", \"C\", \"D\"], \"size\":5000, \"reductions\": [{ \"right_relations\": [\"A\"],  \"size\":3000}]} \
                            ]}");
    auto stmt = m::statement_from_string(diag, query);
    REQUIRE(not diag.num_errors());
    auto query_graph = QueryGraph::Build(*stmt);
    auto &QG = *query_graph;

    std::unique_ptr<CardinalityEstimator> est = std::make_unique<InjectionCardinalityEstimator>(diag, Cat.pool("db"), json_input);

    db.cardinality_estimator(std::move(est));
    auto folding_problem = Subproblem(14);

    auto &ICE = db.cardinality_estimator();
    Cat.default_plan_enumerator(Cat.pool("DPccp"));
    PlanTableSmallOrDense PT(QG);
    Optimizer_ResultDB_utils::optimize_source_plans(QG, PT);
    SECTION("DecomposeHeuristic") {
        Options::Get().yannakakis_heuristic = m::Options::YH_Decompose;
        Optimizer_ResultDB_utils::optimize_join_order(QG, PT, folding_problem);

        REQUIRE(ICE.predict_cardinality(*PT[folding_problem].model) == 5000);
        REQUIRE(PT[folding_problem].cost == 275);
        auto left_problem = PT[folding_problem].left;
        auto right_problem = PT[folding_problem].right;
        REQUIRE(ICE.predict_cardinality(*PT[left_problem].model) == 300);
        REQUIRE(ICE.predict_cardinality(*PT[right_problem].model) == 400);
    }
    SECTION("SizeHeuristic") {
        Options::Get().yannakakis_heuristic = m::Options::YH_Size;
        Optimizer_ResultDB_utils::optimize_join_order(QG, PT, folding_problem);

        REQUIRE(ICE.predict_cardinality(*PT[folding_problem].model) == 5000);
        REQUIRE(PT[folding_problem].cost == 1375);
        auto left_problem = PT[folding_problem].left;
        auto right_problem = PT[folding_problem].right;
        REQUIRE(ICE.predict_cardinality(*PT[left_problem].model) == 300);
        REQUIRE(ICE.predict_cardinality(*PT[right_problem].model) == 400);
    }
    SECTION("WeakCardinalityHeuristic") {
        Options::Get().yannakakis_heuristic = m::Options::YH_WeakCardinality;
        Optimizer_ResultDB_utils::optimize_join_order(QG, PT, folding_problem);

        REQUIRE(ICE.predict_cardinality(*PT[folding_problem].model) == 5000);
        REQUIRE(PT[folding_problem].cost == 1305);
        auto left_problem = PT[folding_problem].left;
        auto right_problem = PT[folding_problem].right;
        REQUIRE(ICE.predict_cardinality(*PT[left_problem].model) == 300);
        REQUIRE(ICE.predict_cardinality(*PT[right_problem].model) == 400);
    }
}