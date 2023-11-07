#include "catch2/catch.hpp"

#include <mutable/catalog/CardinalityEstimator.hpp>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/catalog/CostFunctionCout.hpp>
#include <mutable/IR/HeuristicSearchPlanEnumerator.hpp>
#include <mutable/IR/PlanTable.hpp>
#include <mutable/mutable.hpp>
#include <mutable/util/HeuristicSearch.hpp>


using namespace m;
using namespace m::pe;
using namespace m::pe::hs;


/*======================================================================================================================
* Helper functions for test setup.
*====================================================================================================================*/

template<typename PlanTable>
void init_PT_base_case(const QueryGraph &G, PlanTable &PT, const CardinalityEstimator &CE)
{
    //auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();
    using Subproblem = SmallBitset;
    for (auto &ds : G.sources()) {
        Subproblem s = Subproblem::Singleton(ds->id());
        auto bt = as<const BaseTable>(*ds);
        PT[s].cost = 0;
        PT[s].model = CE.estimate_scan(G, s);
    }
}

/* Dependencies between the search state counters, that hold true for all search configurations without cost-based
 * pruning during the search. */
template<typename State, typename SearchAlgorithm, typename StateManager>
void check_state_counter_dependencies(SearchAlgorithm &S, StateManager &SM, uint64_t budget)
{
    CHECK(SM.num_new()+SM.num_duplicates() == State::NUM_STATES_GENERATED()+1);
    CHECK(SM.num_duplicates() == SM.num_discarded()+SM.num_cheaper());
    CHECK(SM.num_decrease_key() <= SM.num_cheaper());
    CHECK(S.num_cached_heuristic_value() == SM.num_duplicates());
    CHECK(State::NUM_STATES_EXPANDED() <= budget);
    CHECK(State::NUM_STATES_CONSTRUCTED() == State::NUM_STATES_GENERATED()+1);
    CHECK(State::NUM_STATES_CONSTRUCTED() == SM.num_new()+SM.num_duplicates());
    CHECK(State::NUM_STATES_DISPOSED() == SM.num_duplicates()+SM.num_pruned_by_cost());
}


/*======================================================================================================================
 * Test Heuristic Search strategies.
 *====================================================================================================================*/

/*======================================================================================================================
 * AStar
 *====================================================================================================================*/

/** Overview of the Test Cases:
 * - AStar_Chain_Bottomup_GOO
 * - AStar_Star_TopDown_zero
 * - Astar_Clique_TopDown_sum
 */

TEST_CASE("AStar_Chain_Bottomup_GOO", "[core][IR]")
{
    using Subproblem = SmallBitset;
    using PlanTable = PlanTableSmallOrDense;

    /* Get Catalog and create new database to use for unit testing. */
    Catalog::Clear();
    Catalog &Cat = Catalog::Get();
    auto &db = Cat.add_database("db");
    Cat.set_database_in_use(db);

    /* Create pooled strings. */
    const char *str_R0 = Cat.pool("R0");
    const char *str_R1 = Cat.pool("R1");
    const char *str_R2 = Cat.pool("R2");
    const char *str_R3 = Cat.pool("R3");

    const char *col_id = Cat.pool("id");
    const char *col_fid_R1 = Cat.pool("fid_R1");
    const char *col_fid_R2 = Cat.pool("fid_R2");
    const char *col_fid_R3 = Cat.pool("fid_R3");

    /* Create tables. */
    Table &tbl_R0 = db.add_table(str_R0);
    Table &tbl_R1 = db.add_table(str_R1);
    Table &tbl_R2 = db.add_table(str_R2);
    Table &tbl_R3 = db.add_table(str_R3);

    /* Add columns to tables. */
    tbl_R0.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R3.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R1.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R2.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));

    tbl_R0.push_back(col_fid_R1, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R1.push_back(col_fid_R2, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R2.push_back(col_fid_R3, Type::Get_Integer(Type::TY_Vector, 4));

    /* Add data to tables. */
    std::size_t num_rows_R0 = 1500;
    std::size_t num_rows_R1 = 2000;
    std::size_t num_rows_R2 = 1000;
    std::size_t num_rows_R3 = 500;
    tbl_R0.store(Cat.create_store(tbl_R0));
    tbl_R1.store(Cat.create_store(tbl_R1));
    tbl_R2.store(Cat.create_store(tbl_R2));
    tbl_R3.store(Cat.create_store(tbl_R3));
    tbl_R0.layout(Cat.data_layout());
    tbl_R1.layout(Cat.data_layout());
    tbl_R2.layout(Cat.data_layout());
    tbl_R3.layout(Cat.data_layout());

    for (std::size_t i = 0; i < num_rows_R0; ++i) { tbl_R0.store().append(); }
    for (std::size_t i = 0; i < num_rows_R1; ++i) { tbl_R1.store().append(); }
    for (std::size_t i = 0; i < num_rows_R2; ++i) { tbl_R2.store().append(); }
    for (std::size_t i = 0; i < num_rows_R3; ++i) { tbl_R3.store().append(); }

    /* Define query: chain query
     *
     * R0 - R1 - R2 - R3
     *
     */

    const std::string query = " SELECT * \
                                FROM R0, R1, R2, R3 \
                                WHERE R0.fid_R1 = R1.id \
                                  AND R1.fid_R2 = R2.id \
                                  AND R2.fid_R3 = R3.id;";

    Diagnostic diag(false, std::cout, std::cerr); // What is diagnostic?
    auto stmt = m::statement_from_string(diag, query); // TODO: use command_from_string
    REQUIRE(not diag.num_errors());
    auto query_graph = QueryGraph::Build(*stmt);
    auto &G = *query_graph.get();

    /* Create subproblems to manually add entries in the `PlanTable`. */
    const Subproblem R0(1);
    const Subproblem R1(2);
    const Subproblem R2(4);
    const Subproblem R3(8);

    /* Initialize the `InjectionCardinalityEstimator`. */
    std::istringstream json_input;
    json_input.str("{ \"mine\": [ \
                   {\"relations\": [\"R0\"], \"size\":1500}, \
                   {\"relations\": [\"R1\"], \"size\":2000}, \
                   {\"relations\": [\"R2\"], \"size\":1000}, \
                   {\"relations\": [\"R3\"], \"size\":500}, \
                   {\"relations\": [\"R0\", \"R1\"], \"size\":40}, \
                   {\"relations\": [\"R1\", \"R2\"], \"size\":150}, \
                   {\"relations\": [\"R2\", \"R3\"], \"size\":3200}, \
                   {\"relations\": [\"R0\", \"R1\", \"R2\"], \"size\":50000}, \
                   {\"relations\": [\"R1\", \"R2\", \"R3\"], \"size\":700}, \
                   {\"relations\": [\"R0\", \"R1\", \"R2\", \"R3\"], \"size\":15000} \
                   ]}");

    auto ICE = std::make_unique<InjectionCardinalityEstimator>(diag, "mine", json_input);
    db.cardinality_estimator(std::move(ICE));

    /* Initialize `PlanTable` for base case. */
    PlanTable plan_table(G);
    init_PT_base_case(G, plan_table, db.cardinality_estimator());

    PlanTable expected(G);
    init_PT_base_case(G, expected, db.cardinality_estimator());

    /* Get `SmallBitset` respresentation of the goal state for testing the costs. */
    const Subproblem All = Subproblem::All(G.num_sources());

    /* Get costfunction Cout, adjacency matrix and join condition. */
    CostFunctionCout C_out;
    const AdjacencyMatrix &M = G.adjacency_matrix();
    static cnf::CNF condition; // TODO use join condition

    /* Heuristic search configurations. */
    using State = search_states::SubproblemsArray;


    SECTION("BottomUp_GOO")
    {
        /* Run heuristic search. */
        using H = heuristics::GOO<PlanTable, State, expansions::BottomUpComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::BottomUpComplete, H, config::AStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());


        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::BottomUpComplete,
                                              SearchAlgorithm,
                                              heuristics::GOO,
                                              config::AStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .weighting_factor = 1.f
                                                });

        /* Caution: In Case of the Injected Cardinality Estimator the join operation is commutative. Therefore the given
         * order of the left and the right subproblem is saved. As subproblems in heuristic search are ordered
         * lexicographically increasing we should keep this order here to check for equality. (Other option: change
         * equality operator for PlanTable entries with the same costs such that they are also equal if the joined
         * subproblems are only swapped.) */

        /* Fill `expected` with the anticipated plan. */
        expected.update(G, db.cardinality_estimator(), C_out, R1, R2, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1|R2, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0, R1|R2|R3, condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15850);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 850);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM,
                                                                               std::numeric_limits<uint64_t>::max());

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 6);
        CHECK(State::NUM_STATES_EXPANDED() == 3);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 7);
        CHECK(State::NUM_STATES_DISPOSED() == 0);

        CHECK(SM.num_new() == 7);
        CHECK(SM.num_duplicates() == 0);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 7);
        CHECK(SM.num_states_in_regular_queue() == 3);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }

}

TEST_CASE("AStar_Star_TopDown_zero", "[core][IR]")
{
    using Subproblem = SmallBitset;
    using PlanTable = PlanTableSmallOrDense;

    /* Get Catalog and create new database to use for unit testing. */
    Catalog::Clear();
    Catalog &Cat = Catalog::Get();
    auto &db = Cat.add_database("db");
    Cat.set_database_in_use(db);

    /* Create pooled strings. */
    const char *str_R0 = Cat.pool("R0");
    const char *str_R1 = Cat.pool("R1");
    const char *str_R2 = Cat.pool("R2");
    const char *str_R3 = Cat.pool("R3");

    const char *col_id = Cat.pool("id");
    const char *col_fid_R1 = Cat.pool("fid_R1");
    const char *col_fid_R2 = Cat.pool("fid_R2");
    const char *col_fid_R3 = Cat.pool("fid_R3");

    /* Create tables. */
    Table &tbl_R0 = db.add_table(str_R0);
    Table &tbl_R1 = db.add_table(str_R1);
    Table &tbl_R2 = db.add_table(str_R2);
    Table &tbl_R3 = db.add_table(str_R3);

    /* Add columns to tables. */
    tbl_R0.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R3.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R1.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R2.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));

    tbl_R0.push_back(col_fid_R1, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R0.push_back(col_fid_R2, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R0.push_back(col_fid_R3, Type::Get_Integer(Type::TY_Vector, 4));

    /* Add data to tables. */
    std::size_t num_rows_R0 = 1500;
    std::size_t num_rows_R1 = 2000;
    std::size_t num_rows_R2 = 1000;
    std::size_t num_rows_R3 = 500;
    tbl_R0.store(Cat.create_store(tbl_R0));
    tbl_R1.store(Cat.create_store(tbl_R1));
    tbl_R2.store(Cat.create_store(tbl_R2));
    tbl_R3.store(Cat.create_store(tbl_R3));
    tbl_R0.layout(Cat.data_layout());
    tbl_R1.layout(Cat.data_layout());
    tbl_R2.layout(Cat.data_layout());
    tbl_R3.layout(Cat.data_layout());

    for (std::size_t i = 0; i < num_rows_R0; ++i) { tbl_R0.store().append(); }
    for (std::size_t i = 0; i < num_rows_R1; ++i) { tbl_R1.store().append(); }
    for (std::size_t i = 0; i < num_rows_R2; ++i) { tbl_R2.store().append(); }
    for (std::size_t i = 0; i < num_rows_R3; ++i) { tbl_R3.store().append(); }

    /* Define query: chain query
     *
     *     R2
     *     |
     *  R3-R0-R1
     *
     */

    const std::string query = " SELECT * \
                                FROM R0, R1, R2, R3 \
                                WHERE R0.fid_R1 = R1.id \
                                  AND R0.fid_R2 = R2.id \
                                  AND R0.fid_R3 = R3.id;";

    Diagnostic diag(false, std::cout, std::cerr); // What is diagnostic?
    auto stmt = m::statement_from_string(diag, query); // TODO: use command_from_string
    REQUIRE(not diag.num_errors());
    auto query_graph = QueryGraph::Build(*stmt);
    auto &G = *query_graph.get();

    /* Create subproblems to manually add entries in the `PlanTable`. */
    const Subproblem R0(1);
    const Subproblem R1(2);
    const Subproblem R2(4);
    const Subproblem R3(8);

    /* Initialize the `InjectionCardinalityEstimator`. */
    std::istringstream json_input;
    json_input.str("{ \"mine\": [ \
                   {\"relations\": [\"R0\"], \"size\":1500}, \
                   {\"relations\": [\"R1\"], \"size\":2000}, \
                   {\"relations\": [\"R2\"], \"size\":1000}, \
                   {\"relations\": [\"R3\"], \"size\":500}, \
                   {\"relations\": [\"R0\", \"R1\"], \"size\":40}, \
                   {\"relations\": [\"R0\", \"R2\"], \"size\":70}, \
                   {\"relations\": [\"R0\", \"R3\"], \"size\":100}, \
                   {\"relations\": [\"R0\", \"R1\", \"R2\"], \"size\":50000}, \
                   {\"relations\": [\"R0\", \"R1\", \"R3\"], \"size\":100}, \
                   {\"relations\": [\"R0\", \"R2\", \"R3\"], \"size\":1800}, \
                   {\"relations\": [\"R0\", \"R1\", \"R2\", \"R3\"], \"size\":4000} \
                   ]}");

    auto ICE = std::make_unique<InjectionCardinalityEstimator>(diag, "mine", json_input);
    db.cardinality_estimator(std::move(ICE));

    /* Initialize `PlanTable` for base case. */
    PlanTable plan_table(G);
    init_PT_base_case(G, plan_table, db.cardinality_estimator());

    PlanTable expected(G);
    init_PT_base_case(G, expected, db.cardinality_estimator());

    /* Get `SmallBitset` respresentation of the goal state for testing the costs. */
    const Subproblem All = Subproblem::All(G.num_sources());

    /* Get costfunction Cout, adjacency matrix and join condition. */
    CostFunctionCout C_out;
    const AdjacencyMatrix &M = G.adjacency_matrix();
    static cnf::CNF condition; // TODO use join condition

    /* Heuristic search configurations. */
    using State = search_states::SubproblemsArray;


    SECTION("TopDown_zero")
    {
        /* Run heuristic search. */
        using H = heuristics::zero<PlanTable, State, expansions::TopDownComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::TopDownComplete, H, config::AStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());


        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::TopDownComplete,
                                              SearchAlgorithm,
                                              heuristics::zero,
                                              config::AStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .weighting_factor = 1.f
                                                });

        /* Fill `expected` with the anticipated plan. */
        expected.update(G, db.cardinality_estimator(), C_out, R0, R1, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0|R1, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R2, R0|R1|R3, condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 4140);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 140);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM,
                                                                               std::numeric_limits<uint64_t>::max());


        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 11);
        CHECK(State::NUM_STATES_EXPANDED() == 6);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 12);
        CHECK(State::NUM_STATES_DISPOSED() == 4);

        CHECK(SM.num_new() == 8);
        CHECK(SM.num_duplicates() == 4);
        /* Numbers of Counters discarded, cheaper and decrease_key depend on the expansion order of states with the
         * same f-value, therefore they are not tested here. */
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 8);
        CHECK(SM.num_states_in_regular_queue() == 1);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 4);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }
}


TEST_CASE("AStar_Clique_TopDown_sum", "[core][IR]")
{
    using Subproblem = SmallBitset;
    using PlanTable = PlanTableSmallOrDense;

    /* Get Catalog and create new database to use for unit testing. */
    Catalog::Clear();
    Catalog &Cat = Catalog::Get();
    auto &db = Cat.add_database("db");
    Cat.set_database_in_use(db);

    /* Create pooled strings. */
    const char *str_R0 = Cat.pool("R0");
    const char *str_R1 = Cat.pool("R1");
    const char *str_R2 = Cat.pool("R2");
    const char *str_R3 = Cat.pool("R3");

    const char *col_id = Cat.pool("id");
    const char *col_fid_R1 = Cat.pool("fid_R1");
    const char *col_fid_R2 = Cat.pool("fid_R2");
    const char *col_fid_R3 = Cat.pool("fid_R3");

    /* Create tables. */
    Table &tbl_R0 = db.add_table(str_R0);
    Table &tbl_R1 = db.add_table(str_R1);
    Table &tbl_R2 = db.add_table(str_R2);
    Table &tbl_R3 = db.add_table(str_R3);

    /* Add columns to tables. */
    tbl_R0.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R3.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R1.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R2.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));

    tbl_R0.push_back(col_fid_R1, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R0.push_back(col_fid_R2, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R0.push_back(col_fid_R3, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R1.push_back(col_fid_R2, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R1.push_back(col_fid_R3, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R2.push_back(col_fid_R3, Type::Get_Integer(Type::TY_Vector, 4));

    /* Add data to tables. */
    std::size_t num_rows_R0 = 1500;
    std::size_t num_rows_R1 = 2000;
    std::size_t num_rows_R2 = 1000;
    std::size_t num_rows_R3 = 500;
    tbl_R0.store(Cat.create_store(tbl_R0));
    tbl_R1.store(Cat.create_store(tbl_R1));
    tbl_R2.store(Cat.create_store(tbl_R2));
    tbl_R3.store(Cat.create_store(tbl_R3));
    tbl_R0.layout(Cat.data_layout());
    tbl_R1.layout(Cat.data_layout());
    tbl_R2.layout(Cat.data_layout());
    tbl_R3.layout(Cat.data_layout());

    for (std::size_t i = 0; i < num_rows_R0; ++i) { tbl_R0.store().append(); }
    for (std::size_t i = 0; i < num_rows_R1; ++i) { tbl_R1.store().append(); }
    for (std::size_t i = 0; i < num_rows_R2; ++i) { tbl_R2.store().append(); }
    for (std::size_t i = 0; i < num_rows_R3; ++i) { tbl_R3.store().append(); }

    /* Define query: chain query
     *
     *     R0
     *   / | \
     *  R3-|-R1
     *   \ | /
     *     R2
     *
     */

    const std::string query = " SELECT * \
                                FROM R0, R1, R2, R3 \
                                WHERE R0.fid_R1 = R1.id \
                                  AND R0.fid_R2 = R2.id \
                                  AND R0.fid_R3 = R3.id \
                                  AND R1.fid_R2 = R2.id \
                                  AND R1.fid_R3 = R3.id \
                                  AND R2.fid_R3 = R3.id;";

    Diagnostic diag(false, std::cout, std::cerr); // What is diagnostic?
    auto stmt = m::statement_from_string(diag, query); // TODO: use command_from_string
    REQUIRE(not diag.num_errors());
    auto query_graph = QueryGraph::Build(*stmt);
    auto &G = *query_graph.get();

    /* Create subproblems to manually add entries in the `PlanTable`. */
    const Subproblem R0(1);
    const Subproblem R1(2);
    const Subproblem R2(4);
    const Subproblem R3(8);

    /* Initialize the `InjectionCardinalityEstimator`. */
    std::istringstream json_input;
    json_input.str("{ \"mine\": [ \
                   {\"relations\": [\"R0\"], \"size\":1500}, \
                   {\"relations\": [\"R1\"], \"size\":2000}, \
                   {\"relations\": [\"R2\"], \"size\":1000}, \
                   {\"relations\": [\"R3\"], \"size\":500}, \
                   {\"relations\": [\"R0\", \"R1\"], \"size\":40}, \
                   {\"relations\": [\"R0\", \"R2\"], \"size\":70}, \
                   {\"relations\": [\"R0\", \"R3\"], \"size\":100}, \
                   {\"relations\": [\"R1\", \"R2\"], \"size\":150}, \
                   {\"relations\": [\"R1\", \"R3\"], \"size\":60}, \
                   {\"relations\": [\"R2\", \"R3\"], \"size\":3200}, \
                   {\"relations\": [\"R0\", \"R1\", \"R2\"], \"size\":50000}, \
                   {\"relations\": [\"R0\", \"R1\", \"R3\"], \"size\":100}, \
                   {\"relations\": [\"R0\", \"R2\", \"R3\"], \"size\":1800}, \
                   {\"relations\": [\"R1\", \"R2\", \"R3\"], \"size\":700}, \
                   {\"relations\": [\"R0\", \"R1\", \"R2\", \"R3\"], \"size\":4000} \
                   ]}");

    auto ICE = std::make_unique<InjectionCardinalityEstimator>(diag, "mine", json_input);
    db.cardinality_estimator(std::move(ICE));

    /* Initialize `PlanTable` for base case. */
    PlanTable plan_table(G);
    init_PT_base_case(G, plan_table, db.cardinality_estimator());

    PlanTable expected(G);
    init_PT_base_case(G, expected, db.cardinality_estimator());

    /* Get `SmallBitset` respresentation of the goal state for testing the costs. */
    const Subproblem All = Subproblem::All(G.num_sources());

    /* Get costfunction Cout, adjacency matrix and join condition. */
    CostFunctionCout C_out;
    const AdjacencyMatrix &M = G.adjacency_matrix();
    static cnf::CNF condition; // TODO use join condition

    /* Heuristic search configurations. */
    using State = search_states::SubproblemsArray;


    SECTION("TopDown_sum")
    {
        /* Run heuristic search. */
        using H = heuristics::sum<PlanTable, State, expansions::TopDownComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::TopDownComplete, H, config::AStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());


        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::TopDownComplete,
                                              SearchAlgorithm,
                                              heuristics::sum,
                                              config::AStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .weighting_factor = 1.f
                                                });

        /* Fill `expected` with the anticipated plan. */
        expected.update(G, db.cardinality_estimator(), C_out, R0, R2, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0|R2, R1|R3, condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 4130);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 130);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM,
                                                                               std::numeric_limits<uint64_t>::max());


        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 12);
        CHECK(State::NUM_STATES_EXPANDED() == 4);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 13);
        CHECK(State::NUM_STATES_DISPOSED() == 1);

        CHECK(SM.num_new() == 12);
        CHECK(SM.num_duplicates() == 1);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 1);
        CHECK(SM.num_decrease_key() == 1);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 12);
        CHECK(SM.num_states_in_regular_queue() == 7);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 1);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }
}


/*======================================================================================================================
 * AnytimeAStar
 *====================================================================================================================*/

/** Short descriptions of the Test Cases:
 * - AnytimeAStar_general_functionality_and_TopDown_path_completion:
 *   For the heuristics zero, sum and GOO both the BottomUp and the TopDown version of AnytimeAStar is run witout a
 *   budget.  Each test section is named in the format "SEARCHDIRECTION_HEURISTIC_budget_#EXPANSIONS".
 *   Within each Section the following is tested:
 *   - correct path result
 *   - correct path costs
 *   - correct search counters partially reflecting the course of the search
 *
 *   TopDown GOO is used for testing the budgeting and the path completion functionality of AnytimeAStar TopDown.
 *   For each budget from 0 to 8 a Section comprising the aforementioned Checks exists.  Due to the stops of the regular
 *   heuristic search after the given number of expansions different staring points for the path completion have to be
 *   chosen, leading to different plans returned by the search.
 *   Thereby, the following cases of the search are tested:
 *   - duplicate occurence (duplicates)
 *   - g-value reduction of already existing states (cheaper)
 *   - g-value reduction of states currently in the queue (decrease_key)
 *   - g-value reduction of already expanded states and their readding to the queue (=reopening of states)
 *   - use of the cached heuristic value for duplicate states (cached_heuristic)
 *   - change of the path found by path completion with the goal as a staring state due to a cheaper path to one
 *     of the states in the original path
 *   - change of the path found by path completion with the goal as a staring state due to cheaper paths to the goal
 *
 * - AnytimeAStar_BottomUp_path_completion:
 *   BottomUp zero is used for testing the budgeting and the path completion functionality of AnytimeAStar BottomUp.
 *   Thereby, the following cases of the search additionally tested:
 *   - duplicate occurence (duplicates)
 *   - discarding of higher g-values for already existing states (discarded)
 *
 * - AnytimeAStar_TopDown_cost-based_pruning_and_weighted_search:
 *   The prior Test Cases did not include examples in which cost-based pruning occurs during the search.
 *   Here we test that the expected number of states are pruned after the expected correct number of the expansions of
 *   the search.  The upper_bound for cost-based pruning is not initialized to test the initialization of the upper
 *   bound during the search.  TopDown GOO is used for testing, as only TopDown GOO an TopDown zero lead to pruning
 *   based on an upper bound during the search.
 *   The following is tested:
 *   - correct initialization of the upper bound by the first goal state occuring during the search
 *   - pruning of child states according to the set upper bound
 *   - pruning due to a reduction of the upper bound for pruning
 *
 *   For weighted search the following is tested:
 *   - correct path result of weighted search, differing from the path result found by the unweighted version
 *
 * - AnytimeAStar_BottomUp_and_TopDown_initial_upper_bound:
 *   For both search directions the functionality of the upper bound initialization by GOO is tested
 *   TopDown sum is used for testing:
 *   - correct initialization of the upper bound by an initial GOO run
 *   - pruning according to the f-value for admissible heuristics
 *
 *   TopDown weighted sum is used for testing:
 *   - pruning according to the unweighted f-value for weighted search with an admissible heuristics
 *
 *   BottomUp zero is used for testing:
 *   - correct initialization of the upper bound by an initial GOO run
 *
 *   For TopDown zero and BottomUp zero, sum and GOO the setting of the upper bound for pruning does not become
 *   effective during the search.  This is because the expansion of a state with a goal state leads to the goal state
 *   having the same f-value as the expanded state.  Thus, as soon as a goal state is found during the search, it is
 *   expanded in the next iteration, not leading to any pruning.
 *
 * - AnytimeAStar_weighted_path_completion:
 *   For weighted anytimeAStar the unweighted f-values have to be considered to determine the starting state for GOO
 *   path completion.  In this Test Case the starting state would differ if the weighted value had been considered.
 *
 *  */



TEST_CASE("AnytimeAStar_general_functionality_and_TopDown_path_completion", "[core][IR]")
{
    using Subproblem = SmallBitset;
    using PlanTable = PlanTableSmallOrDense;

    /* Get Catalog and create new database to use for unit testing. */
    Catalog::Clear();
    Catalog &Cat = Catalog::Get();
    auto &db = Cat.add_database("db");
    Cat.set_database_in_use(db);

    /* Create pooled strings. */
    const char *str_R0 = Cat.pool("R0");
    const char *str_R1 = Cat.pool("R1");
    const char *str_R2 = Cat.pool("R2");
    const char *str_R3 = Cat.pool("R3");

    const char *col_id = Cat.pool("id");
    const char *col_fid_R1 = Cat.pool("fid_R1");
    const char *col_fid_R2 = Cat.pool("fid_R2");
    const char *col_fid_R3 = Cat.pool("fid_R3");

    /* Create tables. */
    Table &tbl_R0 = db.add_table(str_R0);
    Table &tbl_R1 = db.add_table(str_R1);
    Table &tbl_R2 = db.add_table(str_R2);
    Table &tbl_R3 = db.add_table(str_R3);

    /* Add columns to tables. */
    tbl_R0.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R3.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R1.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R2.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));

    tbl_R0.push_back(col_fid_R1, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R0.push_back(col_fid_R3, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R1.push_back(col_fid_R2, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R2.push_back(col_fid_R3, Type::Get_Integer(Type::TY_Vector, 4));

    /* Add data to tables. */
    std::size_t num_rows_R0 = 1500;
    std::size_t num_rows_R1 = 2000;
    std::size_t num_rows_R2 = 1000;
    std::size_t num_rows_R3 = 500;
    tbl_R0.store(Cat.create_store(tbl_R0));
    tbl_R1.store(Cat.create_store(tbl_R1));
    tbl_R2.store(Cat.create_store(tbl_R2));
    tbl_R3.store(Cat.create_store(tbl_R3));
    tbl_R0.layout(Cat.data_layout());
    tbl_R1.layout(Cat.data_layout());
    tbl_R2.layout(Cat.data_layout());
    tbl_R3.layout(Cat.data_layout());

    for (std::size_t i = 0; i < num_rows_R0; ++i) { tbl_R0.store().append(); }
    for (std::size_t i = 0; i < num_rows_R1; ++i) { tbl_R1.store().append(); }
    for (std::size_t i = 0; i < num_rows_R2; ++i) { tbl_R2.store().append(); }
    for (std::size_t i = 0; i < num_rows_R3; ++i) { tbl_R3.store().append(); }

    /* Define query: cycle query
     *
     *    R0
     *   /  \
     *  R3  R1
     *   \  /
     *    R2
     */

    const std::string query = " SELECT * \
                                FROM R0, R1, R2, R3 \
                                WHERE R0.fid_R1 = R1.id \
                                  AND R0.fid_R3 = R3.id \
                                  AND R1.fid_R2 = R2.id \
                                  AND R2.fid_R3 = R3.id;";

    Diagnostic diag(false, std::cout, std::cerr); // What is diagnostic?
    auto stmt = m::statement_from_string(diag, query); // TODO: use command_from_string
    REQUIRE(not diag.num_errors());
    auto query_graph = QueryGraph::Build(*stmt);
    auto &G = *query_graph.get();

    /* Create subproblems to manually add entries in the `PlanTable`. */
    const Subproblem R0(1);
    const Subproblem R1(2);
    const Subproblem R2(4);
    const Subproblem R3(8);

    /* Initialize the `InjectionCardinalityEstimator`. */
    std::istringstream json_input;
    json_input.str("{ \"mine\": [ \
                   {\"relations\": [\"R0\"], \"size\":1500}, \
                   {\"relations\": [\"R1\"], \"size\":2000}, \
                   {\"relations\": [\"R2\"], \"size\":1000}, \
                   {\"relations\": [\"R3\"], \"size\":500}, \
                   {\"relations\": [\"R0\", \"R1\"], \"size\":40}, \
                   {\"relations\": [\"R0\", \"R3\"], \"size\":100}, \
                   {\"relations\": [\"R1\", \"R2\"], \"size\":150}, \
                   {\"relations\": [\"R2\", \"R3\"], \"size\":3200}, \
                   {\"relations\": [\"R0\", \"R1\", \"R3\"], \"size\":100}, \
                   {\"relations\": [\"R0\", \"R1\", \"R2\"], \"size\":50000}, \
                   {\"relations\": [\"R0\", \"R2\", \"R3\"], \"size\":1800}, \
                   {\"relations\": [\"R1\", \"R2\", \"R3\"], \"size\":700}, \
                   {\"relations\": [\"R0\", \"R1\", \"R2\", \"R3\"], \"size\":15000} \
                   ]}");

    auto ICE = std::make_unique<InjectionCardinalityEstimator>(diag, "mine", json_input);
    db.cardinality_estimator(std::move(ICE));

    /* Initialize `PlanTable` for base case. */
    PlanTable plan_table(G);
    init_PT_base_case(G, plan_table, db.cardinality_estimator());

    PlanTable expected(G);
    init_PT_base_case(G, expected, db.cardinality_estimator());

    /* Get `SmallBitset` respresentation of the goal state for testing the costs. */
    const Subproblem All = Subproblem::All(G.num_sources());

    /* Get costfunction Cout, adjacency matrix and join condition. */
    CostFunctionCout C_out;
    const AdjacencyMatrix &M = G.adjacency_matrix();
    static cnf::CNF condition; // TODO use join condition

    /* Heuristic search configurations. */
    using State = search_states::SubproblemsArray;

    /* General functionality of AnytimeAStar. */
    /* The following sections test the general functionality of AnytimeAStar without an expansion limit for both search
     * directions and the heuristics zero, sum and GOO. Each search has to lead to the expected plan and match the
     * expected counter values. */

    SECTION("BottomUp_zero_budget_max")
    {
        /* Run heuristic search. */
        using H = heuristics::zero<PlanTable, State, expansions::BottomUpComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::BottomUpComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = std::numeric_limits<uint64_t>::max();

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::BottomUpComplete,
                                              SearchAlgorithm,
                                              heuristics::zero,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget,
                                                });

        /* Caution: In Case of the Injected Cardinality Estimator the join operation is commutative. Therefore the given
         * order of the left and the right subproblem is saved. As subproblems in heuristic search are ordered
         * lexicographically increasing we should keep this order here to check for equality. (Other option: change
         * equality operator for PlanTable entries with the same costs such that they are also equal if the joined
         * subproblems are only swapped.) */

        /* Fill `expected` with the anticipated plan. */
        expected.update(G, db.cardinality_estimator(), C_out, R0, R1, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0|R1, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R2, R3|(R0|R1), condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15140);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 140);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 10);
        CHECK(State::NUM_STATES_EXPANDED() == 4);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 11);
        CHECK(State::NUM_STATES_DISPOSED() == 1);

        CHECK(SM.num_new() == 10);
        CHECK(SM.num_duplicates() == 1);
        CHECK(SM.num_discarded() == 1);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 10);
        CHECK(SM.num_states_in_regular_queue() == 5);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);

        CHECK(S.num_cached_heuristic_value() == 1);
    }


    SECTION("BottomUp_zero_budget_0")
    {
        /* Run heuristic search. */
        using H = heuristics::zero<PlanTable, State, expansions::BottomUpComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::BottomUpComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 0;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::BottomUpComplete,
                                              SearchAlgorithm,
                                              heuristics::zero,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget,
                                                });

        /* Fill `expected` with the anticipated plan. */
        expected.update(G, db.cardinality_estimator(), C_out, R0, R1, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0|R1, R3, condition);
        // Lexicographically increasing order not consisent for plans found by GOO
        expected.update(G, db.cardinality_estimator(), C_out, R0|R1|R3, R2, condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15140);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 140);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 0);
        CHECK(State::NUM_STATES_EXPANDED() == 0);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 1);
        CHECK(State::NUM_STATES_DISPOSED() == 0);

        CHECK(SM.num_new() == 1);
        CHECK(SM.num_duplicates() == 0);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 1);
        CHECK(SM.num_states_in_regular_queue() == 1);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);

        CHECK(S.num_cached_heuristic_value() == 0);
    }


    SECTION("BottomUp_sum_budget_max")
    {
        /* Run heuristic search. */
        using H = heuristics::sum<PlanTable, State, expansions::BottomUpComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::BottomUpComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = std::numeric_limits<uint64_t>::max();

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::BottomUpComplete,
                                              SearchAlgorithm,
                                              heuristics::sum,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        expected.update(G, db.cardinality_estimator(), C_out, R0, R1, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0|R1, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R2,  R0|R1|R3, condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15140);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 140);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 8);
        CHECK(State::NUM_STATES_EXPANDED() == 3);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 9);
        CHECK(State::NUM_STATES_DISPOSED() == 0);

        CHECK(SM.num_new() == 9);
        CHECK(SM.num_duplicates() == 0);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 9);
        CHECK(SM.num_states_in_regular_queue() == 5);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);

        CHECK(S.num_cached_heuristic_value() == 0);
    }

    SECTION("BottomUp_GOO_budget_max")
    {
        /* Run heuristic search. */
        using H = heuristics::GOO<PlanTable, State, expansions::BottomUpComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::BottomUpComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = std::numeric_limits<uint64_t>::max();

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::BottomUpComplete,
                                              SearchAlgorithm,
                                              heuristics::GOO,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        expected.update(G, db.cardinality_estimator(), C_out, R0, R1, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0|R1, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R2,  R0|R1|R3, condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15140);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 140);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 8);
        CHECK(State::NUM_STATES_EXPANDED() == 3);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 9);
        CHECK(State::NUM_STATES_DISPOSED() == 0);

        CHECK(SM.num_new() == 9);
        CHECK(SM.num_duplicates() == 0);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 9);
        CHECK(SM.num_states_in_regular_queue() == 5);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);

        CHECK(S.num_cached_heuristic_value() == 0);
    }


    SECTION("TopDown_zero_budget_max")
    {
        /* Run heuristic search. */
        using H = heuristics::zero<PlanTable, State, expansions::TopDownComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::TopDownComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = std::numeric_limits<uint64_t>::max();

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::TopDownComplete,
                                              SearchAlgorithm,
                                              heuristics::zero,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        expected.update(G, db.cardinality_estimator(), C_out, R0, R1, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0|R1, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R2,  R0|R1|R3, condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15140);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 140);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 19);
        CHECK(State::NUM_STATES_EXPANDED() == 10);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 20);

        CHECK(SM.num_new() == 12);
        // duplic, discarded, cheaper, decrease_key depend on expansion order of states with f-value 0
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 12);
        CHECK(SM.num_states_in_regular_queue() == 1);
        CHECK(SM.num_states_in_beam_queue() == 0);
        // num_st_cached_heuristic_value depends on expansion order of states with f-value 0
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }


    SECTION("TopDown_sum_budget_max")
    {
        /* Run heuristic search. */
        using H = heuristics::sum<PlanTable, State, expansions::TopDownComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::TopDownComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = std::numeric_limits<uint64_t>::max();

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::TopDownComplete,
                                              SearchAlgorithm,
                                              heuristics::sum,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        expected.update(G, db.cardinality_estimator(), C_out, R0, R1, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0|R1, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R2,  R0|R1|R3, condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15140);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 140);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 9);
        CHECK(State::NUM_STATES_EXPANDED() == 3);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 10);
        CHECK(State::NUM_STATES_DISPOSED() == 0);

        CHECK(SM.num_new() == 10);
        CHECK(SM.num_duplicates() == 0);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 10);
        CHECK(SM.num_states_in_regular_queue() == 6);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);

        CHECK(S.num_cached_heuristic_value() == 0);
    }


    /* TopDown GOO variant for testing the path completion of AnytimeAStar. */
    /** In the case that no path to the goal is found within the expansion budget, TDGOO is used to complete the path.
     * The starting state for the completion is the state with the lowest g-value among the states with the least
     * steps/joins remaining to reach the goal. The GOO heuristic is neither admissible, nor consistent in the TopDown
     * variant. For both admissible and non-admissible heuristics to starting points out of the optimal path can
     * occur. Besides testing the correct funcionlity of path completion the following sections test:
     * - duplicate occurence (duplicates)
     * - g-value reduction of already existing states (cheaper)
     * - g-value reduction of states currently in the queue (decrease_key)
     * - g-value reduction of already expanded states and their readding to the queue (=reopening of states)
     * - use of the cached heuristic value for duplicate states (cached_heuristic)
     * - change of the path found by path completion with the goal as a staring state due to a cheaper path to one
     *   of the states in the original path
     * - change of the path found by path completion with the goal as a staring state due to cheaper paths to the goal
     * */

    SECTION("TopDown_GOO_budget_0")
    {
        /* Run heuristic search. */
        using H = heuristics::GOO<PlanTable, State, expansions::TopDownComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::TopDownComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 0;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::TopDownComplete,
                                              SearchAlgorithm,
                                              heuristics::GOO,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        /* Full path completion via TDGOO from the starting state. */
        expected.update(G, db.cardinality_estimator(), C_out, R0, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1, R2, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0|R3, R1|R2, condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15250);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 250);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 0);
        CHECK(State::NUM_STATES_EXPANDED() == 0);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 1);
        CHECK(State::NUM_STATES_DISPOSED() == 0);

        CHECK(SM.num_new() == 1);
        CHECK(SM.num_duplicates() == 0);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 1);
        CHECK(SM.num_states_in_regular_queue() == 1);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);

        CHECK(S.num_cached_heuristic_value() == 0);
    }

    SECTION("TopDown_GOO_budget_1")
    {
        /* Run heuristic search. */
        using H = heuristics::GOO<PlanTable, State, expansions::TopDownComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::TopDownComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 1;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::TopDownComplete,
                                              SearchAlgorithm,
                                              heuristics::GOO,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        /* Starting state for the path completion: {{R0,R2,R3},{R1}}. */
        expected.update(G, db.cardinality_estimator(), C_out, R0, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0|R3, R2, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1, R0|R2|R3, condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 16900);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 1900);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 6);
        CHECK(State::NUM_STATES_EXPANDED() == 1);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 7);
        CHECK(State::NUM_STATES_DISPOSED() == 0);

        CHECK(SM.num_new() == 7);
        CHECK(SM.num_duplicates() == 0);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 7);
        CHECK(SM.num_states_in_regular_queue() == 6);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);

        CHECK(S.num_cached_heuristic_value() == 0);
    }

    SECTION("TopDown_GOO_budget_2")
    {
        /* Run heuristic search. */
        using H = heuristics::GOO<PlanTable, State, expansions::TopDownComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::TopDownComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 2;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::TopDownComplete,
                                              SearchAlgorithm,
                                              heuristics::GOO,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        /* Starting state for the path completion: {{R2,R3},{R0},{R1}}. */
        expected.update(G, db.cardinality_estimator(), C_out, R2, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0, R2|R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1, R0|R2|R3, condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 20000);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 5000);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 8);
        CHECK(State::NUM_STATES_EXPANDED() == 2);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 9);
        CHECK(State::NUM_STATES_DISPOSED() == 0);

        CHECK(SM.num_new() == 9);
        CHECK(SM.num_duplicates() == 0);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 9);
        CHECK(SM.num_states_in_regular_queue() == 7);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);

        CHECK(S.num_cached_heuristic_value() == 0);
    }

    SECTION("TopDown_GOO_budget_3")
    {
        /* Run heuristic search. */
        using H = heuristics::GOO<PlanTable, State, expansions::TopDownComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::TopDownComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 3;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::TopDownComplete,
                                              SearchAlgorithm,
                                              heuristics::GOO,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        /* Leads to the same plan as with budget 2. The state {{R2,R3},{R0},{R1}} is expanded, adding the goal state to
         * the queue. The goal state is the starting state for the path completion, no more path completion necessary.*/
        expected.update(G, db.cardinality_estimator(), C_out, R2, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0, R2|R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1, R0|R2|R3, condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 20000);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 5000);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 9);
        CHECK(State::NUM_STATES_EXPANDED() == 3);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 10);
        CHECK(State::NUM_STATES_DISPOSED() == 0);

        CHECK(SM.num_new() == 10);
        CHECK(SM.num_duplicates() == 0);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 10);
        CHECK(SM.num_states_in_regular_queue() == 7);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);

        CHECK(S.num_cached_heuristic_value() == 0);

    }

    SECTION("TopDown_GOO_budget_4")
    {
        /* Run heuristic search. */
        using H = heuristics::GOO<PlanTable, State, expansions::TopDownComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::TopDownComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 4;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::TopDownComplete,
                                              SearchAlgorithm,
                                              heuristics::GOO,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        /* Found cheaper path to state {{R2,R3},{R0},{R1}} by the expansion of {{R1,R2,R3},{R0}}. This leads to a
         * reopening, since {{R2,R3},{R0},{R1}} was expanded already. No new path to the goal state was found. But since
         * the previous path to the goal state included the state {{R2,R3},{R0},{R1}}, the previous path is now updated
         * with the cheaper join to {{R1,R2,R3},{R0}} instead of {{R0,R2,R3},{R1}}. */
        expected.update(G, db.cardinality_estimator(), C_out, R2, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1, R2|R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0, R1|R2|R3, condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 18900);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 3900);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 11);
        CHECK(State::NUM_STATES_EXPANDED() == 4);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 12);
        CHECK(State::NUM_STATES_DISPOSED() == 1);

        CHECK(SM.num_new() == 11); // already expanded state is pushed to the queue again with cheaper g
        CHECK(SM.num_duplicates() == 1);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 1); // #reopened = #cheaper-#decrease_key
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 11);
        CHECK(SM.num_states_in_regular_queue() == 8); // the reopened state is in the queue
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);

        CHECK(S.num_cached_heuristic_value() == 1);
    }

    SECTION("TopDown_GOO_budget_5")
    {
        /* Run heuristic search. */
        using H = heuristics::GOO<PlanTable, State, expansions::TopDownComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::TopDownComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 5;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::TopDownComplete,
                                              SearchAlgorithm,
                                              heuristics::GOO,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        /* Leads to the same plan as with budget 4. Here {{R2,R3},{R0},{R1}} is expanded, leading to the goal state on
         * a cheaper path. This cheaper path was already considered in the prior section as the state
         * {{R2,R3},{R0},{R1}} was reached on a cheaper path. */
        expected.update(G, db.cardinality_estimator(), C_out, R2, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1, R2|R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0, R1|R2|R3, condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 18900);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 3900);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 12);
        CHECK(State::NUM_STATES_EXPANDED() == 5);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 13);
        CHECK(State::NUM_STATES_DISPOSED() == 2);

        CHECK(SM.num_new() == 11);
        CHECK(SM.num_duplicates() == 2);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 2);
        CHECK(SM.num_decrease_key() == 1); // cheaper path to the goal in the queue
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 11);
        CHECK(SM.num_states_in_regular_queue() == 7); // no new state added
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);

        CHECK(S.num_cached_heuristic_value() == 2);
    }

    SECTION("TopDown_GOO_budget_6")
    {
        /* Run heuristic search. */
        using H = heuristics::GOO<PlanTable, State, expansions::TopDownComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::TopDownComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 6;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::TopDownComplete,
                                              SearchAlgorithm,
                                              heuristics::GOO,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        /* Again a cheaper path to the goal is found as the new expansion adds a goal state with lower g-value to the
         * queue. */
        expected.update(G, db.cardinality_estimator(), C_out, R1, R2, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1|R2, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0, R1|R2|R3, condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15850);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 850);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 13);
        CHECK(State::NUM_STATES_EXPANDED() == 6);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 14);
        CHECK(State::NUM_STATES_DISPOSED() == 3);

        CHECK(SM.num_new() == 11);
        CHECK(SM.num_duplicates() == 3);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 3);
        CHECK(SM.num_decrease_key() == 2); // cheaper path to the goal in the queue
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 11);
        CHECK(SM.num_states_in_regular_queue() == 6); // no new state added
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);

        CHECK(S.num_cached_heuristic_value() == 3);
    }

    SECTION("TopDown_GOO_budget_7")
    {
        /* Run heuristic search. */
        using H = heuristics::GOO<PlanTable, State, expansions::TopDownComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::TopDownComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 7;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::TopDownComplete,
                                              SearchAlgorithm,
                                              heuristics::GOO,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        /* The goal state is expanded, not leading to a cheaper path than already found by path completion. */
        expected.update(G, db.cardinality_estimator(), C_out, R1, R2, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1|R2, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0, R1|R2|R3, condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15850);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 850);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 13);
        CHECK(State::NUM_STATES_EXPANDED() == 6); // "expansion" of the goal state does not count as an expansion
        CHECK(State::NUM_STATES_CONSTRUCTED() == 14);
        CHECK(State::NUM_STATES_DISPOSED() == 3);

        CHECK(SM.num_new() == 11);
        CHECK(SM.num_duplicates() == 3);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 3);
        CHECK(SM.num_decrease_key() == 2);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 11);
        CHECK(SM.num_states_in_regular_queue() == 5);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);

        CHECK(S.num_cached_heuristic_value() == 3);
    }

    SECTION("TopDown_GOO_budget_8")
    {
        /* Run heuristic search. */
        using H = heuristics::GOO<PlanTable, State, expansions::TopDownComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::TopDownComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 8;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::TopDownComplete,
                                              SearchAlgorithm,
                                              heuristics::GOO,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                             {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        /* As the path to the goal state is found within 7 expansions, a higher budget has to lead to the same path and
         * the same counter values. */
        expected.update(G, db.cardinality_estimator(), C_out, R1, R2, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1|R2, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0, R1|R2|R3, condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15850);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 850);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 13);
        CHECK(State::NUM_STATES_EXPANDED() == 6);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 14);
        CHECK(State::NUM_STATES_DISPOSED() == 3);

        CHECK(SM.num_new() == 11);
        CHECK(SM.num_duplicates() == 3);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 3);
        CHECK(SM.num_decrease_key() == 2);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 11);
        CHECK(SM.num_states_in_regular_queue() == 5);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);

        CHECK(S.num_cached_heuristic_value() == 3);
    }

    SECTION("TopDown_GOO_budget_max")
    {
        /* Run heuristic search. */
        using H = heuristics::GOO<PlanTable, State, expansions::TopDownComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::TopDownComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = std::numeric_limits<uint64_t>::max();

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::TopDownComplete,
                                              SearchAlgorithm,
                                              heuristics::GOO,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        /* As the path to the goal state is found within 7 expansions, a higher budget has to lead to the same path and
         * the same counter values. */
        expected.update(G, db.cardinality_estimator(), C_out, R1, R2, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1|R2, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0, R1|R2|R3, condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15850);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 850);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 13);
        CHECK(State::NUM_STATES_EXPANDED() == 6);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 14);
        CHECK(State::NUM_STATES_DISPOSED() == 3);

        CHECK(SM.num_new() == 11);
        CHECK(SM.num_duplicates() == 3);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 3);
        CHECK(SM.num_decrease_key() == 2);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 11);
        CHECK(SM.num_states_in_regular_queue() == 5);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);

        CHECK(S.num_cached_heuristic_value() == 3);
    }

}


TEST_CASE("AnytimeAStar_BottomUp_path_completion", "[core][IR]")
{
    using Subproblem = SmallBitset;
    using PlanTable = PlanTableSmallOrDense;

    /* Get Catalog and create new database to use for unit testing. */
    Catalog::Clear();
    Catalog &Cat = Catalog::Get();
    auto &db = Cat.add_database("db");
    Cat.set_database_in_use(db);

    /* Create pooled strings. */
    const char *str_R0 = Cat.pool("R0");
    const char *str_R1 = Cat.pool("R1");
    const char *str_R2 = Cat.pool("R2");
    const char *str_R3 = Cat.pool("R3");

    const char *col_id = Cat.pool("id");
    const char *col_fid_R1 = Cat.pool("fid_R1");
    const char *col_fid_R2 = Cat.pool("fid_R2");
    const char *col_fid_R3 = Cat.pool("fid_R3");

    /* Create tables. */
    Table &tbl_R0 = db.add_table(str_R0);
    Table &tbl_R1 = db.add_table(str_R1);
    Table &tbl_R2 = db.add_table(str_R2);
    Table &tbl_R3 = db.add_table(str_R3);

    /* Add columns to tables. */
    tbl_R0.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R3.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R1.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R2.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));

    tbl_R0.push_back(col_fid_R1, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R0.push_back(col_fid_R3, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R1.push_back(col_fid_R2, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R2.push_back(col_fid_R3, Type::Get_Integer(Type::TY_Vector, 4));

    /* Add data to tables. */
    std::size_t num_rows_R0 = 1500;
    std::size_t num_rows_R1 = 2000;
    std::size_t num_rows_R2 = 1000;
    std::size_t num_rows_R3 = 500;
    tbl_R0.store(Cat.create_store(tbl_R0));
    tbl_R1.store(Cat.create_store(tbl_R1));
    tbl_R2.store(Cat.create_store(tbl_R2));
    tbl_R3.store(Cat.create_store(tbl_R3));
    tbl_R0.layout(Cat.data_layout());
    tbl_R1.layout(Cat.data_layout());
    tbl_R2.layout(Cat.data_layout());
    tbl_R3.layout(Cat.data_layout());

    for (std::size_t i = 0; i < num_rows_R0; ++i) { tbl_R0.store().append(); }
    for (std::size_t i = 0; i < num_rows_R1; ++i) { tbl_R1.store().append(); }
    for (std::size_t i = 0; i < num_rows_R2; ++i) { tbl_R2.store().append(); }
    for (std::size_t i = 0; i < num_rows_R3; ++i) { tbl_R3.store().append(); }

    /* Define query: cycle query
     *
     *    R0
     *   /  \
     *  R3  R1
     *   \  /
     *    R2
     */

    const std::string query = " SELECT * \
                                FROM R0, R1, R2, R3 \
                                WHERE R0.fid_R1 = R1.id \
                                  AND R0.fid_R3 = R3.id \
                                  AND R1.fid_R2 = R2.id \
                                  AND R2.fid_R3 = R3.id;";

    Diagnostic diag(false, std::cout, std::cerr); // What is diagnostic?
    auto stmt = m::statement_from_string(diag, query); // TODO: use command_from_string
    REQUIRE(not diag.num_errors());
    auto query_graph = QueryGraph::Build(*stmt);
    auto &G = *query_graph.get();

    /* Create subproblems to manually add entries in the `PlanTable`. */
    const Subproblem R0(1);
    const Subproblem R1(2);
    const Subproblem R2(4);
    const Subproblem R3(8);

    /* Initialize the `InjectionCardinalityEstimator`. */
    std::istringstream json_input;
    json_input.str("{ \"mine\": [ \
                   {\"relations\": [\"R0\"], \"size\":1500}, \
                   {\"relations\": [\"R1\"], \"size\":2000}, \
                   {\"relations\": [\"R2\"], \"size\":1000}, \
                   {\"relations\": [\"R3\"], \"size\":500}, \
                   {\"relations\": [\"R0\", \"R1\"], \"size\":40}, \
                   {\"relations\": [\"R0\", \"R3\"], \"size\":100}, \
                   {\"relations\": [\"R1\", \"R2\"], \"size\":150}, \
                   {\"relations\": [\"R2\", \"R3\"], \"size\":3200}, \
                   {\"relations\": [\"R0\", \"R1\", \"R3\"], \"size\":600}, \
                   {\"relations\": [\"R0\", \"R1\", \"R2\"], \"size\":50000}, \
                   {\"relations\": [\"R0\", \"R2\", \"R3\"], \"size\":400}, \
                   {\"relations\": [\"R1\", \"R2\", \"R3\"], \"size\":700}, \
                   {\"relations\": [\"R0\", \"R1\", \"R2\", \"R3\"], \"size\":15000} \
                   ]}");

    auto ICE = std::make_unique<InjectionCardinalityEstimator>(diag, "mine", json_input);
    db.cardinality_estimator(std::move(ICE));

    /* Initialize `PlanTable` for base case. */
    PlanTable plan_table(G);
    init_PT_base_case(G, plan_table, db.cardinality_estimator());

    PlanTable expected(G);
    init_PT_base_case(G, expected, db.cardinality_estimator());

    /* Get `SmallBitset` respresentation of the goal state for testing the costs. */
    const Subproblem All = Subproblem::All(G.num_sources());

    /* Get costfunction Cout, adjacency matrix and join condition. */
    CostFunctionCout C_out;
    const AdjacencyMatrix &M = G.adjacency_matrix();
    static cnf::CNF condition; // TODO use join condition

    /* Heuristic search configurations. */
    using State = search_states::SubproblemsArray;


    /* BottomUp zero variant for testing the path completion of AnytimeAStar. */
    /* Besides testing the correct funcionality of path completion the following sections test:
     * - duplicate occurence (duplicates)
     * - discarding of higher g-values for already existing states (discarded)
     */

    SECTION("BottomUp_zero_budget_0")
    {
        /* Run heuristic search. */
        using H = heuristics::zero<PlanTable, State, expansions::BottomUpComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::BottomUpComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 0;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::BottomUpComplete,
                                              SearchAlgorithm,
                                              heuristics::zero,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        expected.update(G, db.cardinality_estimator(), C_out, R0, R1, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0|R1, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0|R1|R3, R2 , condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15640);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 640);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);


        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 0);
        CHECK(State::NUM_STATES_EXPANDED() == 0);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 1);
        CHECK(State::NUM_STATES_DISPOSED() == 0);

        CHECK(SM.num_new() == 1);
        CHECK(SM.num_duplicates() == 0);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 1);
        CHECK(SM.num_states_in_regular_queue() == 1);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }

        SECTION("BottomUp_zero_budget_1")
    {
        /* Run heuristic search. */
        using H = heuristics::zero<PlanTable, State, expansions::BottomUpComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::BottomUpComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());
        uint64_t budget = 1;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::BottomUpComplete,
                                              SearchAlgorithm,
                                              heuristics::zero,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        expected.update(G, db.cardinality_estimator(), C_out, R0, R1, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0|R1, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0|R1|R3, R2 , condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15640);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 640);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 4);
        CHECK(State::NUM_STATES_EXPANDED() == 1);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 5);
        CHECK(State::NUM_STATES_DISPOSED() == 0);

        CHECK(SM.num_new() == 5);
        CHECK(SM.num_duplicates() == 0);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 5);
        CHECK(SM.num_states_in_regular_queue() == 4);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }

    SECTION("BottomUp_zero_budget_2")
    {
        /* Run heuristic search. */
        using H = heuristics::zero<PlanTable, State, expansions::BottomUpComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::BottomUpComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 2;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::BottomUpComplete,
                                              SearchAlgorithm,
                                              heuristics::zero,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        /* Leads to the same plan as with budget 1, as the state with the least remaining joins to the full plan is
         * chosen as the starting point for the plan completion. */
        expected.update(G, db.cardinality_estimator(), C_out, R0, R1, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0|R1, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R2 , R0|R1|R3 , condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15640);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 640);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 7);
        CHECK(State::NUM_STATES_EXPANDED() == 2);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 8);
        CHECK(State::NUM_STATES_DISPOSED() == 0);

        CHECK(SM.num_new() == 8);
        CHECK(SM.num_duplicates() == 0);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 8);
        CHECK(SM.num_states_in_regular_queue() == 6);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }

    SECTION("BottomUp_zero_budget_3")
    {
        /* Run heuristic search. */
        using H = heuristics::zero<PlanTable, State, expansions::BottomUpComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::BottomUpComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 3;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::BottomUpComplete,
                                              SearchAlgorithm,
                                              heuristics::zero,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        /* Leads to the same plan as with budget 1, as the state with the least remaining joins to the full plan is
         * chosen as the starting point for the plan reconstruction. */
        expected.update(G, db.cardinality_estimator(), C_out, R0, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R2 , R0|R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1 , R0|R2|R3 , condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15500);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 500);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 9);
        CHECK(State::NUM_STATES_EXPANDED() == 3);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 10);
        CHECK(State::NUM_STATES_DISPOSED() == 1);

        CHECK(SM.num_new() == 9);
        CHECK(SM.num_duplicates() == 1);
        CHECK(SM.num_discarded() == 1);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 9);
        CHECK(SM.num_states_in_regular_queue() == 6);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 1);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }

    SECTION("BottomUp_zero_budget_4")
    {
        /* Run heuristic search. */
        using H = heuristics::zero<PlanTable, State, expansions::BottomUpComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::BottomUpComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());
        uint64_t budget = 4;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::BottomUpComplete,
                                              SearchAlgorithm,
                                              heuristics::zero,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        expected.update(G, db.cardinality_estimator(), C_out, R1, R2, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0 , R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1|R2 , R0|R3 , condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15250);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 250);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 12);
        CHECK(State::NUM_STATES_EXPANDED() == 4);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 13);
        CHECK(State::NUM_STATES_DISPOSED() == 2);

        CHECK(SM.num_new() == 11);
        CHECK(SM.num_duplicates() == 2);
        CHECK(SM.num_discarded() == 2);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 11);
        CHECK(SM.num_states_in_regular_queue() == 7);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 2);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }

    SECTION("BottomUp_zero_budget_5")
    {
        /* Run heuristic search. */
        using H = heuristics::zero<PlanTable, State, expansions::BottomUpComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::BottomUpComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 5;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::BottomUpComplete,
                                              SearchAlgorithm,
                                              heuristics::zero,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        /* Leads to the same plan as with budget 4, as the state {{R1,R2},{R0,R3}} is expanded as the fifth expansion,
         * leading to the goal state {R0,R1,R2,R3}. As the goal state now is the state with the lowest f-value among the
         * states with the least remaining joins to the full plan, this state is chosen as the starting point for the
         * plan completion. Since it already is the goal state, it provides a full plan, so no further completion is
         * needed. */
        expected.update(G, db.cardinality_estimator(), C_out, R1, R2, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0 , R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1|R2 , R0|R3 , condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15250);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 250);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 13);
        CHECK(State::NUM_STATES_EXPANDED() == 5);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 14);
        CHECK(State::NUM_STATES_DISPOSED() == 2);

        CHECK(SM.num_new() == 12);
        CHECK(SM.num_duplicates() == 2);
        CHECK(SM.num_discarded() == 2);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 12);
        CHECK(SM.num_states_in_regular_queue() == 7);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 2);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }

    SECTION("BottomUp_zero_budget_6")
    {
        /* Run heuristic search. */
        using H = heuristics::zero<PlanTable, State, expansions::BottomUpComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::BottomUpComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 6;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::BottomUpComplete,
                                              SearchAlgorithm,
                                              heuristics::zero,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        /* Leads to the same plan as with budget 5, as the state {R0,R1,R2,R3} is expanded as the sixth expansion,
         * leading to the end of the search. Thus, no completion is needed. */
        expected.update(G, db.cardinality_estimator(), C_out, R1, R2, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0 , R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1|R2 , R0|R3 , condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15250);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 250);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 13);
        CHECK(State::NUM_STATES_EXPANDED() == 5);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 14);
        CHECK(State::NUM_STATES_DISPOSED() == 2);

        CHECK(SM.num_new() == 12);
        CHECK(SM.num_duplicates() == 2);
        CHECK(SM.num_discarded() == 2);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 12);
        CHECK(SM.num_states_in_regular_queue() == 6);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 2);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }

    SECTION("BottomUp_zero_budget_7")
    {
        /* Run heuristic search. */
        using H = heuristics::zero<PlanTable, State, expansions::BottomUpComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::BottomUpComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());
        uint64_t budget = 7;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::BottomUpComplete,
                                              SearchAlgorithm,
                                              heuristics::zero,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        /* Leads to the same plan as with budget 6, as the search finishes within 6 expansions. */
        expected.update(G, db.cardinality_estimator(), C_out, R1, R2, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0 , R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1|R2 , R0|R3 , condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15250);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 250);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        /* Since the search finished within 6 expansions, the counters should remain the same as for the test section
         * with budget 6. */
        CHECK(State::NUM_STATES_GENERATED() == 13);
        CHECK(State::NUM_STATES_EXPANDED() == 5);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 14);
        CHECK(State::NUM_STATES_DISPOSED() == 2);

        CHECK(SM.num_new() == 12);
        CHECK(SM.num_duplicates() == 2);
        CHECK(SM.num_discarded() == 2);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 12);
        CHECK(SM.num_states_in_regular_queue() == 6);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 2);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }

    SECTION("BottomUp_zero_budget_max")
    {
        /* Run heuristic search. */
        using H = heuristics::zero<PlanTable, State, expansions::BottomUpComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::BottomUpComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());
        uint64_t budget = std::numeric_limits<uint64_t>::max();

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::BottomUpComplete,
                                              SearchAlgorithm,
                                              heuristics::zero,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        /* Leads to the same plan as with budget 6, as the search finishes within a budget of 6 expansions. */
        expected.update(G, db.cardinality_estimator(), C_out, R1, R2, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0 , R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1|R2 , R0|R3 , condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15250);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 250);

        /* Dependencies between the search state counters, that must hold true for all search configurations. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        /* Since the search finished within 6 expansions, the counters should remain the same as for the test section
         * with budget 6. */
        CHECK(State::NUM_STATES_GENERATED() == 13);
        CHECK(State::NUM_STATES_EXPANDED() == 5);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 14);
        CHECK(State::NUM_STATES_DISPOSED() == 2);

        CHECK(SM.num_new() == 12);
        CHECK(SM.num_duplicates() == 2);
        CHECK(SM.num_discarded() == 2);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 12);
        CHECK(SM.num_states_in_regular_queue() == 6);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 2);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }
}


TEST_CASE("AnytimeAStar_TopDown_cost-based_pruning_and_weighted_search", "[core][IR]")
{
    using Subproblem = SmallBitset;
    using PlanTable = PlanTableSmallOrDense;

    /* Get Catalog and create new database to use for unit testing. */
    Catalog::Clear();
    Catalog &Cat = Catalog::Get();
    auto &db = Cat.add_database("db");
    Cat.set_database_in_use(db);

    /* Create pooled strings. */
    const char *str_R0 = Cat.pool("R0");
    const char *str_R1 = Cat.pool("R1");
    const char *str_R2 = Cat.pool("R2");
    const char *str_R3 = Cat.pool("R3");

    const char *col_id = Cat.pool("id");
    const char *col_fid_R1 = Cat.pool("fid_R1");
    const char *col_fid_R2 = Cat.pool("fid_R2");
    const char *col_fid_R3 = Cat.pool("fid_R3");

    /* Create tables. */
    Table &tbl_R0 = db.add_table(str_R0);
    Table &tbl_R1 = db.add_table(str_R1);
    Table &tbl_R2 = db.add_table(str_R2);
    Table &tbl_R3 = db.add_table(str_R3);

    /* Add columns to tables. */
    tbl_R0.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R3.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R1.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R2.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));

    tbl_R0.push_back(col_fid_R1, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R0.push_back(col_fid_R3, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R1.push_back(col_fid_R2, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R2.push_back(col_fid_R3, Type::Get_Integer(Type::TY_Vector, 4));

    /* Add data to tables. */
    std::size_t num_rows_R0 = 1450;
    std::size_t num_rows_R1 = 2000;
    std::size_t num_rows_R2 = 940;
    std::size_t num_rows_R3 = 500;
    tbl_R0.store(Cat.create_store(tbl_R0));
    tbl_R1.store(Cat.create_store(tbl_R1));
    tbl_R2.store(Cat.create_store(tbl_R2));
    tbl_R3.store(Cat.create_store(tbl_R3));
    tbl_R0.layout(Cat.data_layout());
    tbl_R1.layout(Cat.data_layout());
    tbl_R2.layout(Cat.data_layout());
    tbl_R3.layout(Cat.data_layout());

    for (std::size_t i = 0; i < num_rows_R0; ++i) { tbl_R0.store().append(); }
    for (std::size_t i = 0; i < num_rows_R1; ++i) { tbl_R1.store().append(); }
    for (std::size_t i = 0; i < num_rows_R2; ++i) { tbl_R2.store().append(); }
    for (std::size_t i = 0; i < num_rows_R3; ++i) { tbl_R3.store().append(); }

    /* Define query: cycle query
     *
     *    R0
     *   /  \
     *  R3  R1
     *   \  /
     *    R2
     */

    const std::string query = " SELECT * \
                                FROM R0, R1, R2, R3 \
                                WHERE R0.fid_R1 = R1.id \
                                  AND R0.fid_R3 = R3.id \
                                  AND R1.fid_R2 = R2.id \
                                  AND R2.fid_R3 = R3.id;";

    Diagnostic diag(false, std::cout, std::cerr); // What is diagnostic?
    auto stmt = m::statement_from_string(diag, query); // TODO: use command_from_string
    REQUIRE(not diag.num_errors());
    auto query_graph = QueryGraph::Build(*stmt);
    auto &G = *query_graph.get();

    /* Create subproblems to manually add entries in the `PlanTable`. */
    const Subproblem R0(1);
    const Subproblem R1(2);
    const Subproblem R2(4);
    const Subproblem R3(8);

    /* Initialize the `InjectionCardinalityEstimator`. */
    std::istringstream json_input;
    json_input.str("{ \"mine\": [ \
                   {\"relations\": [\"R0\"], \"size\":1450}, \
                   {\"relations\": [\"R1\"], \"size\":2000}, \
                   {\"relations\": [\"R2\"], \"size\":940}, \
                   {\"relations\": [\"R3\"], \"size\":500}, \
                   {\"relations\": [\"R0\", \"R1\"], \"size\":40}, \
                   {\"relations\": [\"R0\", \"R3\"], \"size\":100}, \
                   {\"relations\": [\"R1\", \"R2\"], \"size\":700}, \
                   {\"relations\": [\"R2\", \"R3\"], \"size\":3200}, \
                   {\"relations\": [\"R0\", \"R1\", \"R3\"], \"size\":5800}, \
                   {\"relations\": [\"R0\", \"R1\", \"R2\"], \"size\":5000}, \
                   {\"relations\": [\"R0\", \"R2\", \"R3\"], \"size\":2500}, \
                   {\"relations\": [\"R1\", \"R2\", \"R3\"], \"size\":1700}, \
                   {\"relations\": [\"R0\", \"R1\", \"R2\", \"R3\"], \"size\":15000} \
                   ]}");

    auto ICE = std::make_unique<InjectionCardinalityEstimator>(diag, "mine", json_input);
    db.cardinality_estimator(std::move(ICE));

    /* Initialize `PlanTable` for base case. */
    PlanTable plan_table(G);
    init_PT_base_case(G, plan_table, db.cardinality_estimator());

    PlanTable expected(G);
    init_PT_base_case(G, expected, db.cardinality_estimator());

    /* Get `SmallBitset` respresentation of the goal state for testing the costs. */
    const Subproblem All = Subproblem::All(G.num_sources());

    /* Get costfunction Cout, adjacency matrix and join condition. */
    CostFunctionCout C_out;
    const AdjacencyMatrix &M = G.adjacency_matrix();
    static cnf::CNF condition; // TODO use join condition

    /* Heuristic search configurations. */
    using State = search_states::SubproblemsArray;


    /* TopDown GOO variant for testing cost-based pruning of AnytimeAStar without an initial upper bound for pruning. */
    /* The TopDown GOO heuristic is not admissible, therefore only the g-values of the states are allowed to be
     * considered for pruning.
     * Besides testing the correct funcionality of cost-based pruning the following sections test:
     * - pruning due to a reduction of the upper bound for pruning
     */

    SECTION("TopDown_GOO_budget_4")
    {
        /* Run heuristic search. */
        using H = heuristics::GOO<PlanTable, State, expansions::TopDownComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::TopDownComplete, H, config::anytimeAStar_with_cbp,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 4;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::TopDownComplete,
                                              SearchAlgorithm,
                                              heuristics::GOO,
                                              config::anytimeAStar_with_cbp
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        expected.update(G, db.cardinality_estimator(), C_out, R2, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0 , R2|R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1 , R0|R2|R3 , condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 20700);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 5700);

        /* Not all of the dependencies between the search state counters hold true for cost-based pruning. */

        /* Configuration-specific assertions. */
        /* Leads to cost-based pruning of the child states {{R0,R1},{R2},{R3}} and {{R0,R3},{R1},{R2}} of
         * {{R0,R1,R3},{2}} since both of them have a g-value of 5800. In the previous iteration of the algorithm the
         * goal state with the f-value of 5700 was added, leading to their pruning. */
        CHECK(State::NUM_STATES_GENERATED() == 11);
        CHECK(State::NUM_STATES_EXPANDED() == 4);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 12);
        CHECK(State::NUM_STATES_DISPOSED() == 2);

        const auto &SM = S.state_manager();

        CHECK(SM.num_new() == 10);
        CHECK(SM.num_duplicates() == 0);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 2);

        CHECK(SM.num_states_seen() == 10);
        CHECK(SM.num_states_in_regular_queue() == 6);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 1);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }

    SECTION("TopDown_GOO_budget_7")
    {
        /* Run heuristic search. */
        using H = heuristics::GOO<PlanTable, State, expansions::TopDownComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::TopDownComplete, H, config::anytimeAStar_with_cbp,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 7;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::TopDownComplete,
                                              SearchAlgorithm,
                                              heuristics::GOO,
                                              config::anytimeAStar_with_cbp
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        expected.update(G, db.cardinality_estimator(), C_out, R2, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1 , R2|R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0 , R1|R2|R3 , condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 19900);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 4900);

        /* Configuration-specific assertions. */
        /* In the previous iteration of the algorithm the goal state with the f-value of 4900 was added. Thus, the
         * threshold for cost-based pruning is decreased to 4900. During the 7th expansion the child states
         * {{R0,R1},{R2},{R3}} and {{R1,R2},{R0},{R3}} of {{R0,R1,R2},{3}} since both of them have a g-value of 5000.
         * */
        CHECK(State::NUM_STATES_GENERATED() == 16);
        CHECK(State::NUM_STATES_EXPANDED() == 7);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 17);
        CHECK(State::NUM_STATES_DISPOSED() == 6);

        const auto &SM = S.state_manager();

        CHECK(SM.num_new() == 11);
        CHECK(SM.num_duplicates() == 2);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 2);
        CHECK(SM.num_decrease_key() == 1);
        CHECK(SM.num_pruned_by_cost() == 4);

        CHECK(SM.num_states_seen() == 11);
        CHECK(SM.num_states_in_regular_queue() == 5);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 4);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }

    SECTION("TopDown_GOO_budget_9")
    {
        /* Run heuristic search. */
        using H = heuristics::GOO<PlanTable, State, expansions::TopDownComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::TopDownComplete, H, config::anytimeAStar_with_cbp,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 9;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::TopDownComplete,
                                              SearchAlgorithm,
                                              heuristics::GOO,
                                              config::anytimeAStar_with_cbp
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        expected.update(G, db.cardinality_estimator(), C_out, R0, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R2 , R0|R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1 , R0|R2|R3 , condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 17600);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 2600);

        /* Configuration-specific assertions. */
        /* No more cost-based pruning should occur. The search should "expand" the goal state in the 9th expansion. */
        CHECK(State::NUM_STATES_GENERATED() == 17);
        CHECK(State::NUM_STATES_EXPANDED() == 8);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 18);
        CHECK(State::NUM_STATES_DISPOSED() == 7);

        const auto &SM = S.state_manager();

        CHECK(SM.num_new() == 11);
        CHECK(SM.num_duplicates() == 3);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 3);
        CHECK(SM.num_decrease_key() == 2);
        CHECK(SM.num_pruned_by_cost() == 4);

        CHECK(SM.num_states_seen() == 11);
        CHECK(SM.num_states_in_regular_queue() == 3);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 5);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }

    SECTION("TopDown_weighted_2_GOO_budget_max")
    {
        /* Run heuristic search. */
        using H = heuristics::GOO<PlanTable, State, expansions::TopDownComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::TopDownComplete, H, config::weighted_anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());
        uint64_t budget = std::numeric_limits<uint64_t>::max();

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::TopDownComplete,
                                              SearchAlgorithm,
                                              heuristics::GOO,
                                              config::weighted_anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .weighting_factor = 2.f,
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        /* The weighted search leads to a differet plan than the unweighted version. */
        expected.update(G, db.cardinality_estimator(), C_out, R2, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R0, R2|R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1, R0|R2|R3 , condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 20700);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 5700);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 9);
        CHECK(State::NUM_STATES_EXPANDED() == 3);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 10);
        CHECK(State::NUM_STATES_DISPOSED() == 0);

        const auto &SM = S.state_manager();

        CHECK(SM.num_new() == 10);
        CHECK(SM.num_duplicates() == 0);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 10);
        CHECK(SM.num_states_in_regular_queue() == 6);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }

}

TEST_CASE("AnytimeAStar_BottomUp_and_TopDown_initial_upper_bound", "[core][IR]")
{
    using Subproblem = SmallBitset;
    using PlanTable = PlanTableSmallOrDense;

    /* Get Catalog and create new database to use for unit testing. */
    Catalog::Clear();
    Catalog &Cat = Catalog::Get();
    auto &db = Cat.add_database("db");
    Cat.set_database_in_use(db);

    /* Create pooled strings. */
    const char *str_R0 = Cat.pool("R0");
    const char *str_R1 = Cat.pool("R1");
    const char *str_R2 = Cat.pool("R2");
    const char *str_R3 = Cat.pool("R3");

    const char *col_id = Cat.pool("id");
    const char *col_fid_R1 = Cat.pool("fid_R1");
    const char *col_fid_R2 = Cat.pool("fid_R2");
    const char *col_fid_R3 = Cat.pool("fid_R3");

    /* Create tables. */
    Table &tbl_R0 = db.add_table(str_R0);
    Table &tbl_R1 = db.add_table(str_R1);
    Table &tbl_R2 = db.add_table(str_R2);
    Table &tbl_R3 = db.add_table(str_R3);

    /* Add columns to tables. */
    tbl_R0.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R3.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R1.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R2.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));

    tbl_R0.push_back(col_fid_R1, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R0.push_back(col_fid_R3, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R1.push_back(col_fid_R2, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R2.push_back(col_fid_R3, Type::Get_Integer(Type::TY_Vector, 4));

    /* Add data to tables. */
    std::size_t num_rows_R0 = 1450;
    std::size_t num_rows_R1 = 2000;
    std::size_t num_rows_R2 = 940;
    std::size_t num_rows_R3 = 500;
    tbl_R0.store(Cat.create_store(tbl_R0));
    tbl_R1.store(Cat.create_store(tbl_R1));
    tbl_R2.store(Cat.create_store(tbl_R2));
    tbl_R3.store(Cat.create_store(tbl_R3));
    tbl_R0.layout(Cat.data_layout());
    tbl_R1.layout(Cat.data_layout());
    tbl_R2.layout(Cat.data_layout());
    tbl_R3.layout(Cat.data_layout());

    for (std::size_t i = 0; i < num_rows_R0; ++i) { tbl_R0.store().append(); }
    for (std::size_t i = 0; i < num_rows_R1; ++i) { tbl_R1.store().append(); }
    for (std::size_t i = 0; i < num_rows_R2; ++i) { tbl_R2.store().append(); }
    for (std::size_t i = 0; i < num_rows_R3; ++i) { tbl_R3.store().append(); }

    /* Define query: cycle query
     *
     *    R0
     *   /  \
     *  R3  R1
     *   \  /
     *    R2
     */

    const std::string query = " SELECT * \
                                FROM R0, R1, R2, R3 \
                                WHERE R0.fid_R1 = R1.id \
                                  AND R0.fid_R3 = R3.id \
                                  AND R1.fid_R2 = R2.id \
                                  AND R2.fid_R3 = R3.id;";

    Diagnostic diag(false, std::cout, std::cerr); // What is diagnostic?
    auto stmt = m::statement_from_string(diag, query); // TODO: use command_from_string
    REQUIRE(not diag.num_errors());
    auto query_graph = QueryGraph::Build(*stmt);
    auto &G = *query_graph.get();

    /* Create subproblems to manually add entries in the `PlanTable`. */
    const Subproblem R0(1);
    const Subproblem R1(2);
    const Subproblem R2(4);
    const Subproblem R3(8);

    /* Initialize the `InjectionCardinalityEstimator`. */
    std::istringstream json_input;
    json_input.str("{ \"mine\": [ \
                   {\"relations\": [\"R0\"], \"size\":1450}, \
                   {\"relations\": [\"R1\"], \"size\":2000}, \
                   {\"relations\": [\"R2\"], \"size\":940}, \
                   {\"relations\": [\"R3\"], \"size\":500}, \
                   {\"relations\": [\"R0\", \"R1\"], \"size\":40}, \
                   {\"relations\": [\"R0\", \"R3\"], \"size\":50000}, \
                   {\"relations\": [\"R1\", \"R2\"], \"size\":700}, \
                   {\"relations\": [\"R2\", \"R3\"], \"size\":3200}, \
                   {\"relations\": [\"R0\", \"R1\", \"R3\"], \"size\":5800}, \
                   {\"relations\": [\"R0\", \"R1\", \"R2\"], \"size\":5000}, \
                   {\"relations\": [\"R0\", \"R2\", \"R3\"], \"size\":2500}, \
                   {\"relations\": [\"R1\", \"R2\", \"R3\"], \"size\":1700}, \
                   {\"relations\": [\"R0\", \"R1\", \"R2\", \"R3\"], \"size\":15000} \
                   ]}");

    auto ICE = std::make_unique<InjectionCardinalityEstimator>(diag, "mine", json_input);
    db.cardinality_estimator(std::move(ICE));

    /* Initialize `PlanTable` for base case. */
    PlanTable plan_table(G);
    init_PT_base_case(G, plan_table, db.cardinality_estimator());


    /* Get costfunction Cout, adjacency matrix and join condition. */
    CostFunctionCout C_out;
    const AdjacencyMatrix &M = G.adjacency_matrix();
    static cnf::CNF condition; // TODO use join condition

    /* Heuristic search configurations. */
    using State = search_states::SubproblemsArray;


    /* Initialization of the upper bound for cost-based pruning via BottomUp GOO. */
    double upper_bound = [&]() {
        /*----- Run GOO to compute upper bound of plan cost. -----*/
        GOO Goo;
        Goo(G, C_out, plan_table);
        return plan_table.get_final().cost;
    }();


    /* TopDown sum variant for testing cost-based pruning of AnytimeAStar with an initial upper bound for pruning. */
    /* The TopDown sum heuristic is admissible, therefore the f-values of the states are considered for pruning. Here
     * the pruning only occurs if the f-value is considered. If only the g-value would be considered, the respective
     * states would not be pruned. Only the case for budget 1 is tested, as with a higher budget no further pruning
     * would occur. The upper bound is not decreased the TopDown sum heuristic since the expansion of a state with a
     * goal state leads to the goal state having the same f-value as the expanded state. Thus, as soon as a goal state
     * is found during the search, it is expanded in the next iteration. Thereby, a decrease of the upper bound only
     * occurs if it was initialized before the search and the decreased upper bound is not put into effect for pruning.
     * The same holds for the BottomUp zero and sum heuristics. */

    SECTION("TopDown_sum_budget_1")
    {
        /* Run heuristic search. */
        using H = heuristics::sum<PlanTable, State, expansions::TopDownComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::TopDownComplete, H, config::anytimeAStar_with_cbp,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 1;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::TopDownComplete,
                                              SearchAlgorithm,
                                              heuristics::sum,
                                              config::anytimeAStar_with_cbp
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .upper_bound = upper_bound,
                                                .expansion_budget = budget,
                                                });

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /** Cannot match a PlanTable with an expected plan with the PlanTable of the search. This is because the initial
         * run of GOO to determine the inital upper bound for cost-based pruning, leads to a partially filled PlanTable.
         */

        /* Configuration-specific assertions. */
        /* Leads to cost-based pruning of the state {{R0,R3},{R1,R2}} since it has an f-value of 50700, consisting of a
         * g-value of 0 and and h-value of 50700. The initial upper bound is 18240, based on the cost of a BottomUp GOO
         * run from the initial state. Based on the f-value the state {{R0,R3},{R1,R2}} is pruned during the first
         * expansion of the initial state. */
        CHECK(State::NUM_STATES_GENERATED() == 6);
        CHECK(State::NUM_STATES_EXPANDED() == 1);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 7);
        CHECK(State::NUM_STATES_DISPOSED() == 1);

        const auto &SM = S.state_manager();

        CHECK(SM.num_new() == 6);
        CHECK(SM.num_duplicates() == 0);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 1);

        CHECK(SM.num_states_seen() == 6);
        CHECK(SM.num_states_in_regular_queue() == 5);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }

    SECTION("TopDown_weighted_4_sum_budget_1")
    {
        /* Run heuristic search. */
        using H = heuristics::sum<PlanTable, State, expansions::TopDownComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::TopDownComplete, H, config::weighted_anytimeAStar_with_cbp,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 1;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::TopDownComplete,
                                              SearchAlgorithm,
                                              heuristics::sum,
                                              config::weighted_anytimeAStar_with_cbp
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .upper_bound = upper_bound,
                                                .weighting_factor = 4.f,
                                                .expansion_budget = budget
                                                });

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /** Cannot match a PlanTable with an expected plan with the PlanTable of the search. This is because the initial
         * run of GOO to determine the inital upper bound for cost-based pruning, leads to a partially filled PlanTable.
         */

        /* Configuration-specific assertions. */
        /* Leads to cost-based pruning of the state {{R0,R3},{R1,R2}}, since it has an unweighted f-value of
         * 50700.  The initial upper bound is 18240, based on the cost of a BottomUp GOO run from the initial state.
         * The states {{R0,R1,R2},{R3}}, {{R0,R1,R3},{R2}} have weighted f-values of 20000 and 23200, but unweighted
         * f-values/h-values of 5000 and 5800.  Since only the unweighted f-values are allowed to be considered for
         * pruning, they are not allowed to be pruned.  Based on the f-value, the state {{R0,R3},{R1,R2}} is pruned
         * during the first expansion of the initial state. */
        CHECK(State::NUM_STATES_GENERATED() == 6);
        CHECK(State::NUM_STATES_EXPANDED() == 1);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 7);
        CHECK(State::NUM_STATES_DISPOSED() == 1);

        const auto &SM = S.state_manager();

        CHECK(SM.num_new() == 6);
        CHECK(SM.num_duplicates() == 0);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 1);

        CHECK(SM.num_states_seen() == 6);
        CHECK(SM.num_states_in_regular_queue() == 5);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }

    /* BottomUp zero variant for testing cost-based pruning of AnytimeAStar with an initial upper bound for pruning. */
    /* The BottomUp zero heuristic is admissible, therefore the f-values of the states are considered for pruning. But
     * since the g-value is equal to the f-value for the zero heuristic, this cannot be explicitly tested here. As no
     * other BottomUp heuristic is admissible, the g-value is considered for all heuristics in the BottomUp case. Here,
     * the zero heuristic is chosen as an example. As mentioned before, due to the construction of BottomUp zero and
     * Bottomup GOO, a decrease of the initial upper bound during the search does not become effective for pruning.
     * Therefore, only the initial upper bound is used for pruning states. */

    SECTION("BottomUp_zero_budget_1")
    {
        /* Run heuristic search. */
        using H = heuristics::zero<PlanTable, State, expansions::BottomUpComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::BottomUpComplete, H, config::anytimeAStar_with_cbp,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 1;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::BottomUpComplete,
                                              SearchAlgorithm,
                                              heuristics::zero,
                                              config::anytimeAStar_with_cbp
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .upper_bound = upper_bound,
                                                .expansion_budget = budget
                                                });

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Configuration-specific assertions. */
        /* Leads to cost-based pruning of the state {{R0,R3},{R1},{R2}} since it has an f-value of 50000, consisting of
         * a g-value of 50000 and and h-value of 0. The initial upper bound is 18240, based on the cost of a BottomUp
         * GOO run from the initial state. Based on the f-value the state {{R0,R3},{R1},{R2}} is pruned during the first
         * expansion of the initial state. */
        CHECK(State::NUM_STATES_GENERATED() == 4);
        CHECK(State::NUM_STATES_EXPANDED() == 1);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 5);
        CHECK(State::NUM_STATES_DISPOSED() == 1);

        const auto &SM = S.state_manager();

        CHECK(SM.num_new() == 4);
        CHECK(SM.num_duplicates() == 0);
        CHECK(SM.num_discarded() == 0);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 1);

        CHECK(SM.num_states_seen() == 4);
        CHECK(SM.num_states_in_regular_queue() == 3);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 0);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }

    SECTION("BottomUp_zero_budget_max")
    {
        /* Run heuristic search. */
        using H = heuristics::zero<PlanTable, State, expansions::BottomUpComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::BottomUpComplete, H, config::anytimeAStar_with_cbp,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = std::numeric_limits<uint64_t>::max();

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::BottomUpComplete,
                                              SearchAlgorithm,
                                              heuristics::zero,
                                              config::anytimeAStar_with_cbp
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .upper_bound = upper_bound,
                                                .expansion_budget = budget
                                                });

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Configuration-specific assertions. */
        /* Leads to cost-based pruning of the state {{R0,R3},{R1,R2}} since it has an f-value of 50700, consisting of
         * a g-value of 50700 and and h-value of 0. The initial upper bound is 18240, based on the cost of a BottomUp
         * GOO run from the initial state. Based on the f-value, the state {{R0,R3},{R1,R2}} is pruned during the first
         * expansion of the initial state. */
        CHECK(State::NUM_STATES_GENERATED() == 11);
        CHECK(State::NUM_STATES_EXPANDED() == 4);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 12);
        CHECK(State::NUM_STATES_DISPOSED() == 3);

        const auto &SM = S.state_manager();

        CHECK(SM.num_new() == 9);
        CHECK(SM.num_duplicates() == 1);
        CHECK(SM.num_discarded() == 1);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 2);

        CHECK(SM.num_states_seen() == 9);
        CHECK(SM.num_states_in_regular_queue() == 4);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 1);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }

}

TEST_CASE("AnytimeAStar_weighted_path_completion", "[core][IR]")
{
    using Subproblem = SmallBitset;
    using PlanTable = PlanTableSmallOrDense;

    /* Get Catalog and create new database to use for unit testing. */
    Catalog::Clear();
    Catalog &Cat = Catalog::Get();
    auto &db = Cat.add_database("db");
    Cat.set_database_in_use(db);

    /* Create pooled strings. */
    const char *str_R0 = Cat.pool("R0");
    const char *str_R1 = Cat.pool("R1");
    const char *str_R2 = Cat.pool("R2");
    const char *str_R3 = Cat.pool("R3");

    const char *col_id = Cat.pool("id");
    const char *col_fid_R1 = Cat.pool("fid_R1");
    const char *col_fid_R2 = Cat.pool("fid_R2");
    const char *col_fid_R3 = Cat.pool("fid_R3");

    /* Create tables. */
    Table &tbl_R0 = db.add_table(str_R0);
    Table &tbl_R1 = db.add_table(str_R1);
    Table &tbl_R2 = db.add_table(str_R2);
    Table &tbl_R3 = db.add_table(str_R3);

    /* Add columns to tables. */
    tbl_R0.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R3.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R1.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R2.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));

    tbl_R0.push_back(col_fid_R1, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R0.push_back(col_fid_R3, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R1.push_back(col_fid_R2, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_R2.push_back(col_fid_R3, Type::Get_Integer(Type::TY_Vector, 4));

    /* Add data to tables. */
    std::size_t num_rows_R0 = 100;
    std::size_t num_rows_R1 = 100;
    std::size_t num_rows_R2 = 100;
    std::size_t num_rows_R3 = 100;
    tbl_R0.store(Cat.create_store(tbl_R0));
    tbl_R1.store(Cat.create_store(tbl_R1));
    tbl_R2.store(Cat.create_store(tbl_R2));
    tbl_R3.store(Cat.create_store(tbl_R3));
    tbl_R0.layout(Cat.data_layout());
    tbl_R1.layout(Cat.data_layout());
    tbl_R2.layout(Cat.data_layout());
    tbl_R3.layout(Cat.data_layout());

    for (std::size_t i = 0; i < num_rows_R0; ++i) { tbl_R0.store().append(); }
    for (std::size_t i = 0; i < num_rows_R1; ++i) { tbl_R1.store().append(); }
    for (std::size_t i = 0; i < num_rows_R2; ++i) { tbl_R2.store().append(); }
    for (std::size_t i = 0; i < num_rows_R3; ++i) { tbl_R3.store().append(); }

    /* Define query: cycle query
     *
     *    R0
     *   /  \
     *  R3  R1
     *   \  /
     *    R2
     */

    const std::string query = " SELECT * \
                                FROM R0, R1, R2, R3 \
                                WHERE R0.fid_R1 = R1.id \
                                  AND R0.fid_R3 = R3.id \
                                  AND R1.fid_R2 = R2.id \
                                  AND R2.fid_R3 = R3.id;";

    Diagnostic diag(false, std::cout, std::cerr); // What is diagnostic?
    auto stmt = m::statement_from_string(diag, query); // TODO: use command_from_string
    REQUIRE(not diag.num_errors());
    auto query_graph = QueryGraph::Build(*stmt);
    auto &G = *query_graph.get();

    /* Create subproblems to manually add entries in the `PlanTable`. */
    const Subproblem R0(1);
    const Subproblem R1(2);
    const Subproblem R2(4);
    const Subproblem R3(8);

    /* Initialize the `InjectionCardinalityEstimator`. */
    std::istringstream json_input;
    json_input.str("{ \"mine\": [ \
                   {\"relations\": [\"R0\"], \"size\":100}, \
                   {\"relations\": [\"R1\"], \"size\":100}, \
                   {\"relations\": [\"R2\"], \"size\":100}, \
                   {\"relations\": [\"R3\"], \"size\":100}, \
                   {\"relations\": [\"R0\", \"R1\"], \"size\":240}, \
                   {\"relations\": [\"R0\", \"R3\"], \"size\":290}, \
                   {\"relations\": [\"R1\", \"R2\"], \"size\":250}, \
                   {\"relations\": [\"R2\", \"R3\"], \"size\":3200}, \
                   {\"relations\": [\"R0\", \"R1\", \"R3\"], \"size\":400}, \
                   {\"relations\": [\"R0\", \"R1\", \"R2\"], \"size\":50000}, \
                   {\"relations\": [\"R0\", \"R2\", \"R3\"], \"size\":380}, \
                   {\"relations\": [\"R1\", \"R2\", \"R3\"], \"size\":700}, \
                   {\"relations\": [\"R0\", \"R1\", \"R2\", \"R3\"], \"size\":15000} \
                   ]}");

    auto ICE = std::make_unique<InjectionCardinalityEstimator>(diag, "mine", json_input);
    db.cardinality_estimator(std::move(ICE));

    /* Initialize `PlanTable` for base case. */
    PlanTable plan_table(G);
    init_PT_base_case(G, plan_table, db.cardinality_estimator());

    PlanTable expected(G);
    init_PT_base_case(G, expected, db.cardinality_estimator());

    /* Get `SmallBitset` respresentation of the goal state for testing the costs. */
    const Subproblem All = Subproblem::All(G.num_sources());

    /* Get costfunction Cout, adjacency matrix and join condition. */
    CostFunctionCout C_out;
    const AdjacencyMatrix &M = G.adjacency_matrix();
    static cnf::CNF condition; // TODO use join condition

    /* Heuristic search configurations. */
    using State = search_states::SubproblemsArray;

    SECTION("BottomUp_weighed_20_sum_budget_max")
    {
        /* Run heuristic search. */
        using H = heuristics::sum<PlanTable, State, expansions::BottomUpComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::BottomUpComplete, H, config::weighted_anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = std::numeric_limits<uint64_t>::max();

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::BottomUpComplete,
                                              SearchAlgorithm,
                                              heuristics::sum,
                                              config::weighted_anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .weighting_factor = 20.f,
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        /* This is to ensure that weighted search results in a plan different from the unweighted version. */
        expected.update(G, db.cardinality_estimator(), C_out, R0, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R2 , R0|R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1, R0|R2|R3 , condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15670);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 670);

        /* Dependencies between the search state counters. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 13);
        CHECK(State::NUM_STATES_EXPANDED() == 5);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 14);
        CHECK(State::NUM_STATES_DISPOSED() == 2);

        CHECK(SM.num_new() == 12);
        CHECK(SM.num_duplicates() == 2);
        CHECK(SM.num_discarded() == 2);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 12);
        CHECK(SM.num_states_in_regular_queue() == 6);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 2);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }

    SECTION("BottomUp_sum_budget_4")
    {
        /* Run heuristic search. */
        using H = heuristics::sum<PlanTable, State, expansions::BottomUpComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::BottomUpComplete, H, config::anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 4;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::BottomUpComplete,
                                              SearchAlgorithm,
                                              heuristics::sum,
                                              config::anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        /* This is to ensure that the GOO path completion of the weighted search results in the same starting
         * point/plan. */
        expected.update(G, db.cardinality_estimator(), C_out, R0, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1 , R2, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1|R2, R0|R3 , condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15540);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 540);

        /* Dependencies between the search state counters. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 12);
        CHECK(State::NUM_STATES_EXPANDED() == 4);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 13);
        CHECK(State::NUM_STATES_DISPOSED() == 2);

        CHECK(SM.num_new() == 11);
        CHECK(SM.num_duplicates() == 2);
        CHECK(SM.num_discarded() == 2);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 11);
        CHECK(SM.num_states_in_regular_queue() == 7);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 2);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }

    SECTION("BottomUp_weighed_20_sum_budget_4")
    {
        /* Run heuristic search. */
        using H = heuristics::sum<PlanTable, State, expansions::BottomUpComplete>;

        using SearchAlgorithm = ai::genericAStar<
            State, expansions::BottomUpComplete, H, config::weighted_anytimeAStar,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >;

        SearchAlgorithm S(plan_table, G, M, C_out, db.cardinality_estimator());

        uint64_t budget = 4;

        bool search_result = heuristic_search<PlanTable,
                                              search_states::SubproblemsArray,
                                              expansions::BottomUpComplete,
                                              SearchAlgorithm,
                                              heuristics::sum,
                                              config::weighted_anytimeAStar
                                              >(plan_table, G, M, C_out, db.cardinality_estimator(), S,
                                              {
                                                .weighting_factor = 20.f,
                                                .expansion_budget = budget
                                                });

        /* Fill `expected` with the anticipated plan. */
        /* After 4 expansions the state with the lowest weighted f-value among the states closest to the goal is state
         * X={{R0,R2,R3},{R1}} with f´(X)=g(X)+w*h(X)=670+20*480=10270. The state Y={{R0,R3},{R1,R2}} has a weighted
         * f-value of f´(Y)=g(Y)+w*h(Y)=540+20*540=11340. For chosing the starting state of GOO path completion, the
         * unweighted f-value should be considered. Among the states closes to the goal Y is the state with the lowest
         * unweighted f-value, with f(Y)=1080 and f(X)=1150. Therefore Y should be chosen as the starting state of GOO
         * path completion. */
        expected.update(G, db.cardinality_estimator(), C_out, R0, R3, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1 , R2, condition);
        expected.update(G, db.cardinality_estimator(), C_out, R1|R2, R0|R3 , condition);

        /* Check for successful run of the search */
        CHECK(search_result == true);

        /* Check for the result of the search to match the expected plan */
        CHECK(expected == plan_table);

        /* Check for the correct costs of the path */
        CHECK(expected[All].cost == plan_table[All].cost);
        CHECK(plan_table[All].cost == 15540);
        CHECK(plan_table[plan_table[All].left].cost + plan_table[plan_table[All].right].cost== 540);

        /* Dependencies between the search state counters. */
        const auto &SM = S.state_manager();
        check_state_counter_dependencies<State, SearchAlgorithm, decltype(SM)>(S, SM, budget);

        /* Configuration-specific assertions. */
        CHECK(State::NUM_STATES_GENERATED() == 12);
        CHECK(State::NUM_STATES_EXPANDED() == 4);
        CHECK(State::NUM_STATES_CONSTRUCTED() == 13);
        CHECK(State::NUM_STATES_DISPOSED() == 2);

        CHECK(SM.num_new() == 11);
        CHECK(SM.num_duplicates() == 2);
        CHECK(SM.num_discarded() == 2);
        CHECK(SM.num_cheaper() == 0);
        CHECK(SM.num_decrease_key() == 0);
        CHECK(SM.num_pruned_by_cost() == 0);

        CHECK(SM.num_states_seen() == 11);
        CHECK(SM.num_states_in_regular_queue() == 7);
        CHECK(SM.num_states_in_beam_queue() == 0);
        CHECK(S.num_cached_heuristic_value() == 2);
        CHECK(SM.num_regular_to_beam() == 0);
        CHECK(SM.num_none_to_beam() == 0);
    }

}
