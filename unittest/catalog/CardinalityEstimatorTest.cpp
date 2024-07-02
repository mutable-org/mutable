#include "catch2/catch.hpp"

#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include <cstring>
#include <mutable/catalog/CardinalityEstimator.hpp>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/mutable.hpp>
#include <mutable/util/ADT.hpp>
#include <sstream>


using namespace m;
using namespace m::ast;


TEST_CASE("Injection estimator estimates", "[core][catalog][cardinality]")
{
    using Subproblem = SmallBitset;
    /* Get Catalog and create new database to use for unit testing. */
    Catalog::Clear();
    Catalog &Cat = Catalog::Get();
    auto &db = Cat.add_database(Cat.pool("db"));
    Cat.set_database_in_use(db);

    std::ostringstream out, err;
    Diagnostic diag(false, out, err);

    /* Create pooled strings. */
    ThreadSafePooledString str_A = Cat.pool("A");
    ThreadSafePooledString str_B = Cat.pool("B");
    ThreadSafePooledString str_C = Cat.pool("C");

    ThreadSafePooledString col_id  = Cat.pool("id");
    ThreadSafePooledString col_aid = Cat.pool("aid");

    /* Create tables. */
    Table &tbl_A = db.add_table(str_A);
    Table &tbl_B = db.add_table(str_B);
    Table &tbl_C = db.add_table(str_C);

    /* Add columns to tables. */
    tbl_A.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_B.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_B.push_back(col_aid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_C.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_C.push_back(col_aid, Type::Get_Integer(Type::TY_Vector, 4));

    /* Add data to tables. */
    std::size_t num_rows_A = 5;
    std::size_t num_rows_B = 10;
    std::size_t num_rows_C = 8;
    tbl_A.store(Cat.create_store(tbl_A));
    tbl_B.store(Cat.create_store(tbl_B));
    tbl_C.store(Cat.create_store(tbl_C));
    tbl_A.layout(Cat.data_layout());
    tbl_B.layout(Cat.data_layout());
    tbl_C.layout(Cat.data_layout());
    for (std::size_t i = 0; i < num_rows_A; ++i) { tbl_A.store().append(); }
    for (std::size_t i = 0; i < num_rows_B; ++i) { tbl_B.store().append(); }
    for (std::size_t i = 0; i < num_rows_C; ++i) { tbl_C.store().append(); }

    /* Define query:
     *
     * A -- B -- C
     */
    const char *query = "SELECT * \
                         FROM A, B, C \
                         WHERE A.id = C.aid AND A.id = B.aid;";
    auto S = m::statement_from_string(diag, query);
    M_insist(diag.num_errors() == 0);
    auto G = QueryGraph::Build(*S);
    std::istringstream json_input;
    json_input.str("{ \"mine\": [ \
                   {\"relations\": [\"A\"], \"size\":500}, \
                   {\"relations\": [\"A\", \"B\"], \"size\":1000} \
                   ]}");
    InjectionCardinalityEstimator ICE(diag, Cat.pool("mine"), json_input);

    //Always check if "A" == 500 because it is in the input_json
    //Always check if "B" == 10 because it is not in the input_json

    SECTION("estimate_scan")
    {
        auto existing_entry_model = ICE.estimate_scan(*G, Subproblem(1UL));
        CHECK(ICE.predict_cardinality(*existing_entry_model) == 500);
        auto non_existing_entry_model = ICE.estimate_scan(*G, Subproblem::Singleton(1));
        CHECK(ICE.predict_cardinality(*non_existing_entry_model) == 10);
    }

    SECTION("estimate_filter")
    {
        cnf::CNF filter;

        auto existing_entry_model = ICE.estimate_scan(*G, Subproblem(1UL));
        auto filter_existing_entry_model = ICE.estimate_filter(*G, *existing_entry_model, filter);
        CHECK(ICE.predict_cardinality(*filter_existing_entry_model) == 500);

        auto non_existing_entry_model = ICE.estimate_scan(*G, Subproblem::Singleton(1));
        auto filter_non_existing_entry_model = ICE.estimate_filter(*G, *non_existing_entry_model, filter);
        CHECK(ICE.predict_cardinality(*filter_non_existing_entry_model) == 10);
    }

    SECTION("estimate_limit")
    {
        auto existing_entry_model = ICE.estimate_scan(*G, Subproblem(1UL));
        auto non_existing_entry_model = ICE.estimate_scan(*G, Subproblem::Singleton(1));
        auto limit_existing_entry_model_high = ICE.estimate_limit(*G, *existing_entry_model, 5000, 0);
        auto limit_non_existing_entry_model_high = ICE.estimate_limit(*G, *non_existing_entry_model, 5000, 0);
        CHECK(ICE.predict_cardinality(*limit_existing_entry_model_high) == 500);
        CHECK(ICE.predict_cardinality(*limit_non_existing_entry_model_high) == 10);

        auto limit_existing_entry_model_low = ICE.estimate_limit(*G, *existing_entry_model, 8, 0);
        auto limit_non_existing_entry_model_low = ICE.estimate_limit(*G, *non_existing_entry_model, 8, 0);
        CHECK(ICE.predict_cardinality(*limit_existing_entry_model_low) == 8);
        CHECK(ICE.predict_cardinality(*limit_non_existing_entry_model_low) == 8);
    }

    SECTION("estimate_grouping (empty)")
    {
        auto existing_entry_model = ICE.estimate_scan(*G, Subproblem(1UL));
        auto non_existing_entry_model = ICE.estimate_scan(*G, Subproblem::Singleton(1));
        std::vector<QueryGraph::group_type> group_by;
        auto grouping_existing_entry_model = ICE.estimate_grouping(*G, *existing_entry_model, group_by);
        auto grouping_non_existing_entry_model = ICE.estimate_grouping(*G, *non_existing_entry_model, group_by);
        CHECK(ICE.predict_cardinality(*grouping_existing_entry_model) == 1);
        CHECK(ICE.predict_cardinality(*grouping_non_existing_entry_model) == 1);
    }

    SECTION("estimate_join")
    {
        auto existing_entry_model_one = ICE.estimate_scan(*G, Subproblem(1UL));
        auto non_existing_entry_model_two = ICE.estimate_scan(*G, Subproblem::Singleton(1));
        auto non_existing_entry_model_three = ICE.estimate_scan(*G, Subproblem::Singleton(2));
        cnf::CNF condition;
        auto existing_entry_model_join = ICE.estimate_join(*G, *existing_entry_model_one, *non_existing_entry_model_two,
                                                      condition);
        auto non_existing_entry_model_join = ICE.estimate_join(*G, *existing_entry_model_one,
                                                               *non_existing_entry_model_three, condition);
        CHECK(ICE.predict_cardinality(*existing_entry_model_join) == 1000);
        CHECK(ICE.predict_cardinality(*non_existing_entry_model_join) == 4000);
    }

    SECTION("wrong database, return cartesian")
    {
        std::istringstream json_input_wrong_db;
        json_input_wrong_db.str("{ \"mine\": [{\"relations\": [\"A\", \"B\"], \"size\":1000}]}");
        InjectionCardinalityEstimator ice_wrong_db(diag, Cat.pool("yours"), json_input_wrong_db);

        auto non_existing_entry_model_one = ice_wrong_db.estimate_scan(*G, Subproblem::Singleton(0));
        auto non_existing_entry_model_two = ice_wrong_db.estimate_scan(*G, Subproblem::Singleton(1));
        cnf::CNF condition;
        auto non_existing_entry_model_join = ice_wrong_db.estimate_join(*G, *non_existing_entry_model_one,
                                                                        *non_existing_entry_model_two, condition);
        CHECK(ice_wrong_db.predict_cardinality(*non_existing_entry_model_one) == 5);
        CHECK(ice_wrong_db.predict_cardinality(*non_existing_entry_model_two) == 10);
        CHECK(ice_wrong_db.predict_cardinality(*non_existing_entry_model_join) == 50);
    }

}

TEST_CASE("Cartesian estimator estimates", "[core][catalog][cardinality]")
{
    using Subproblem = SmallBitset;
    /* Get Catalog and create new database to use for unit testing. */
    Catalog::Clear();
    Catalog &Cat = Catalog::Get();
    auto &db = Cat.add_database(Cat.pool("db"));
    Cat.set_database_in_use(db);

    std::ostringstream out, err;
    Diagnostic diag(false, out, err);

    /* Create pooled strings. */
    ThreadSafePooledString str_A = Cat.pool("A");
    ThreadSafePooledString str_B = Cat.pool("B");
    ThreadSafePooledString str_C = Cat.pool("C");

    ThreadSafePooledString col_id  = Cat.pool("id");
    ThreadSafePooledString col_aid = Cat.pool("aid");

    /* Create tables. */
    Table &tbl_A = db.add_table(str_A);
    Table &tbl_B = db.add_table(str_B);
    Table &tbl_C = db.add_table(str_C);

    /* Add columns to tables. */
    tbl_A.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_B.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_B.push_back(col_aid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_C.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_C.push_back(col_aid, Type::Get_Integer(Type::TY_Vector, 4));

    /* Add data to tables. */
    std::size_t num_rows_A = 5;
    std::size_t num_rows_B = 10;
    std::size_t num_rows_C = 8;
    tbl_A.store(Cat.create_store(tbl_A));
    tbl_B.store(Cat.create_store(tbl_B));
    tbl_C.store(Cat.create_store(tbl_C));
    tbl_A.layout(Cat.data_layout());
    tbl_B.layout(Cat.data_layout());
    tbl_C.layout(Cat.data_layout());
    for (std::size_t i = 0; i < num_rows_A; ++i) { tbl_A.store().append(); }
    for (std::size_t i = 0; i < num_rows_B; ++i) { tbl_B.store().append(); }
    for (std::size_t i = 0; i < num_rows_C; ++i) { tbl_C.store().append(); }

    /* Define query:
     *
     * A -- B -- C
     */
    const char *query = "SELECT * \
                         FROM A, B, C \
                         WHERE A.id = C.aid AND A.id = B.aid;";
    auto S = m::statement_from_string(diag, query);
    M_insist(diag.num_errors() == 0);
    auto G = QueryGraph::Build(*S);
    CartesianProductEstimator CE;

    SECTION("estimate_scan")
    {
        auto scan_model_one = CE.estimate_scan(*G, Subproblem::Singleton(0));
        auto scan_model_two = CE.estimate_scan(*G, Subproblem::Singleton(1));
        auto scan_model_three = CE.estimate_scan(*G, Subproblem::Singleton(2));
        CHECK(CE.predict_cardinality(*scan_model_one) == 5);
        CHECK(CE.predict_cardinality(*scan_model_two) == 10);
        CHECK(CE.predict_cardinality(*scan_model_three) == 8);
    }

    SECTION("estimate_filter")
    {
        auto scan_model = CE.estimate_scan(*G, Subproblem::Singleton(0));
        cnf::CNF filter;
        auto filter_model = CE.estimate_filter(*G, *scan_model, filter);
        CHECK(CE.predict_cardinality(*filter_model) == 5);
    }

    SECTION("estimate_limit")
    {
        auto scan_model_one = CE.estimate_scan(*G, Subproblem::Singleton(0));
        auto scan_model_two = CE.estimate_scan(*G, Subproblem::Singleton(1));
        auto limit_model_one_high = CE.estimate_limit(*G, *scan_model_one, 5000, 0);
        auto limit_model_two_high = CE.estimate_limit(*G, *scan_model_two, 5000, 0);
        CHECK(CE.predict_cardinality(*limit_model_one_high) == 5);
        CHECK(CE.predict_cardinality(*limit_model_two_high) == 10);

        auto limit_model_one_low = CE.estimate_limit(*G, *scan_model_one, 3, 0);
        auto limit_model_two_low = CE.estimate_limit(*G, *scan_model_two, 3, 0);
        CHECK(CE.predict_cardinality(*limit_model_one_low) == 3);
        CHECK(CE.predict_cardinality(*limit_model_two_low) == 3);
    }

    SECTION("estimate_grouping")
    {
        auto scan_model = CE.estimate_scan(*G, Subproblem::Singleton(0));
        std::vector<QueryGraph::group_type> group_by;
        auto grouping_model = CE.estimate_grouping(*G, *scan_model, group_by);
        CHECK(CE.predict_cardinality(*grouping_model) == 5);
    }

    SECTION("estimate_join")
    {
        auto scan_model_one = CE.estimate_scan(*G, Subproblem::Singleton(0));
        auto scan_model_two = CE.estimate_scan(*G, Subproblem::Singleton(1));
        cnf::CNF condition;
        auto join_model = CE.estimate_join(*G, *scan_model_one, *scan_model_two, condition);
        CHECK(CE.predict_cardinality(*join_model) == 50);
    }

    SECTION("estimate_semi_join")
    {
        auto scan_model_one = CE.estimate_scan(*G, Subproblem::Singleton(0));
        auto scan_model_two = CE.estimate_scan(*G, Subproblem::Singleton(1));
        cnf::CNF condition;
        auto join_model = CE.estimate_semi_join(*G, *scan_model_one, *scan_model_two, condition);
        CHECK(CE.predict_cardinality(*join_model) == 5);
    }
}

TEST_CASE("Injection estimator estimates for semi joins", "[core][catalog][cardinality]")
{
    using Subproblem = SmallBitset;
    /* Get Catalog and create new database to use for unit testing. */
    Catalog::Clear();
    Catalog &Cat = Catalog::Get();
    auto &db = Cat.add_database(Cat.pool("db"));
    Cat.set_database_in_use(db);

    std::ostringstream out, err;
    Diagnostic diag(false, out, err);

    /* Create pooled strings. */
    ThreadSafePooledString str_A = Cat.pool("A");
    ThreadSafePooledString str_B = Cat.pool("B");
    ThreadSafePooledString str_C = Cat.pool("C");

    ThreadSafePooledString col_id  = Cat.pool("id");
    ThreadSafePooledString col_aid = Cat.pool("aid");
    ThreadSafePooledString col_bid = Cat.pool("bid");
    ThreadSafePooledString col_cid = Cat.pool("cid");

    /* Create tables. */
    Table &tbl_A = db.add_table(str_A);
    Table &tbl_B = db.add_table(str_B);
    Table &tbl_C = db.add_table(str_C);

    /* Add columns to tables. */
    tbl_A.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_B.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_B.push_back(col_aid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_B.push_back(col_cid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_C.push_back(col_id, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_C.push_back(col_aid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_C.push_back(col_bid, Type::Get_Integer(Type::TY_Vector, 4));

    /* Add data to tables. */
    std::size_t num_rows_A = 5;
    std::size_t num_rows_B = 10;
    std::size_t num_rows_C = 8;
    tbl_A.store(Cat.create_store(tbl_A));
    tbl_B.store(Cat.create_store(tbl_B));
    tbl_C.store(Cat.create_store(tbl_C));
    tbl_A.layout(Cat.data_layout());
    tbl_B.layout(Cat.data_layout());
    tbl_C.layout(Cat.data_layout());
    for (std::size_t i = 0; i < num_rows_A; ++i) { tbl_A.store().append(); }
    for (std::size_t i = 0; i < num_rows_B; ++i) { tbl_B.store().append(); }
    for (std::size_t i = 0; i < num_rows_C; ++i) { tbl_C.store().append(); }

    /* Define query:
     *    C
     *  /  \
     * A -- B
     */
    const char *query = "SELECT * \
                         FROM A, B, C \
                         WHERE A.id = C.aid AND A.id = B.aid AND B.cid = C.bid;";
    auto S = m::statement_from_string(diag, query);
    M_insist(diag.num_errors() == 0);
    auto G = QueryGraph::Build(*S);
    std::istringstream json_input;
    json_input.str("{ \"mine\": [ \
                                {\"relations\": [\"A\"], \"size\":500, \"reductions\": [{ \"right_relations\": [\"B\"],  \"size\":5}, \
                                                                                    { \"right_relations\": [\"C\"],  \"size\":3}, \
                                                                                    { \"right_relations\": [\"B\", \"C\"],  \"size\":3}]}, \
                                {\"relations\": [\"B\"], \"size\":10, \"reductions\": [{ \"right_relations\": [\"A\"],  \"size\":8}, \
                                                                                    { \"right_relations\": [\"C\"],  \"size\":7}, \
                                                                                    { \"right_relations\": [\"A\", \"C\"],  \"size\":5}]}, \
                                {\"relations\": [\"C\"], \"size\":8, \"reductions\": [{ \"right_relations\": [\"B\"],  \"size\":6}, \
                                                                                    { \"right_relations\": [\"C\"],  \"size\":5}, \
                                                                                    { \"right_relations\": [\"B\", \"C\"],  \"size\":3}]}, \
                                {\"relations\": [\"A\", \"B\"], \"size\":1000, \"reductions\": [{\"right_relations\": [\"C\"],  \"size\":500}]}, \
                                {\"relations\": [\"A\", \"C\"], \"size\":2000, \"reductions\": [{\"right_relations\": [\"B\"],  \"size\":100}]} \
                                ]}");
    InjectionCardinalityEstimator ICE(diag, Cat.pool("mine"), json_input);

    SECTION("estimate_scan")
    {
        auto existing_entry_model = ICE.estimate_scan(*G, Subproblem(1UL));
        CHECK(ICE.predict_cardinality(*existing_entry_model) == 500);
        auto non_existing_entry_model = ICE.estimate_scan(*G, Subproblem::Singleton(1));
        CHECK(ICE.predict_cardinality(*non_existing_entry_model) == 10);
    }

    SECTION("estimate_filter")
    {
        cnf::CNF filter;

        auto existing_entry_model = ICE.estimate_scan(*G, Subproblem(1UL));
        auto filter_existing_entry_model = ICE.estimate_filter(*G, *existing_entry_model, filter);
        CHECK(ICE.predict_cardinality(*filter_existing_entry_model) == 500);

        auto non_existing_entry_model = ICE.estimate_scan(*G, Subproblem::Singleton(1));
        auto filter_non_existing_entry_model = ICE.estimate_filter(*G, *non_existing_entry_model, filter);
        CHECK(ICE.predict_cardinality(*filter_non_existing_entry_model) == 10);
    }

    SECTION("estimate_limit")
    {
        auto existing_entry_model = ICE.estimate_scan(*G, Subproblem(1UL));
        auto non_existing_entry_model = ICE.estimate_scan(*G, Subproblem::Singleton(1));
        auto limit_existing_entry_model_high = ICE.estimate_limit(*G, *existing_entry_model, 5000, 0);
        auto limit_non_existing_entry_model_high = ICE.estimate_limit(*G, *non_existing_entry_model, 5000, 0);
        CHECK(ICE.predict_cardinality(*limit_existing_entry_model_high) == 500);
        CHECK(ICE.predict_cardinality(*limit_non_existing_entry_model_high) == 10);

        auto limit_existing_entry_model_low = ICE.estimate_limit(*G, *existing_entry_model, 8, 0);
        auto limit_non_existing_entry_model_low = ICE.estimate_limit(*G, *non_existing_entry_model, 8, 0);
        CHECK(ICE.predict_cardinality(*limit_existing_entry_model_low) == 8);
        CHECK(ICE.predict_cardinality(*limit_non_existing_entry_model_low) == 8);
    }

    SECTION("estimate_grouping (empty)")
    {
        auto existing_entry_model = ICE.estimate_scan(*G, Subproblem(1UL));
        auto non_existing_entry_model = ICE.estimate_scan(*G, Subproblem::Singleton(1));
        std::vector<QueryGraph::group_type> group_by;
        auto grouping_existing_entry_model = ICE.estimate_grouping(*G, *existing_entry_model, group_by);
        auto grouping_non_existing_entry_model = ICE.estimate_grouping(*G, *non_existing_entry_model, group_by);
        CHECK(ICE.predict_cardinality(*grouping_existing_entry_model) == 1);
        CHECK(ICE.predict_cardinality(*grouping_non_existing_entry_model) == 1);
    }

    SECTION("estimate_join")
    {
        auto existing_entry_model_one = ICE.estimate_scan(*G, Subproblem(1UL));
        auto existing_entry_model_two = ICE.estimate_scan(*G, Subproblem::Singleton(1));
        auto existing_entry_model_three = ICE.estimate_scan(*G, Subproblem::Singleton(2));
        cnf::CNF condition;
        auto existing_entry_model_join_one = ICE.estimate_join(*G, *existing_entry_model_one, *existing_entry_model_two,
                                                           condition);
        auto existing_entry_model_join_two = ICE.estimate_join(*G, *existing_entry_model_one,
                                                               *existing_entry_model_three, condition);
        CHECK(ICE.predict_cardinality(*existing_entry_model_join_one) == 1000);
        CHECK(ICE.predict_cardinality(*existing_entry_model_join_two) == 2000);
    }

    SECTION("wrong database, return cartesian")
    {
        std::istringstream json_input_wrong_db;
        json_input_wrong_db.str("{ \"mine\": [{\"relations\": [\"A\", \"B\"], \"size\":1000}]}");
        InjectionCardinalityEstimator ice_wrong_db(diag, Cat.pool("yours"), json_input_wrong_db);

        auto non_existing_entry_model_one = ice_wrong_db.estimate_scan(*G, Subproblem::Singleton(0));
        auto non_existing_entry_model_two = ice_wrong_db.estimate_scan(*G, Subproblem::Singleton(1));
        cnf::CNF condition;
        auto non_existing_entry_model_join = ice_wrong_db.estimate_join(*G, *non_existing_entry_model_one,
                                                                        *non_existing_entry_model_two, condition);
        CHECK(ice_wrong_db.predict_cardinality(*non_existing_entry_model_one) == 5);
        CHECK(ice_wrong_db.predict_cardinality(*non_existing_entry_model_two) == 10);
        CHECK(ice_wrong_db.predict_cardinality(*non_existing_entry_model_join) == 50);
    }

    SECTION("estimate_semi_join")
    {
        auto existing_entry_model_one = ICE.estimate_scan(*G, Subproblem(1UL));
        auto non_existing_entry_model_two = ICE.estimate_scan(*G, Subproblem::Singleton(1));
        auto non_existing_entry_model_three = ICE.estimate_scan(*G, Subproblem::Singleton(2));
        cnf::CNF condition;
        auto model_one_semi_join_two = ICE.estimate_semi_join(*G, *existing_entry_model_one, *non_existing_entry_model_two,
                                                           condition);
        CHECK(ICE.predict_cardinality(*model_one_semi_join_two) == 5);
        auto model_one_semi_join_two_three = ICE.estimate_semi_join(*G, *model_one_semi_join_two, *non_existing_entry_model_three,
                                                                         condition);
        CHECK(ICE.predict_cardinality(*model_one_semi_join_two_three) == 3);
        auto model_join_two_three = ICE.estimate_join(*G, *non_existing_entry_model_two, *non_existing_entry_model_three,
                                                              condition);
        auto model_join_two_three_semi_join = ICE.estimate_join(*G, *non_existing_entry_model_two, *non_existing_entry_model_three,
                                                      condition);
        CHECK(ICE.predict_cardinality(*model_join_two_three_semi_join) == 80);
        auto model_one_semi_join_two_join_three = ICE.estimate_semi_join(*G, *existing_entry_model_one, *non_existing_entry_model_three,
                                                              condition);
        CHECK(ICE.predict_cardinality(*model_one_semi_join_two_join_three) == 3);
        model_one_semi_join_two_join_three = ICE.estimate_semi_join(*G, *model_join_two_three, *non_existing_entry_model_three,
                                                                         condition);
        CHECK(ICE.predict_cardinality(*model_one_semi_join_two_join_three) == 80);
        model_one_semi_join_two_join_three = ICE.estimate_semi_join(*G, *model_one_semi_join_two, *non_existing_entry_model_three,
                                                                    condition);
        CHECK(ICE.predict_cardinality(*model_one_semi_join_two_join_three) == 3);
        auto model_join_one_two = ICE.estimate_join(*G, *existing_entry_model_one, *non_existing_entry_model_two,
                                                    condition);
        auto model_join_one_two_semi_join = ICE.estimate_semi_join(*G, *model_join_one_two, *non_existing_entry_model_three,
                                                             condition);
        CHECK(ICE.predict_cardinality(*model_join_one_two_semi_join) == 500);
        model_join_one_two_semi_join = ICE.estimate_join(*G, *model_join_one_two, *non_existing_entry_model_two,condition);
        CHECK(ICE.predict_cardinality(*model_join_one_two_semi_join) == 1000);
    }

}
