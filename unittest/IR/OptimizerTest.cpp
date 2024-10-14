#include "catch2/catch.hpp"
#include "mutable/Options.hpp"
#include <iostream>
#include <mutable/IR/Optimizer.hpp>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/catalog/Type.hpp>
#include <mutable/IR/PlanTable.hpp>
#include <mutable/mutable.hpp>
#include <mutable/storage/Store.hpp>
#include <mutable/util/ADT.hpp>
#include <parse/Parser.hpp>
#include <parse/Sema.hpp>


using namespace m;

/*======================================================================================================================
 * Test Cost Function.
 *====================================================================================================================*/
TEST_CASE("Optimizer/ResultDB", "[IR]")
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
    ThreadSafePooledString str_E = Cat.pool("E");
    ThreadSafePooledString str_F = Cat.pool("F");
    ThreadSafePooledString str_G = Cat.pool("G");

    ThreadSafePooledString col_id = Cat.pool("id");
    ThreadSafePooledString col_aid = Cat.pool("aid");
    ThreadSafePooledString col_bid = Cat.pool("bid");
    ThreadSafePooledString col_cid = Cat.pool("cid");
    ThreadSafePooledString col_did = Cat.pool("did");
    ThreadSafePooledString col_eid = Cat.pool("eid");
    ThreadSafePooledString col_fid = Cat.pool("fid");
    ThreadSafePooledString col_gid = Cat.pool("gid");

    /* Create tables. */
    Table &tbl_A = db.add_table(str_A);
    Table &tbl_B = db.add_table(str_B);
    Table &tbl_C = db.add_table(str_C);
    Table &tbl_D = db.add_table(str_D);
    Table &tbl_E = db.add_table(str_E);
    Table &tbl_F = db.add_table(str_F);
    Table &tbl_G = db.add_table(str_G);

    /* Add columns to tables. */
    tbl_A.push_back(col_bid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_A.push_back(col_eid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_A.push_back(col_cid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_B.push_back(col_cid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_B.push_back(col_aid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_B.push_back(col_did, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_B.push_back(col_eid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_C.push_back(col_bid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_C.push_back(col_aid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_C.push_back(col_did, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_D.push_back(col_bid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_D.push_back(col_eid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_D.push_back(col_cid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_E.push_back(col_aid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_E.push_back(col_did, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_E.push_back(col_gid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_E.push_back(col_fid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_E.push_back(col_bid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_F.push_back(col_eid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_F.push_back(col_gid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_G.push_back(col_eid, Type::Get_Integer(Type::TY_Vector, 4));
    tbl_G.push_back(col_fid, Type::Get_Integer(Type::TY_Vector, 4));

    /* Add data to tables. */
    tbl_A.store(Cat.create_store(tbl_A));
    tbl_B.store(Cat.create_store(tbl_B));
    tbl_C.store(Cat.create_store(tbl_C));
    tbl_D.store(Cat.create_store(tbl_D));
    tbl_E.store(Cat.create_store(tbl_E));
    tbl_F.store(Cat.create_store(tbl_F));
    tbl_G.store(Cat.create_store(tbl_G));
    tbl_A.layout(Cat.data_layout());
    tbl_B.layout(Cat.data_layout());
    tbl_C.layout(Cat.data_layout());
    tbl_D.layout(Cat.data_layout());
    tbl_E.layout(Cat.data_layout());
    tbl_F.layout(Cat.data_layout());
    tbl_G.layout(Cat.data_layout());

    const Subproblem A(1);
    const Subproblem B(2);
    const Subproblem C(4);
    const Subproblem D(8);
    const Subproblem E(16);
    const Subproblem F(32);
    const Subproblem G(64);

    Options::Get().greedy_cuts = true;

    SECTION("acylic tree")
    {
        /* Define query:
         *
         *       A
         *      / \
         *     B   E
         *    / \ / \
         *    C D F G
         */
        const std::string query = "\
SELECT * \
FROM A, B, C, D, E, F, G \
WHERE A.bid = B.aid AND A.eid = E.aid AND B.cid = C.bid AND B.did = D.bid AND E.fid = F.eid AND E.gid = G.eid;";

        /* Setup semi-join cardinalities */
        for (std::size_t i = 0; i < 100; ++i) { tbl_A.store().append(); }
        for (std::size_t i = 0; i < 200; ++i) { tbl_B.store().append(); }
        for (std::size_t i = 0; i < 1000; ++i) { tbl_C.store().append(); }
        for (std::size_t i = 0; i < 300; ++i) { tbl_D.store().append(); }
        for (std::size_t i = 0; i < 10000; ++i) { tbl_E.store().append(); }
        for (std::size_t i = 0; i < 1500; ++i) { tbl_F.store().append(); }
        for (std::size_t i = 0; i < 500; ++i) { tbl_G.store().append(); }

        std::istringstream json_input;
        json_input.str("{ \"db\": [ \
                                {\"relations\": [\"A\"], \"size\":100, \"reductions\": [{ \"right_relations\": [\"B\", \"C\", \"D\"],  \"size\":50}, \
                                                                                    { \"right_relations\": [\"E\", \"F\", \"G\"],  \"size\":30}, \
                                                                                    { \"right_relations\": [\"B\", \"E\", \"C\", \"D\", \"F\", \"G\"],  \"size\":20}]}, \
                                {\"relations\": [\"B\"], \"size\":200, \"reductions\": [{ \"right_relations\": [\"A\", \"E\", \"F\", \"G\"],  \"size\":80}, \
                                                                                    { \"right_relations\": [\"C\"],  \"size\":120}, \
                                                                                    { \"right_relations\": [\"D\"],  \"size\":50}, \
                                                                                    { \"right_relations\": [\"A\", \"E\", \"F\", \"G\", \"C\"],  \"size\":75},\
                                                                                    { \"right_relations\": [\"A\", \"E\", \"F\", \"G\", \"D\"],  \"size\":45},\
                                                                                    { \"right_relations\": [\"C\", \"D\"],  \"size\":50},\
                                                                                    { \"right_relations\": [\"C\", \"D\", \"A\", \"E\", \"F\", \"G\"],  \"size\":30}]}, \
                                {\"relations\": [\"C\"], \"size\":1000, \"reductions\": [{ \"right_relations\": [\"B\", \"D\", \"A\", \"E\", \"F\", \"G\"],  \"size\":500}]}, \
                                {\"relations\": [\"D\"], \"size\":300, \"reductions\": [{ \"right_relations\": [\"B\", \"C\", \"A\", \"E\", \"F\", \"G\"],  \"size\":100}]}, \
                                {\"relations\": [\"E\"], \"size\":10000, \"reductions\": [{ \"right_relations\": [\"F\"],  \"size\":1000}, \
                                                                                    { \"right_relations\": [\"A\", \"B\", \"C\", \"D\"],  \"size\":2500}, \
                                                                                    { \"right_relations\": [\"G\"],  \"size\":5000}, \
                                                                                    { \"right_relations\": [\"A\", \"B\", \"C\", \"D\", \"F\"],  \"size\":950},\
                                                                                    { \"right_relations\": [\"A\", \"B\", \"C\", \"D\", \"G\"],  \"size\":2000},\
                                                                                    { \"right_relations\": [\"F\", \"G\"],  \"size\":800},\
                                                                                    { \"right_relations\": [\"A\", \"B\", \"C\", \"D\", \"F\", \"G\"],  \"size\":750}]}, \
                                {\"relations\": [\"F\"], \"size\":1500, \"reductions\": [{ \"right_relations\": [\"B\", \"C\", \"D\", \"A\", \"E\", \"G\"],  \"size\":1200}]}, \
                                {\"relations\": [\"G\"], \"size\":500, \"reductions\": [{ \"right_relations\": [\"B\", \"C\", \"D\", \"A\", \"E\", \"F\"],  \"size\":500}]} \
                                ]}");

        auto stmt = m::statement_from_string(diag, query);
        REQUIRE(not diag.num_errors());
        auto query_graph = QueryGraph::Build(*stmt);
        auto &QG = *query_graph;

        std::unique_ptr<CardinalityEstimator> est = std::make_unique<InjectionCardinalityEstimator>(diag, Cat.pool("db"), json_input);

        auto A_model = est->estimate_scan(QG, A);
        auto B_model = est->estimate_scan(QG, B);
        auto C_model = est->estimate_scan(QG, C);
        auto D_model = est->estimate_scan(QG, D);
        auto E_model = est->estimate_scan(QG, E);
        auto F_model = est->estimate_scan(QG, F);
        auto G_model = est->estimate_scan(QG, G);

        db.cardinality_estimator(std::move(est));



        auto &ICE = db.cardinality_estimator();
        Cat.default_plan_enumerator(Cat.pool("DPccp"));

        SECTION("Cardinality orders")
        {

            std::vector<std::unique_ptr<DataModel>> base_models;

            base_models.emplace_back(std::move(A_model));
            base_models.emplace_back(std::move(B_model));
            base_models.emplace_back(std::move(C_model));
            base_models.emplace_back(std::move(D_model));
            base_models.emplace_back(std::move(E_model));
            base_models.emplace_back(std::move(F_model));
            base_models.emplace_back(std::move(G_model));

            /* Check correctness of cardinality orders */
            std::vector<std::size_t> card_orders[QG.num_sources()];

            std::vector<std::size_t> cardinality_order_A = {4, 1};
            std::vector<std::size_t> cardinality_order_B = {3, 0, 2};
            std::vector<std::size_t> cardinality_order_C = {1};
            std::vector<std::size_t> cardinality_order_D = {1};
            std::vector<std::size_t> cardinality_order_E = {5, 0, 6};
            std::vector<std::size_t> cardinality_order_F = {5};
            std::vector<std::size_t> cardinality_order_G = {5};


            auto tree_enumerator = TreeEnumerator(QG.num_sources());
            tree_enumerator.determine_reduced_models(QG, QG.adjacency_matrix(), ICE, card_orders, base_models);

            REQUIRE(std::equal(card_orders[0].begin(), card_orders[0].begin() + 1, cardinality_order_A.begin()));
            REQUIRE(std::equal(card_orders[1].begin(), card_orders[1].begin() + 2, cardinality_order_B.begin()));
            REQUIRE(std::equal(card_orders[2].begin(), card_orders[2].begin(), cardinality_order_C.begin()));
            REQUIRE(std::equal(card_orders[3].begin(), card_orders[3].begin(), cardinality_order_D.begin()));
            REQUIRE(std::equal(card_orders[4].begin(), card_orders[4].begin() + 2, cardinality_order_E.begin()));
            REQUIRE(std::equal(card_orders[5].begin(), card_orders[5].begin(), cardinality_order_F.begin()));
            REQUIRE(std::equal(card_orders[6].begin(), card_orders[6].begin(), cardinality_order_G.begin()));
        }

        SECTION("Check enumeration")
        {
            auto tree_enumerator_correct = TreeEnumerator(QG.num_sources());
            auto SJ = SemiJoinCostFunction();

            /* Initial models */
            auto A_red_model = ICE.estimate_full_reduction(QG, *A_model);
            auto B_red_model = ICE.estimate_full_reduction(QG, *B_model);
            auto C_red_model = ICE.estimate_full_reduction(QG, *C_model);
            auto D_red_model = ICE.estimate_full_reduction(QG, *D_model);
            auto E_red_model = ICE.estimate_full_reduction(QG, *E_model);
            auto F_red_model = ICE.estimate_full_reduction(QG, *F_model);
            auto G_red_model = ICE.estimate_full_reduction(QG, *G_model);

            /* Reduced models */
            auto BC_model = ICE.estimate_semi_join(QG, *B_model, *C_model, {});
            auto BD_model = ICE.estimate_semi_join(QG, *B_model, *D_model, {});
            auto BCD_model = ICE.estimate_semi_join(QG, *BC_model, *D_model, {});
            auto EF_model = ICE.estimate_semi_join(QG, *E_model, *F_model, {});
            auto EG_model = ICE.estimate_semi_join(QG, *E_model, *G_model, {});
            auto EFG_model = ICE.estimate_semi_join(QG, *EF_model, *G_model, {});
            auto AB_model = ICE.estimate_semi_join(QG, *A_model, *BCD_model, {});
            auto EA_model = ICE.estimate_semi_join(QG, *E_model, *AB_model, {});
            auto AE_model = ICE.estimate_semi_join(QG, *A_model, *EFG_model, {});
            auto ABE_model = ICE.estimate_semi_join(QG, *AB_model, *EFG_model, {});
            auto BCDA_model = ICE.estimate_semi_join(QG, *BCD_model, *AE_model, {});
            auto EFGA_model = ICE.estimate_semi_join(QG, *EFG_model, *AB_model, {});
            auto EFA_model = ICE.estimate_semi_join(QG, *EF_model, *AB_model, {});
            auto EGA_model = ICE.estimate_semi_join(QG, *EG_model, *AB_model, {});
            auto GE_model = ICE.estimate_semi_join(QG, *G_model, *EFA_model, {});
            auto FE_model = ICE.estimate_semi_join(QG, *F_model, *EGA_model, {});
            auto BDA_model = ICE.estimate_semi_join(QG, *BD_model, *AE_model, {});
            auto CB_model = ICE.estimate_semi_join(QG, *C_model, *BDA_model, {});
            auto BCA_model = ICE.estimate_semi_join(QG, *BC_model, *AE_model, {});
            auto DB_model = ICE.estimate_semi_join(QG, *D_model, *BCA_model, {});
            auto BA_model = ICE.estimate_semi_join(QG, *B_model, *AE_model, {});

            /* Basic costs */
            auto C_costs = 0;
            auto D_costs = 0;
            auto G_costs = 0;
            auto F_costs = 0;

            /* Recursive costs */
            auto BD_costs = SJ.estimate_semi_join_costs(ICE, *B_model, *D_model)
                                    + SJ.estimate_semi_join_costs(ICE, *D_model, *B_red_model);
            auto BCD_costs = BD_costs + SJ.estimate_semi_join_costs(ICE, *BD_model, *C_model)
                                    + SJ.estimate_semi_join_costs(ICE, *C_model, *B_red_model);
            auto EF_costs = SJ.estimate_semi_join_costs(ICE, *E_model, *F_model)
                                    + SJ.estimate_semi_join_costs(ICE, *F_model, *E_red_model);
            auto EFG_costs = EF_costs + SJ.estimate_semi_join_costs(ICE, *EF_model, *G_model)
                                    + SJ.estimate_semi_join_costs(ICE, *G_model, *E_red_model);
            auto AB_costs = BCD_costs + SJ.estimate_semi_join_costs(ICE, *A_model, *BCD_model)
                                    + SJ.estimate_semi_join_costs(ICE, *BCD_model, *A_red_model);
            auto AE_costs = EFG_costs + SJ.estimate_semi_join_costs(ICE, *A_model, *EFG_model)
                                    + SJ.estimate_semi_join_costs(ICE, *EFG_model, *A_red_model);
            auto ABE_costs = AE_costs + BCD_costs + SJ.estimate_semi_join_costs(ICE, *AE_model, *BCD_model)
                                    + SJ.estimate_semi_join_costs(ICE, *BCD_model, *A_red_model);
            auto BDA_costs = BD_costs + AE_costs + SJ.estimate_semi_join_costs(ICE, *BD_model, *AE_model)
                                    + SJ.estimate_semi_join_costs(ICE, *AE_model, *B_red_model);
            auto BCDA_costs = BDA_costs + SJ.estimate_semi_join_costs(ICE, *BDA_model, *C_model)
                                    + SJ.estimate_semi_join_costs(ICE, *C_model, *B_red_model);
            auto EFA_costs = EF_costs + AB_costs + SJ.estimate_semi_join_costs(ICE, *EF_model, *AB_model)
                                    + SJ.estimate_semi_join_costs(ICE, *AB_model, *E_red_model);
            auto EFGA_costs = EFA_costs + SJ.estimate_semi_join_costs(ICE, *EFA_model, *G_model)
                                    + SJ.estimate_semi_join_costs(ICE, *G_model, *E_red_model);
            auto GE_costs = EFA_costs + SJ.estimate_semi_join_costs(ICE, *G_model, *EFA_model)
                                    + SJ.estimate_semi_join_costs(ICE, *EFA_model, *G_red_model);
            auto EA_costs = AB_costs + SJ.estimate_semi_join_costs(ICE, *E_model, *AB_model)
                                    + SJ.estimate_semi_join_costs(ICE, *AB_model, *E_red_model);
            auto EAG_costs =  EA_costs + SJ.estimate_semi_join_costs(ICE, *EA_model, *G_model)
                                    + SJ.estimate_semi_join_costs(ICE, *G_model, *E_red_model);
            auto FE_costs = EAG_costs + SJ.estimate_semi_join_costs(ICE, *F_model, *EGA_model)
                                    + SJ.estimate_semi_join_costs(ICE, *EGA_model, *F_red_model);
            auto CB_costs = BDA_costs + SJ.estimate_semi_join_costs(ICE, *C_model, *BDA_model)
                                    + SJ.estimate_semi_join_costs(ICE, *BDA_model, *C_red_model);
            auto BA_Costs = AE_costs + SJ.estimate_semi_join_costs(ICE, *B_model, *AE_model)
                                    + SJ.estimate_semi_join_costs(ICE, *AE_model, *B_red_model);
            auto BAC_costs = BA_Costs + SJ.estimate_semi_join_costs(ICE, *BA_model, *C_model)
                                    + SJ.estimate_semi_join_costs(ICE, *C_model, *B_red_model);
            auto DB_costs = BAC_costs + SJ.estimate_semi_join_costs(ICE, *D_model, *BCA_model)
                            + SJ.estimate_semi_join_costs(ICE, *BCA_model, *D_red_model);


            std::vector<std::unique_ptr<DataModel>> base_models;

            base_models.emplace_back(std::move(A_model));
            base_models.emplace_back(std::move(B_model));
            base_models.emplace_back(std::move(C_model));
            base_models.emplace_back(std::move(D_model));
            base_models.emplace_back(std::move(E_model));
            base_models.emplace_back(std::move(F_model));
            base_models.emplace_back(std::move(G_model));

            /* Check correctness of cardinality orders */
            std::vector<std::size_t> card_orders[QG.num_sources()];


            auto tree_enumerator = TreeEnumerator(QG.num_sources());
           // auto best_root = tree_enumerator.find_best_root(QG, QG.adjacency_matrix(), ICE, SJ, card_orders, base_models);

            /*
            REQUIRE(tree_enumerator.parent_node_costs(1,3)->second == D_costs);
            REQUIRE(tree_enumerator.parent_node_costs(4,5)->second == F_costs);
            REQUIRE(tree_enumerator.parent_node_costs(4,6)->second == G_costs);
            REQUIRE(tree_enumerator.parent_node_costs(0,1)->second == BCD_costs);
            REQUIRE(tree_enumerator.parent_node_costs(0,4)->second == EFG_costs);
            REQUIRE(tree_enumerator.parent_node_costs(0,0)->second == ABE_costs);
            REQUIRE(tree_enumerator.parent_node_costs(4,0)->second == AB_costs);
            REQUIRE(tree_enumerator.parent_node_costs(5, 4)->second == EFA_costs);
            REQUIRE(tree_enumerator.parent_node_costs(6,6)->second == GE_costs);
            REQUIRE(tree_enumerator.parent_node_costs(5, 5)->second == FE_costs);
            REQUIRE(tree_enumerator.parent_node_costs(6, 4)->second == EAG_costs);
            REQUIRE(tree_enumerator.parent_node_costs(2, 2)->second == CB_costs);
            REQUIRE(tree_enumerator.parent_node_costs(3, 1)->second == BAC_costs);
            REQUIRE(tree_enumerator.parent_node_costs(3, 3)->second == DB_costs);
            REQUIRE(tree_enumerator.parent_node_costs(2, 1)->second == BDA_costs);
            REQUIRE(tree_enumerator.parent_node_costs(1, 1)->second == BCDA_costs);
            REQUIRE(tree_enumerator.parent_node_costs(4, 4)->second == EFGA_costs);
             */

            std::size_t correct_best_root = 0;
            auto costs = tree_enumerator.parent_node_costs(0,0)->second;
            for (std::size_t i = 1; i < QG.num_sources(); i++) {
                if (tree_enumerator.parent_node_costs(i,i)->second < costs) {
                    costs = tree_enumerator.parent_node_costs(i,i)->second;
                    correct_best_root = i;
                }
            }

            // REQUIRE(correct_best_root == best_root);
        }

        SECTION("Check final semi-join order")
        {

            std::vector<Optimizer_ResultDB::semi_join_order_t> semi_join_reduction_order_correct;

            semi_join_reduction_order_correct.emplace_back(QG[2], QG[4]);
            semi_join_reduction_order_correct.emplace_back(QG[4], QG[5]);
            semi_join_reduction_order_correct.emplace_back(QG[4], QG[6]);
            semi_join_reduction_order_correct.emplace_back(QG[0], QG[4]);
            semi_join_reduction_order_correct.emplace_back(QG[2], QG[0]);
            semi_join_reduction_order_correct.emplace_back(QG[2], QG[3]);

            std::vector<std::unique_ptr<DataModel>> base_models;

            base_models.emplace_back(std::move(A_model));
            base_models.emplace_back(std::move(B_model));
            base_models.emplace_back(std::move(C_model));
            base_models.emplace_back(std::move(D_model));
            base_models.emplace_back(std::move(E_model));
            base_models.emplace_back(std::move(F_model));
            base_models.emplace_back(std::move(G_model));

/*            std::vector<Optimizer_ResultDB::semi_join_order_t> semi_join_reduction_order_actual =
                    Optimizer_ResultDB_utils::enumerate_semi_join_reduction_order(QG, ICE, base_models);

            auto it_correct = semi_join_reduction_order_correct.begin();
            auto it_actual = semi_join_reduction_order_actual.begin();

            while(it_correct != semi_join_reduction_order_correct.end() && it_actual != semi_join_reduction_order_actual.end())
            {
                REQUIRE(it_correct->lhs == it_correct->lhs);
                REQUIRE(it_correct->rhs == it_correct->rhs);
                if(it_correct != semi_join_reduction_order_correct.end())
                {
                    ++it_correct;
                }
                if(it_actual != semi_join_reduction_order_correct.end())
                {
                    ++it_actual;
                }
            }*/


        }
        SECTION("Complete Run Acyclic")
        {
            Optimizer_ResultDB opt;
            auto ret_op = opt.operator()(QG);
            REQUIRE(ret_op.first != nullptr);
            REQUIRE(ret_op.second);
        }



    }
    SECTION("cyclic")
    {
        /* Define query:
         *
         * A -- B \ / F
         * |  / |  E  |
         * C -- D / \ G
         */
        const std::string query = "\
        SELECT * \
        FROM A, B, C, D, E, F, G \
        WHERE A.bid = B.aid AND A.cid = C.aid AND B.did = D.bid AND C.did = D.cid AND B.eid = E.bid AND D.eid = E.did \
                                  AND E.fid = F.eid AND E.gid = G.eid AND G.fid = F.gid AND B.cid = C.bid;";

        /* Setup semi-join cardinalities */
        for (std::size_t i = 0; i < 100; ++i) { tbl_A.store().append(); }
        for (std::size_t i = 0; i < 200; ++i) { tbl_B.store().append(); }
        for (std::size_t i = 0; i < 1000; ++i) { tbl_C.store().append(); }
        for (std::size_t i = 0; i < 300; ++i) { tbl_D.store().append(); }
        for (std::size_t i = 0; i < 10000; ++i) { tbl_E.store().append(); }
        for (std::size_t i = 0; i < 1500; ++i) { tbl_F.store().append(); }
        for (std::size_t i = 0; i < 500; ++i) { tbl_G.store().append(); }

        std::istringstream json_input;
        json_input.str("{ \"db\": [ \
                                {\"relations\": [\"A\"], \"size\":100, \"reductions\": [{ \"right_relations\": [\"B\", \"C\", \"D\", \"E\", \"F\", \"G\"],  \"size\":20}]}, \
                                {\"relations\": [\"B\"], \"size\":200}, \
                                {\"relations\": [\"C\"], \"size\":1000}, \
                                {\"relations\": [\"D\"], \"size\":300}, \
                                {\"relations\": [\"E\"], \"size\":10000, \"reductions\": [{ \"right_relations\": [\"F\", \"G\"],  \"size\":500},\
                                                                                    { \"right_relations\": [\"A\", \"B\", \"C\", \"D\"],  \"size\":800}, \
                                                                                    { \"right_relations\": [\"A\", \"B\", \"C\", \"D\", \"F\", \"G\"],  \"size\":400}]}, \
                                {\"relations\": [\"F\"], \"size\":1500, \"reductions\": [{ \"right_relations\": [\"A\", \"B\", \"C\", \"D\", \"E\", \"G\"],  \"size\":800}]}, \
                                {\"relations\": [\"G\"], \"size\":500, \"reductions\": [{ \"right_relations\": [\"A\", \"B\", \"C\", \"D\", \"E\", \"F\"],  \"size\":250}]}, \
                                {\"relations\": [\"B\", \"D\"], \"size\":400}, \
                                {\"relations\": [\"B\", \"C\"], \"size\":2000}, \
                                {\"relations\": [\"C\", \"D\"], \"size\":3000}, \
                                {\"relations\": [\"F\", \"E\"], \"size\":6000, \"reductions\": [{ \"right_relations\": [\"A\", \"B\", \"C\", \"D\"],  \"size\":3000},\
                                                                                        { \"right_relations\": [\"G\"],  \"size\":2500},\
                                                                                        { \"right_relations\": [\"A\", \"B\", \"C\", \"D\", \"G\"],  \"size\":2000}]}, \
                                {\"relations\": [\"F\", \"G\"], \"size\":5000, \"reductions\": [{ \"right_relations\": [\"A\", \"B\", \"C\", \"D\", \"E\"],  \"size\":4000}]}, \
                                {\"relations\": [\"G\", \"E\"], \"size\":100, \"reductions\": [{ \"right_relations\": [\"A\", \"B\", \"C\", \"D\"],  \"size\":50},\
                                                                                        { \"right_relations\": [\"F\"],  \"size\":30},\
                                                                                        { \"right_relations\": [\"A\", \"B\", \"C\", \"D\", \"F\"],  \"size\":25}]}, \
                                {\"relations\": [\"B\", \"D\", \"C\"], \"size\":5000, \"reductions\": [{ \"right_relations\": [\"A\"],  \"size\":3000},\
                                                                                                { \"right_relations\": [\"E\", \"F\", \"G\"],  \"size\":4500},  \
                                                                                                { \"right_relations\": [\"A\", \"E\", \"F\", \"G\"],  \"size\":2700}]}, \
                                {\"relations\": [\"E\", \"F\", \"G\"], \"size\":10000, \"reductions\": [{ \"right_relations\": [\"A\", \"B\", \"C\", \"D\"],  \"size\":5000}]} \
                                ]}");
        auto stmt = m::statement_from_string(diag, query);
        REQUIRE(not diag.num_errors());
        auto query_graph = QueryGraph::Build(*stmt);
        auto &QG = *query_graph;

        std::unique_ptr<CardinalityEstimator> est = std::make_unique<InjectionCardinalityEstimator>(diag, Cat.pool("db"), json_input);

        db.cardinality_estimator(std::move(est));

        auto &ICE = db.cardinality_estimator();
        Cat.default_plan_enumerator(Cat.pool("DPccp"));

        SECTION("Greedily apply two-vertex cuts")
        {
            auto already_used = Subproblem();
            std::vector<Optimizer_ResultDB_utils::fold_t> folds;
            Optimizer_ResultDB_utils::find_vertex_cuts(QG, Subproblem(31), already_used, folds);

            REQUIRE(folds.size() == 1);
            REQUIRE(already_used == Subproblem(6));

            auto fold = folds.back();
            REQUIRE(fold.contains(1));
            REQUIRE(fold.contains(2));

            Optimizer_ResultDB_utils::find_vertex_cuts(QG, Subproblem(112), already_used, folds);

            REQUIRE(folds.size() == 1);
            REQUIRE(already_used == Subproblem(6));

            fold = folds.back();
            REQUIRE(fold.contains(1));
            REQUIRE(fold.contains(2));

            std::vector<Subproblem> blocks = {Subproblem(112), Subproblem(31)};
            Subproblem cut_vertices = Subproblem::Singleton(4);
            Optimizer_ResultDB_utils::find_and_apply_vertex_cuts(QG, blocks, cut_vertices);

            REQUIRE(QG.num_sources() == 6);
            REQUIRE(blocks.size() == 2);
            REQUIRE(cut_vertices == Subproblem(8));

            auto new_model = ICE.estimate_scan(QG, Subproblem(1));
            REQUIRE(ICE.predict_cardinality(*new_model) == 2000);
        }

        SECTION("Block Cut Forest")
        {
            std::vector<Subproblem> blocks = {Subproblem(112), Subproblem(31)};
            Subproblem cut_vertices = Subproblem::Singleton(4);
            Optimizer_ResultDB_utils::find_and_apply_vertex_cuts(QG, blocks, cut_vertices);

            Optimizer_ResultDB_utils::bc_forest_t bc_forest;

            Optimizer_ResultDB_utils::build_bc_forest(blocks, cut_vertices, bc_forest);

            REQUIRE(bc_forest.size() == 3);
            REQUIRE(bc_forest.contains(Subproblem(13)));
            REQUIRE(bc_forest[Subproblem(13)].size() == 1);
            REQUIRE(bc_forest[Subproblem(13)].front() == Subproblem(8));
            REQUIRE(bc_forest.contains(Subproblem(56)));
            REQUIRE(bc_forest[Subproblem(56)].size() == 1);
            REQUIRE(bc_forest[Subproblem(56)].front() == Subproblem(8));
            REQUIRE(bc_forest.contains(Subproblem(8)));
            REQUIRE(bc_forest[Subproblem(8)].size() == 2);
            std::unordered_set<Subproblem, SubproblemHash> neighbors = {Subproblem(56), Subproblem(13)};
            for (auto neighbor: bc_forest[Subproblem(8)]) {
                REQUIRE(neighbors.contains(neighbor));
            }

            auto visited = Subproblem();
            std::vector<Subproblem> folding_problems;
            std::vector<Optimizer_ResultDB_utils::fold_t> folds;

            Optimizer_ResultDB_utils::visit_bc_forest(bc_forest, blocks, visited, folding_problems, folds);

            REQUIRE(visited == Subproblem(61));
            REQUIRE(folds.size() == 1);
            REQUIRE(folding_problems.size() == 1);
            REQUIRE(folding_problems[0]== Subproblem(56));
            REQUIRE(folds[0].contains(0));
            REQUIRE(folds[0].contains(2));

        }

        /*         SECTION("Cycle Solving Optimizer")
               {
                   std::vector<std::unique_ptr<DataModel>> base_models;

                   auto producers = Optimizer_ResultDB_utils::compute_and_solve_biconnected_components(QG, base_models);

                   REQUIRE(base_models.size() == 4);
                   REQUIRE(ICE.predict_cardinality(*base_models[0]) == 100);
                   REQUIRE(ICE.predict_cardinality(*base_models[1]) == 1500);
                   REQUIRE(ICE.predict_cardinality(*base_models[2]) == 5000);
                   REQUIRE(ICE.predict_cardinality(*base_models[3]) == 100);

                 REQUIRE(QG.num_sources() == 4);
                   REQUIRE(producers[0]->info().subproblem == Subproblem(5));
                   REQUIRE(producers[0]->info().estimated_cardinality == 100);
                   REQUIRE(producers[1]->info().subproblem == Subproblem(2));
                   REQUIRE(producers[1]->info().estimated_cardinality == 1500);
                   REQUIRE(producers[2]->info().subproblem == Subproblem(8));
                   REQUIRE(producers[2]->info().estimated_cardinality == 5000);
                   REQUIRE(producers[3]->info().subproblem == Subproblem(16));
                   REQUIRE(producers[3]->info().estimated_cardinality == 100);

                   std::vector<Optimizer_ResultDB::semi_join_order_t> semi_join_order = Optimizer_ResultDB_utils::enumerate_semi_join_reduction_order(QG, ICE, base_models);
                   REQUIRE(semi_join_order.size() == 3);

        } */
        SECTION("Complete Run Cyclic")
        {
            Optimizer_ResultDB opt;
            auto ret_op = opt.operator()(QG);
            REQUIRE(ret_op.first != nullptr);
            REQUIRE(ret_op.second);
        }
    }
}
