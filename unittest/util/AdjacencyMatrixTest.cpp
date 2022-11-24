#include "catch2/catch.hpp"

#include <iostream>
#include <mutable/mutable.hpp>
#include <mutable/util/AdjacencyMatrix.hpp>


using namespace m;


TEST_CASE("AdjacencyMatrix/Standalone Matrix", "[core][util][unit]")
{
    AdjacencyMatrix adj_mat(4);

    /* Query graph consisting of four relations with four joins edges.
     * (0,2) (0,3) (1,3) (2,3)
     * The resulting adjacency matrix:
     *     0 1 2 3 (indices)
     *     -------
     * 0 | 0 0 1 1
     * 1 | 0 0 0 1
     * 2 | 1 0 0 1
     * 3 | 1 1 1 0
     */
    adj_mat(0, 2) = true;
    adj_mat(2, 0) = true;

    adj_mat(0, 3) = true;
    adj_mat(3, 0) = true;

    adj_mat(1, 3) = adj_mat(3, 1) = true;

    adj_mat(2, 3) = adj_mat(3, 2) = true;

    SECTION("check edges")
    {
        /* Positive checks. */
        REQUIRE(adj_mat(0, 2));
        REQUIRE(adj_mat(2, 0));

        REQUIRE(adj_mat(0, 3));
        REQUIRE(adj_mat(3, 0));

        REQUIRE(adj_mat(1, 3));
        REQUIRE(adj_mat(3, 1));

        REQUIRE(adj_mat(2, 3));
        REQUIRE(adj_mat(3, 2));

        /* Negative checks. */
        REQUIRE_FALSE(adj_mat(0, 1));
        REQUIRE_THROWS_AS(adj_mat.at(13, 9), m::out_of_range);
        REQUIRE_THROWS_AS(adj_mat.at(2, 63), m::out_of_range);
    }

    SECTION("check connectedness of nodes")
    {
        /* {0} - {2} */
        SmallBitset first(1); // node 0 equals 0b0001 -> 1
        SmallBitset second(4); // node 2 equals 0b0100 -> 4
        REQUIRE(adj_mat.is_connected(first, second));

        {
            /* {0} - {1} */
            SmallBitset first(1);
            SmallBitset second(2);
            REQUIRE_FALSE(adj_mat.is_connected(first, second));
        }

        {
            /* {0,1} - {2} */
            SmallBitset first(3);
            SmallBitset second(4);
            REQUIRE(adj_mat.is_connected(first, second));
        }

        {
            /* {0,1} - {3} */
            SmallBitset first(3);
            SmallBitset second(8);
            REQUIRE(adj_mat.is_connected(first, second));
        }

        {
            /* {1} - {0,2} */
            SmallBitset first(2);
            SmallBitset second(5);
            REQUIRE_FALSE(adj_mat.is_connected(first, second));
        }

        {
            /* {1} - {0,2,3} */
            SmallBitset first(2);
            SmallBitset second(13); // 0b1101
            REQUIRE(adj_mat.is_connected(first, second));
        }

        {
            /* {0,2,3} - {1} */
            SmallBitset first(13); // 0b1101
            SmallBitset second(2);
            REQUIRE(adj_mat.is_connected(first, second));
        }
    }
}

TEST_CASE("AdjacencyMatrix/QueryGraph Matrix", "[core][util][unit]")
{
    /* Get Catalog and create new database to use for unit testing. */
    Catalog::Clear();
    Catalog &Cat = Catalog::Get();
    auto &db = Cat.add_database("mydb");
    Cat.set_database_in_use(db);

    Diagnostic diag(false, std::cout, std::cerr);

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

    /* Create adjacency matrix. */
    const char *query = "SELECT * \
                         FROM A, B, C, D \
                         WHERE A.id = C.aid AND A.id = D.aid AND B.id = D.bid AND C.id = D.cid;";
    auto stmt = m::statement_from_string(diag, query);
    auto query_graph = QueryGraph::Build(*stmt);

    const AdjacencyMatrix &adj_mat = query_graph->adjacency_matrix();

    /* Mapping of relation name to SmallBitset of data source ID. */
    std::unordered_map<const char *, SmallBitset> map;
    for (auto &ds : query_graph->sources()) {
        auto tbl = cast<const BaseTable>(ds.get());
        REQUIRE(tbl != nullptr);
        const char *str_name = tbl->table().name;
        map[str_name] = SmallBitset(1 << ds->id());
    }

    SmallBitset first;
    SmallBitset second;

    SECTION("check connectedness of edges")
    {
        /* {A} - {C} */
        first = map[str_A];
        second = map[str_C];
        REQUIRE(adj_mat.is_connected(first, second));

        /* {A} - {B} */
        first = map[str_A];
        second = map[str_B];
        REQUIRE_FALSE(adj_mat.is_connected(first, second));

        /* {A,B} - {C} */
        first = map[str_A] | map[str_B];
        second = map[str_C];
        REQUIRE(adj_mat.is_connected(first, second));

        /* {A,B} - {D} */
        first = map[str_A] | map[str_B];
        second = map[str_D];
        REQUIRE(adj_mat.is_connected(first, second));

        /* {B} - {A,C} */
        first = map[str_B];
        second = map[str_A] | map[str_C];
        REQUIRE_FALSE(adj_mat.is_connected(first, second));

        /* {B} - {A,C,D} */
        first = map[str_B];
        second = map[str_A] | map[str_C] | map[str_D];
        REQUIRE(adj_mat.is_connected(first, second));

        /* {A,C,D} - {B} */
        first = map[str_A] | map[str_C] | map[str_D];
        second = map[str_B];
        REQUIRE(adj_mat.is_connected(first, second));
    }
}

TEST_CASE("AdjacencyMatrix", "[core][util][unit]")
{
    AdjacencyMatrix M(4);

    /* Query graph consisting of four relations with two joins edges.
     * (0,2) (0,3)
     * The resulting adjacency matrix:
     *     0 1 2 3 (indices)
     *     -------
     * 0 | 0 0 1 1
     * 1 | 0 0 0 0
     * 2 | 1 0 0 0
     * 3 | 1 0 0 0
     */
    M(0, 2) = M(2, 0) = true;
    M(0, 3) = M(3, 0) = true;

    SECTION("reachable from singleton")
    {
        {
            SmallBitset S(0b0100UL); // 2
            SmallBitset R(0b1101UL); // 0, 2, 3
            REQUIRE(M.reachable(S) == R);
        }
        {
            SmallBitset S(0b0010UL); // 1
            SmallBitset R(0b0010UL); // 1
            REQUIRE(M.reachable(S) == R);
        }
    }

    SECTION("reachable from set")
    {
        {
            SmallBitset S(0b1100UL); // 2, 3
            SmallBitset R(0b1101UL); // 0, 2, 3
            REQUIRE(M.reachable(S) == R);
        }
        {
            SmallBitset S(0b1101UL); // 0, 2, 3
            SmallBitset R(0b1101UL); // 0, 2, 3
            REQUIRE(M.reachable(S) == R);
        }
        {
            SmallBitset S(0b0110UL); // 1, 2
            SmallBitset R(0b1111UL); // 0, 1, 2, 3
            REQUIRE(M.reachable(S) == R);
        }
    }

    SECTION("reachable from singleton within subset")
    {
        SmallBitset subset(0b0110); // 1, 2
        {
            SmallBitset S(0b0010UL); // 1
            SmallBitset R(0b0010UL); // 1
            REQUIRE(M.reachable(S, subset) == R);
        }
        {
            SmallBitset S(0b0100UL); // 2
            SmallBitset R(0b0100UL); // 2
            REQUIRE(M.reachable(S, subset) == R);
        }
    }

    SECTION("reachable from singleton outside subset")
    {
        SmallBitset subset(0b0110); // 1, 2
        SmallBitset S(0b0001UL); // 0
        SmallBitset R(0b0000UL); // ∅
        REQUIRE(M.reachable(S, subset) == R);
    }

    SECTION("reachable from set within subset")
    {
        SmallBitset subset(0b0111); // 0, 1, 2
        {
            SmallBitset S(0b0101UL); // 0, 2
            SmallBitset R(0b0101UL); // 0, 2
            REQUIRE(M.reachable(S, subset) == R);
        }
        {
            SmallBitset S(0b0011UL); // 0, 1
            SmallBitset R(0b0111UL); // 0, 1, 2
            REQUIRE(M.reachable(S, subset) == R);
        }
    }

    SECTION("reachable from set partially within subset")
    {
        SmallBitset subset(0b1110); // 1, 2, 3
        SmallBitset S(0b0011UL); // 0, 1
        SmallBitset R(0b0010UL); // 1
        REQUIRE(M.reachable(S, subset) == R);
    }

    SECTION("reachable from set partially within subset")
    {
        SmallBitset subset(0b1110); // 1, 2, 3
        SmallBitset S(0b0001UL); // 0
        SmallBitset R(0b0000UL); // ∅
        REQUIRE(M.reachable(S, subset) == R);
    }

    SECTION("connected singleton")
    {
        {
            SmallBitset S(0b0001UL); // 0
            REQUIRE(M.is_connected(S));
        }
        {
            SmallBitset S(0b0010UL); // 1
            REQUIRE(M.is_connected(S));
        }
    }

    SECTION("connected set")
    {
        {
            SmallBitset S(0b0011UL); // 0, 1
            REQUIRE(not M.is_connected(S));
        }
        {
            SmallBitset S(0b0101UL); // 0, 2
            REQUIRE(M.is_connected(S));
        }
    }
}

TEST_CASE("AdjacencyMatrix/QueryGraph Matrix Negative", "[core][util][unit]")
{
    /* Get Catalog and create new database to use for unit testing. */
    Catalog::Clear();
    Catalog &Cat = Catalog::Get();
    auto &db = Cat.add_database("mydb");
    Cat.set_database_in_use(db);

    Diagnostic diag(false, std::cout, std::cerr);

    /* Create pooled strings. */
    const char *str_A    = Cat.pool("A");
    const char *str_B    = Cat.pool("B");
    const char *str_C    = Cat.pool("C");

    const char *col_id = Cat.pool("id");
    const char *col_aid = Cat.pool("aid");

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

    SECTION("create non-binary join")
    {
        /* Create adjacency matrix. */
        const char *query = "SELECT * \
                         FROM A, B, C \
                         WHERE A.id = B.aid + C.aid;";
        auto stmt = m::statement_from_string(diag, query);
        auto query_graph = QueryGraph::Build(*stmt);

        REQUIRE_THROWS_AS(query_graph->adjacency_matrix(), std::invalid_argument);
    }

    SECTION("create no join")
    {
        /* Create adjacency matrix. */
        const char *query = "SELECT * \
                         FROM A \
                         WHERE A.id = 0;";
        auto stmt = statement_from_string(diag, query);
        auto query_graph = QueryGraph::Build(*stmt);
        const AdjacencyMatrix &adj_mat = query_graph->adjacency_matrix();

        REQUIRE_FALSE(adj_mat(0, 0));
    }
}

TEST_CASE("AdjacencyMatrix/transitive_closure_directed", "[core][util][unit]")
{
    SECTION("empty")
    {
        AdjacencyMatrix M(5);
        AdjacencyMatrix closure = M.transitive_closure_directed();
        CHECK(closure == M);
    }

    SECTION("self-connected nodes")
    {
        AdjacencyMatrix M(3);
        M(0, 0) = M(1, 1) = M(2, 2) = true;
        AdjacencyMatrix closure = M.transitive_closure_directed();
        CHECK(closure == M);
    }

    SECTION("transitive edge")
    {
        /*  A →  B ↔  C ←  D →  E */
        const unsigned A = 0;
        const unsigned B = 1;
        const unsigned C = 2;
        const unsigned D = 3;
        const unsigned E = 4;
        AdjacencyMatrix M(5);
        M(A, B) = true;
        M(B, C) = true;
        M(C, B) = true;
        M(D, C) = M(D, E) = true;
        AdjacencyMatrix closure = M.transitive_closure_directed();

        AdjacencyMatrix expected(5);
        expected(A, B) = expected(A, C) = true;
        expected(B, B) = expected(B, C) = true;
        expected(C, B) = expected(C, C) = true;
        expected(D, B) = expected(D, C) = expected(D, E) = true;
        CHECK(expected == closure);
    }
}

TEST_CASE("AdjacencyMatrix/transitive_closure_undirected", "[core][util][unit]")
{
    SECTION("empty")
    {
        AdjacencyMatrix M(5);
        AdjacencyMatrix closure = M.transitive_closure_undirected();
        CHECK(closure == M);
    }

    SECTION("self-connected nodes")
    {
        AdjacencyMatrix M(3);
        M(0, 0) = M(1, 1) = M(2, 2) = true;

        AdjacencyMatrix closure = M.transitive_closure_undirected();
        CHECK(closure == M);
    }

    SECTION("transitive edge")
    {
        /*  0 1 0
         *  1 0 1
         *  0 1 0 */
        AdjacencyMatrix M(3);
        M(0, 1) = M(1, 0) = true;
        M(1, 2) = M(2, 1) = true;

        /*  1 1 1
         *  1 1 1
         *  1 1 1 */
        AdjacencyMatrix closure = M.transitive_closure_undirected();
        for (std::size_t i = 0; i != 3; ++i)
            for (std::size_t j = 0; j != 3; ++j)
                CHECK(closure(i, j));
    }

    SECTION("disconnected subgraphs")
    {
        /* 0 1 0 0 0
         * 1 0 1 0 0
         * 0 1 0 0 0
         * 0 0 0 0 1
         * 0 0 0 1 0 */
        AdjacencyMatrix M(5);
        M(0, 1) = M(1, 0) = true;
        M(1, 2) = M(2, 1) = true;
        M(3, 4) = M(4, 3) = true;

        /* 1 1 1 0 0
         * 1 1 1 0 0
         * 1 1 1 0 0
         * 0 0 0 1 1
         * 0 0 0 1 1 */
        AdjacencyMatrix closure = M.transitive_closure_undirected();
        for (std::size_t i = 0; i != 3; ++i)
            for (std::size_t j = 0; j != 3; ++j)
                CHECK(closure(i, j));
        for (std::size_t i = 3; i != 5; ++i) {
            for (std::size_t j = 0; j != 3; ++j) {
                CHECK_FALSE(closure(i, j));
                CHECK_FALSE(closure(j, i));
            }
        }
        for (std::size_t i = 3; i != 5; ++i)
            for (std::size_t j = 3; j != 5; ++j)
                CHECK(closure(i, j));
    }
}

TEST_CASE("AdjacencyMatrix/for_each_CSG_undirected", "[core][util][unit]")
{
    AdjacencyMatrix M;

    /* The enumerated CSGs. */
    std::vector<SmallBitset> CSGs;

    auto inserter = [&CSGs, &M](SmallBitset S) {
        CHECK(M.is_connected(S));
        CHECK(std::find(CSGs.begin(), CSGs.end(), S) == CSGs.end()); // no duplicates
        CSGs.emplace_back(S);
    };

    auto CHECK_CSG = [&CSGs](SmallBitset S) {
        CHECK(std::find(CSGs.begin(), CSGs.end(), S) != CSGs.end());
    };

    SECTION("4-chain")
    {
        const SmallBitset A(1UL << 0);
        const SmallBitset B(1UL << 1);
        const SmallBitset C(1UL << 2);
        const SmallBitset D(1UL << 3);

        /*  A ↔  B
         *  ↕
         *  C ↔  D
         *
         *
         *  0 1 1 0
         *  1 0 0 0
         *  1 0 0 1
         *  0 0 1 0
         */
        M = AdjacencyMatrix(4);
        M(0, 1) = M(1, 0) = true;
        M(0, 2) = M(2, 0) = true;
        M(2, 3) = M(3, 2) = true;

        SECTION("empty set")
        {
            const SmallBitset super;
            M.for_each_CSG_undirected(super, inserter);
            CHECK(CSGs.empty());
        }

        SECTION("singleton {A}")
        {
            SmallBitset super(A);
            M.for_each_CSG_undirected(super, inserter);
            REQUIRE(CSGs.size() == 1);
            CHECK(CSGs[0] == A);
        }

        SECTION("{A, B}")
        {
            const SmallBitset super(A|B);
            M.for_each_CSG_undirected(super, inserter);
            REQUIRE(CSGs.size() == 3);
            CHECK_CSG(A);
            CHECK_CSG(B);
            CHECK_CSG(A|B);
            CHECK(CSGs[0] == A);
            CHECK(CSGs[1] == B);
            CHECK(CSGs[2] == (A|B));
        }

        SECTION("{A, B} from A")
        {
            const SmallBitset super(A|B);
            M.for_each_CSG_undirected(super, A, inserter);
            REQUIRE(CSGs.size() == 2);
            CHECK_CSG(A);
            CHECK_CSG(A|B);
            CHECK(CSGs[0] == A);
            CHECK(CSGs[1] == (A|B));
        }

        SECTION("{A, B} from B")
        {
            const SmallBitset super(A|B);
            M.for_each_CSG_undirected(super, B, inserter);
            REQUIRE(CSGs.size() == 2);
            CHECK_CSG(B);
            CHECK_CSG(A|B);
            CHECK(CSGs[0] == B);
            CHECK(CSGs[1] == (A|B));
        }

        SECTION("{A, B, C}")
        {
            const SmallBitset super(A|B|C);
            M.for_each_CSG_undirected(super, inserter);
            REQUIRE(CSGs.size() == 6);
            CHECK(CSGs[0] == A);
            CHECK(CSGs[1] == B);
            CHECK(CSGs[2] == (A|B));
            CHECK(CSGs[3] == C);
            CHECK(CSGs[4] == (A|C));
            CHECK(CSGs[5] == (A|B|C));
        }

        SECTION("{A, B, C} from A")
        {
            const SmallBitset super(A|B|C);
            M.for_each_CSG_undirected(super, A, inserter);
            REQUIRE(CSGs.size() == 4);
            CHECK(CSGs[0] == A);
            CHECK(CSGs[1] == (A|B));
            CHECK(CSGs[2] == (A|C));
            CHECK(CSGs[3] == (A|B|C));
        }

        SECTION("{A, B, C} from B")
        {
            const SmallBitset super(A|B|C);
            M.for_each_CSG_undirected(super, B, inserter);
            REQUIRE(CSGs.size() == 3);
            CHECK(CSGs[0] == B);
            CHECK(CSGs[1] == (A|B));
            CHECK(CSGs[2] == (A|B|C));
        }

        SECTION("{A, B, D}")
        {
            const SmallBitset super(A|B|D);
            M.for_each_CSG_undirected(super, inserter);
            REQUIRE(CSGs.size() == 4);
            CHECK(CSGs[0] == A);
            CHECK(CSGs[1] == B);
            CHECK(CSGs[2] == (A|B));
            CHECK(CSGs[3] == D);
        }

        SECTION("{A, B, D} from A")
        {
            const SmallBitset super(A|B|D);
            M.for_each_CSG_undirected(super, A, inserter);
            REQUIRE(CSGs.size() == 2);
            CHECK(CSGs[0] == A);
            CHECK(CSGs[1] == (A|B));
        }

        SECTION("{A, B, D} from B")
        {
            const SmallBitset super(A|B|D);
            M.for_each_CSG_undirected(super, B, inserter);
            REQUIRE(CSGs.size() == 2);
            CHECK(CSGs[0] == B);
            CHECK(CSGs[1] == (A|B));
        }

        SECTION("{A, B, D} from D")
        {
            const SmallBitset super(A|B|D);
            M.for_each_CSG_undirected(super, D, inserter);
            REQUIRE(CSGs.size() == 1);
            CHECK(CSGs[0] == D);
        }

        SECTION("{A, C, D}")
        {
            const SmallBitset super(A|C|D);
            M.for_each_CSG_undirected(super, inserter);
            REQUIRE(CSGs.size() == 6);
            CHECK(CSGs[0] == A);
            CHECK(CSGs[1] == C);
            CHECK(CSGs[2] == (A|C));
            CHECK(CSGs[3] == D);
            CHECK(CSGs[4] == (C|D));
            CHECK(CSGs[5] == (A|C|D));
        }

        SECTION("{B, C, D}")
        {
            const SmallBitset super(B|C|D);
            M.for_each_CSG_undirected(super, inserter);
            REQUIRE(CSGs.size() == 4);
            CHECK(CSGs[0] == B);
            CHECK(CSGs[1] == C);
            CHECK(CSGs[2] == D);
            CHECK(CSGs[3] == (C|D));
        }

        SECTION("{A, B, C, D}")
        {
            const SmallBitset super(A|B|C|D);
            M.for_each_CSG_undirected(super, inserter);
            REQUIRE(CSGs.size() == 10);
            CHECK(CSGs[0] == A);
            CHECK(CSGs[1] == B);
            CHECK(CSGs[2] == (A|B));
            CHECK(CSGs[3] == C);
            CHECK(CSGs[4] == (A|C));
            CHECK(CSGs[5] == (A|B|C));
            CHECK(CSGs[6] == D);
            CHECK(CSGs[7] == (D|C));
            CHECK(CSGs[8] == (A|C|D));
            CHECK(CSGs[9] == (A|B|C|D));
        }

        SECTION("{A, B, C, D} from A")
        {
            const SmallBitset super(A|B|C|D);
            M.for_each_CSG_undirected(super, A, inserter);
            REQUIRE(CSGs.size() == 6);
            CHECK(CSGs[0] == A);
            CHECK(CSGs[1] == (A|B));
            CHECK(CSGs[2] == (A|C));
            CHECK(CSGs[3] == (A|B|C));
            CHECK(CSGs[4] == (A|C|D));
            CHECK(CSGs[5] == (A|B|C|D));
        }

        SECTION("{A, B, C, D} from B")
        {
            const SmallBitset super(A|B|C|D);
            M.for_each_CSG_undirected(super, B, inserter);
            REQUIRE(CSGs.size() == 4);
            CHECK(CSGs[0] == B);
            CHECK(CSGs[1] == (A|B));
            CHECK(CSGs[2] == (A|B|C));
            CHECK(CSGs[3] == (A|B|C|D));
        }

        SECTION("{A, B, C, D} from A|B")
        {
            const SmallBitset super(A|B|C|D);
            M.for_each_CSG_undirected(super, A|B, inserter);
            REQUIRE(CSGs.size() == 7);
            CHECK(CSGs[0] == A);
            CHECK(CSGs[1] == (A|C));
            CHECK(CSGs[2] == (A|C|D));
            CHECK(CSGs[3] == B);
            CHECK(CSGs[4] == (A|B));
            CHECK(CSGs[5] == (A|B|C));
            CHECK(CSGs[6] == (A|B|C|D));
        }

        SECTION("{A, B, C, D} from A|D")
        {
            const SmallBitset super(A|B|C|D);
            M.for_each_CSG_undirected(super, A|D, inserter);
            REQUIRE(CSGs.size() == 8);
            CHECK(CSGs[0] == A);
            CHECK(CSGs[1] == (A|B));
            CHECK(CSGs[2] == (A|C));
            CHECK(CSGs[3] == (A|B|C));
            CHECK(CSGs[4] == D);
            CHECK(CSGs[5] == (C|D));
            CHECK(CSGs[6] == (A|C|D));
            CHECK(CSGs[7] == (A|B|C|D));
        }
    }

    SECTION("4 with triangle")
    {
        const SmallBitset A(1UL << 0);
        const SmallBitset B(1UL << 1);
        const SmallBitset C(1UL << 2);
        const SmallBitset D(1UL << 3);
        /*    C
         *   / \
         *  A---D---B
         *
         *  0 0 1 1
         *  0 0 0 1
         *  1 0 0 1
         *  1 1 1 0
         */
        M = AdjacencyMatrix(4);
        M(0, 2) = M(2, 0) = true;
        M(0, 3) = M(3, 0) = true;
        M(1, 3) = M(3, 1) = true;
        M(2, 3) = M(3, 2) = true;

        SECTION("singleton {A}")
        {
            SmallBitset super(A);
            M.for_each_CSG_undirected(super, inserter);
            REQUIRE(CSGs.size() == 1);
            CHECK(CSGs[0] == A);
        }

        SECTION("{A, B}")
        {
            const SmallBitset super(A|B);
            M.for_each_CSG_undirected(super, inserter);
            REQUIRE(CSGs.size() == 2);
            CHECK_CSG(A);
            CHECK_CSG(B);
            CHECK(CSGs[0] == A);
            CHECK(CSGs[1] == B);
        }

        SECTION("{A, C}")
        {
            const SmallBitset super(A|C);
            M.for_each_CSG_undirected(super, inserter);
            REQUIRE(CSGs.size() == 3);
            CHECK_CSG(A);
            CHECK_CSG(C);
            CHECK(CSGs[0] == A);
            CHECK(CSGs[1] == C);
            CHECK(CSGs[2] == (A|C));
        }

        SECTION("{A, D}")
        {
            const SmallBitset super(A|D);
            M.for_each_CSG_undirected(super, inserter);
            REQUIRE(CSGs.size() == 3);
            CHECK_CSG(A);
            CHECK_CSG(D);
            CHECK(CSGs[0] == A);
            CHECK(CSGs[1] == D);
            CHECK(CSGs[2] == (A|D));
        }

        SECTION("{B, C}")
        {
            const SmallBitset super(B|C);
            M.for_each_CSG_undirected(super, inserter);
            REQUIRE(CSGs.size() == 2);
            CHECK_CSG(B);
            CHECK_CSG(C);
            CHECK(CSGs[0] == B);
            CHECK(CSGs[1] == C);
        }

        SECTION("{B, D}")
        {
            const SmallBitset super(B|D);
            M.for_each_CSG_undirected(super, inserter);
            REQUIRE(CSGs.size() == 3);
            CHECK_CSG(B);
            CHECK_CSG(D);
            CHECK(CSGs[0] == B);
            CHECK(CSGs[1] == D);
            CHECK(CSGs[2] == (B|D));
        }

        SECTION("{C, D}")
        {
            const SmallBitset super(C|D);
            M.for_each_CSG_undirected(super, inserter);
            REQUIRE(CSGs.size() == 3);
            CHECK_CSG(C);
            CHECK_CSG(D);
            CHECK(CSGs[0] == C);
            CHECK(CSGs[1] == D);
            CHECK(CSGs[2] == (C|D));
        }

        SECTION("{A, B, C}")
        {
            const SmallBitset super(A|B|C);
            M.for_each_CSG_undirected(super, inserter);
            REQUIRE(CSGs.size() == 4);
            CHECK_CSG(A);
            CHECK_CSG(B);
            CHECK_CSG(C);
            CHECK(CSGs[0] == A);
            CHECK(CSGs[1] == B);
            CHECK(CSGs[2] == C);
            CHECK(CSGs[3] == (A|C));
        }

        SECTION("{A, B, D}")
        {
            const SmallBitset super(A|B|D);
            M.for_each_CSG_undirected(super, inserter);
            REQUIRE(CSGs.size() == 6);
            CHECK_CSG(A);
            CHECK_CSG(B);
            CHECK_CSG(D);
            CHECK_CSG(A|D);
            CHECK_CSG(B|D);
            CHECK_CSG(A|B|D);
            CHECK(CSGs[0] == A);
            CHECK(CSGs[1] == B);
            CHECK(CSGs[2] == D);
            CHECK(CSGs[3] == (A|D));
            CHECK(CSGs[4] == (B|D));
            CHECK(CSGs[5] == (A|B|D));
        }

        SECTION("{A, C, D}")
        {
            const SmallBitset super(A|C|D);
            M.for_each_CSG_undirected(super, inserter);
            REQUIRE(CSGs.size() == 7);
            CHECK_CSG(A);
            CHECK_CSG(C);
            CHECK_CSG(D);
            CHECK_CSG(A|C);
            CHECK_CSG(A|D);
            CHECK_CSG(A|C|D);
            CHECK(CSGs[0] == A);
            CHECK(CSGs[1] == C);
            CHECK(CSGs[2] == (A|C));
            CHECK(CSGs[3] == D);
            CHECK(CSGs[4] == (A|D));
            CHECK(CSGs[5] == (C|D));
            CHECK(CSGs[6] == (A|C|D));
        }

        SECTION("{B, C, D}")
        {
            const SmallBitset super(B|C|D);
            M.for_each_CSG_undirected(super, inserter);
            REQUIRE(CSGs.size() == 6);
            CHECK_CSG(B);
            CHECK_CSG(C);
            CHECK_CSG(D);
            CHECK_CSG(B|D);
            CHECK_CSG(C|D);
            CHECK_CSG(B|C|D);
            CHECK(CSGs[0] == B);
            CHECK(CSGs[1] == C);
            CHECK(CSGs[2] == D);
            CHECK(CSGs[3] == (B|D));
            CHECK(CSGs[4] == (C|D));
            CHECK(CSGs[5] == (B|C|D));
        }

        SECTION("{A, B, C, D}")
        {
            const SmallBitset super(A|B|C|D);
            M.for_each_CSG_undirected(super, inserter);
            REQUIRE(CSGs.size() == 12);
            CHECK_CSG(A);
            CHECK_CSG(B);
            CHECK_CSG(C);
            CHECK_CSG(D);
            CHECK_CSG(A|C);
            CHECK_CSG(A|D);
            CHECK_CSG(B|D);
            CHECK_CSG(C|D);
            CHECK_CSG(A|C|D);
            CHECK_CSG(B|C|D);
            CHECK_CSG(A|B|D);
            CHECK_CSG(A|B|C|D);
            CHECK(CSGs[0] == A);
            CHECK(CSGs[1] == B);
            CHECK(CSGs[2] == C);
            CHECK(CSGs[3] == (A|C));
            CHECK(CSGs[4] == D);
            CHECK(CSGs[5] == (A|D));
            CHECK(CSGs[6] == (B|D));
            CHECK(CSGs[7] == (A|B|D));
            CHECK(CSGs[8] == (C|D));
            CHECK(CSGs[9] == (A|C|D));
            CHECK(CSGs[10] == (B|C|D));
            CHECK(CSGs[11] == (A|B|C|D));
        }
    }
}

TEST_CASE("AdjacencyMatrix/for_each_CSG_pair_undirected", "[core][util][unit]")
{
    AdjacencyMatrix M;

    /* The enumerated CSGs. */
    using csg_cmp_pair = std::pair<SmallBitset, SmallBitset>;
    struct hash
    {
        std::size_t operator()(const SmallBitset S) const {
            return murmur3_64(uint64_t(S));
        }

        std::size_t operator()(const csg_cmp_pair &P) const {
            return murmur3_64(uint64_t(P.first)) * murmur3_64(uint64_t(P.second));
        }
    };
    std::unordered_set<SmallBitset, hash> CSGs_enumerated, CSGs_solved;
    std::unordered_set<csg_cmp_pair, hash> pairs;
    auto inserter = [&M, &CSGs_enumerated, &CSGs_solved, &pairs](SmallBitset first, SmallBitset second) {
        CHECK(not first.empty());
        CHECK(not second.empty());
        CHECK((first & second).empty());
        CHECK(M.is_connected(first));
        CHECK(M.is_connected(second));
        CHECK(M.is_connected(first, second));

        /*----- Ensure that `first` and `second` have been solved. ---------------------------------------------------*/
        auto is_solved = [&CSGs_solved](SmallBitset S) -> bool {
            return S.singleton() or CSGs_solved.count(S);
        };

        auto make_solved = [&CSGs_solved, &CSGs_enumerated](SmallBitset S) {
            const auto erased = CSGs_enumerated.erase(S); // erase from CSGs_enumerated
            if (not erased) {
                S.print_fixed_length(std::cerr, 8);
                std::cerr << " has not been enumerated yet\n";
            }
            CHECK(erased);
            auto res = CSGs_solved.emplace(S); // add to CSGs_solved
            if (not res.second) {
                S.print_fixed_length(std::cerr, 8);
                std::cerr << " was already assumed solved\n";
            }
            CHECK(res.second);
        };

        if (not is_solved(first))  make_solved(first);
        if (not is_solved(second)) make_solved(second);
        CHECK(not is_solved(first|second)); // ERROR: has been used already

        CSGs_enumerated.emplace(first|second);
        auto res = pairs.emplace(first, second);
        CHECK(res.second); // no duplicates
    };

    auto CHECK_PAIR = [&pairs](SmallBitset first, SmallBitset second) {
        const bool has_pair = pairs.count(csg_cmp_pair(first, second)) + pairs.count(csg_cmp_pair(second, first));
        if (not has_pair) {
            std::cerr << "missing pair (";
            first.print_fixed_length(std::cerr, 8);
            std::cerr << ", ";
            second.print_fixed_length(std::cerr, 8);
            std::cerr << ")\n";
        }
        CHECK(has_pair);
    };

    SECTION("4-chain")
    {
        const SmallBitset A(1UL << 0);
        const SmallBitset B(1UL << 1);
        const SmallBitset C(1UL << 2);
        const SmallBitset D(1UL << 3);
        /*  A ↔  B
         *  ↕
         *  C ↔  D
         *
         *
         *  0 1 1 0
         *  1 0 0 0
         *  1 0 0 1
         *  0 0 1 0
         */
        M = AdjacencyMatrix(4);
        M(0, 1) = M(1, 0) = true; // A ↔ B
        M(0, 2) = M(2, 0) = true; // A ↔ C
        M(2, 3) = M(3, 2) = true; // C ↔ D

        SECTION("empty set")
        {
            const SmallBitset super;
            M.for_each_CSG_pair_undirected(super, inserter);
            CHECK(pairs.empty());
        }

        SECTION("singleton {A}")
        {
            SmallBitset super;
            super[0] = true; // A
            M.for_each_CSG_pair_undirected(super, inserter);
            CHECK(pairs.empty());
        }

        SECTION("{A, B}")
        {
            const SmallBitset super(A|B);
            M.for_each_CSG_pair_undirected(super, inserter);
            CHECK_PAIR(A, B);
            CHECK(pairs.size() == 1);
        }

        SECTION("{A, B, C}")
        {
            const SmallBitset super(A|B|C);
            M.for_each_CSG_pair_undirected(super, inserter);
            CHECK_PAIR(A, B);
            CHECK_PAIR(A, C);
            CHECK_PAIR(A|B, C);
            CHECK_PAIR(A|C, B);
            CHECK(pairs.size() == 4);
        }

        SECTION("{A, B, D}")
        {
            const SmallBitset super(A|B|D);
            M.for_each_CSG_pair_undirected(super, inserter);
            CHECK_PAIR(A, B);
            CHECK(pairs.size() == 1);
        }

        SECTION("{A, C, D}")
        {
            const SmallBitset super(A|C|D);
            M.for_each_CSG_pair_undirected(super, inserter);
            CHECK_PAIR(A, C);
            CHECK_PAIR(C, D);
            CHECK_PAIR(A, C|D);
            CHECK_PAIR(A|C, D);
            CHECK(pairs.size() == 4);
        }

        SECTION("{B, C, D}")
        {
            const SmallBitset super(B|C|D);
            M.for_each_CSG_pair_undirected(super, inserter);
            CHECK_PAIR(C, D);
            CHECK(pairs.size() == 1);
        }

        SECTION("{A, B, C, D}")
        {
            const SmallBitset super(A|B|C|D);
            M.for_each_CSG_pair_undirected(super, inserter);
            CHECK_PAIR(A, B);
            CHECK_PAIR(A, C);
            CHECK_PAIR(A, C|D);
            CHECK_PAIR(B, A|C);
            CHECK_PAIR(B, A|C|D);
            CHECK_PAIR(C, A|B);
            CHECK_PAIR(C, D);
            CHECK_PAIR(D, C|A);
            CHECK_PAIR(D, C|A|B);
            CHECK_PAIR(A|B, C|D);
            CHECK(pairs.size() == 10);
        }
    }

    SECTION("4 with triangle")
    {
        const SmallBitset A(1UL << 0);
        const SmallBitset B(1UL << 1);
        const SmallBitset C(1UL << 2);
        const SmallBitset D(1UL << 3);
        /*    C
         *   / \
         *  A---D---B
         *
         *  0 0 1 1
         *  0 0 0 1
         *  1 0 0 1
         *  1 1 1 0
         */
        M = AdjacencyMatrix(4);
        M(0, 2) = M(2, 0) = true;
        M(0, 3) = M(3, 0) = true;
        M(1, 3) = M(3, 1) = true;
        M(2, 3) = M(3, 2) = true;

        SECTION("{A, B, C, D}")
        {
            const SmallBitset super(A|B|C|D);
            auto print_rel = [](uint64_t idx) {
                switch (idx) {
                    case 0: std::cerr << 'A'; break;
                    case 1: std::cerr << 'B'; break;
                    case 2: std::cerr << 'C'; break;
                    case 3: std::cerr << 'D'; break;
                }
            };

            M.for_each_CSG_pair_undirected(super, inserter);

            CHECK_PAIR(A, C);
            CHECK_PAIR(A, D);
            CHECK_PAIR(B, D);
            CHECK_PAIR(C, D);
            CHECK_PAIR(A, B|D);
            CHECK_PAIR(A, C|D);
            CHECK_PAIR(A, B|C|D);
            CHECK_PAIR(B, C|D);
            CHECK_PAIR(A|C, D);
            CHECK_PAIR(A|C, B|D);
            CHECK_PAIR(A|D, B);
            CHECK_PAIR(A|D, C);
            CHECK_PAIR(B|D, C);
            CHECK_PAIR(A|C|D, B);
            CHECK_PAIR(A|B|D, C);
            CHECK(pairs.size() == 15);
        }
    }
}

TEST_CASE("AdjacencyMatrix/minimum_spanning_forest", "[core][util][unit]")
{
    auto weight = [](std::size_t u, std::size_t v) -> double { return u + v; };

    SECTION("no edges")
    {
        AdjacencyMatrix M(5);
        const AdjacencyMatrix MSF = M.minimum_spanning_forest(weight);
        CHECK(MSF == M);
    }

    SECTION("self-connected")
    {
        AdjacencyMatrix M(3);
        M(0, 0) = M(1, 1) = M(2, 2) = true;
        const AdjacencyMatrix MSF = M.minimum_spanning_forest(weight);
        for (std::size_t i = 0; i != 3; ++i)
            for (std::size_t j = 0; j != 3; ++j)
                CHECK_FALSE(MSF(i, j));
    }

    SECTION("triangle")
    {
        AdjacencyMatrix M(3);
        M(0, 1) = M(1, 0) = true;
        M(0, 2) = M(2, 0) = true;
        M(1, 2) = M(2, 1) = true;

        const AdjacencyMatrix MSF = M.minimum_spanning_forest(weight);

        AdjacencyMatrix expected(3);
        expected(0, 1) = expected(1, 0) = true;
        expected(0, 2) = expected(2, 0) = true;
        CHECK(expected == MSF);
    }

    SECTION("rectangle")
    {
        AdjacencyMatrix M(4);
        M(0, 1) = M(1, 0) = true;
        M(1, 2) = M(2, 1) = true;
        M(2, 3) = M(3, 2) = true;
        M(3, 0) = M(0, 3) = true;

        const AdjacencyMatrix MSF = M.minimum_spanning_forest(weight);

        AdjacencyMatrix expected(4);
        expected(0, 1) = expected(1, 0) = true;
        expected(1, 2) = expected(2, 1) = true;
        expected(3, 0) = expected(0, 3) = true;
        CHECK(expected == MSF);
    }

    SECTION("star4")
    {
        AdjacencyMatrix M(4);
        M(0, 1) = M(1, 0) = true;
        M(0, 2) = M(2, 0) = true;
        M(0, 3) = M(3, 0) = true;

        const AdjacencyMatrix MSF = M.minimum_spanning_forest(weight);
        CHECK(M == MSF);
    }

    SECTION("clique4")
    {
        AdjacencyMatrix M(4);
        M(0, 1) = M(1, 0) = true;
        M(0, 2) = M(2, 0) = true;
        M(0, 3) = M(3, 0) = true;
        M(1, 2) = M(2, 1) = true;
        M(1, 3) = M(3, 1) = true;
        M(2, 3) = M(3, 2) = true;

        const AdjacencyMatrix MSF = M.minimum_spanning_forest(weight);
        AdjacencyMatrix expected(4);
        expected(0, 1) = expected(1, 0) = true;
        expected(0, 2) = expected(2, 0) = true;
        expected(0, 3) = expected(3, 0) = true;
        CHECK(expected == MSF);
    }

    SECTION("two disconnected triangles")
    {
        AdjacencyMatrix M(6);
        M(0, 1) = M(1, 0) = true;
        M(0, 2) = M(2, 0) = true;
        M(1, 2) = M(2, 1) = true;
        M(3, 4) = M(4, 3) = true;
        M(3, 5) = M(5, 3) = true;
        M(4, 5) = M(5, 4) = true;

        const AdjacencyMatrix MSF = M.minimum_spanning_forest(weight);
        AdjacencyMatrix expected(6);
        expected(0, 1) = expected(1, 0) = true;
        expected(0, 2) = expected(2, 0) = true;
        expected(3, 4) = expected(4, 3) = true;
        expected(3, 5) = expected(5, 3) = true;
        CHECK(expected == MSF);
    }
}

TEST_CASE("AdjacencyMatrix/tree_directed_away_from", "[core][util][unit]")
{
    SECTION("no edges")
    {
        AdjacencyMatrix tree(1);
        AdjacencyMatrix directed_tree = tree.tree_directed_away_from(SmallBitset(1UL));
        CHECK(tree == directed_tree);
    }

    SECTION("single edge")
    {
        AdjacencyMatrix tree(2);
        tree(0, 1) = tree(1, 0) = true;
        SECTION("root 0")
        {
            const AdjacencyMatrix directed_tree = tree.tree_directed_away_from(SmallBitset(1UL));
            AdjacencyMatrix expected(2);
            expected(0, 1) = true;
            CHECK(expected == directed_tree);
        }
        SECTION("root 1")
        {
            const AdjacencyMatrix directed_tree = tree.tree_directed_away_from(SmallBitset(2UL));
            AdjacencyMatrix expected(2);
            expected(1, 0) = true;
            CHECK(expected == directed_tree);
        }
    }

    SECTION("multiple edges, one level")
    {
        /*    1
         *   /
         *  0-2
         *   \
         *    3
         */
        AdjacencyMatrix tree(4);
        tree(0, 1) = tree(1, 0) = true;
        tree(0, 2) = tree(2, 0) = true;
        tree(0, 3) = tree(3, 0) = true;

        SECTION("root 0")
        {
            const AdjacencyMatrix directed_tree = tree.tree_directed_away_from(SmallBitset(1UL));
            AdjacencyMatrix expected(4);
            expected(0, 1) = true;
            expected(0, 2) = true;
            expected(0, 3) = true;
            CHECK(expected == directed_tree);
        }

        SECTION("root 1")
        {
            const AdjacencyMatrix directed_tree = tree.tree_directed_away_from(SmallBitset(2UL));
            AdjacencyMatrix expected(4);
            expected(1, 0) = true;
            expected(0, 2) = true;
            expected(0, 3) = true;
            CHECK(expected == directed_tree);
        }

        SECTION("root 2")
        {
            const AdjacencyMatrix directed_tree = tree.tree_directed_away_from(SmallBitset(1UL << 2));
            AdjacencyMatrix expected(4);
            expected(2, 0) = true;
            expected(0, 1) = true;
            expected(0, 3) = true;
            CHECK(expected == directed_tree);
        }

        SECTION("root 3")
        {
            const AdjacencyMatrix directed_tree = tree.tree_directed_away_from(SmallBitset(1UL << 3));
            AdjacencyMatrix expected(4);
            expected(3, 0) = true;
            expected(0, 1) = true;
            expected(0, 2) = true;
            CHECK(expected == directed_tree);
        }
    }

    SECTION("simple chain")
    {
        AdjacencyMatrix tree(4);
        tree(0, 1) = tree(1, 0) = true;
        tree(1, 2) = tree(2, 1) = true;
        tree(2, 3) = tree(3, 2) = true;

        SECTION("root 0")
        {
            const AdjacencyMatrix directed_tree = tree.tree_directed_away_from(SmallBitset(1UL));
            AdjacencyMatrix expected(4);
            expected(0, 1) = true;
            expected(1, 2) = true;
            expected(2, 3) = true;
            CHECK(expected == directed_tree);
        }

        SECTION("root 1")
        {
            const AdjacencyMatrix directed_tree = tree.tree_directed_away_from(SmallBitset(2UL));
            AdjacencyMatrix expected(4);
            expected(1, 0) = true;
            expected(1, 2) = true;
            expected(2, 3) = true;
            CHECK(expected == directed_tree);
        }

        SECTION("root 2")
        {
            const AdjacencyMatrix directed_tree = tree.tree_directed_away_from(SmallBitset(1UL << 2));
            AdjacencyMatrix expected(4);
            expected(1, 0) = true;
            expected(2, 1) = true;
            expected(2, 3) = true;
            CHECK(expected == directed_tree);
        }

        SECTION("root 3")
        {
            const AdjacencyMatrix directed_tree = tree.tree_directed_away_from(SmallBitset(1UL << 3));
            AdjacencyMatrix expected(4);
            expected(1, 0) = true;
            expected(2, 1) = true;
            expected(3, 2) = true;
            CHECK(expected == directed_tree);
        }
    }
}

TEST_CASE("AdjacencyMatrix/Matrix output", "[core][util][unit]")
{
    AdjacencyMatrix adj_mat(4);
    std::ostringstream out;

    SECTION("empty matrix output")
    {
        out << adj_mat;
        const std::string expected = "\
Adjacency Matrix\n\
0000\n\
0000\n\
0000\n\
0000\
";
        REQUIRE(out.str() == expected);
    }

    SECTION("non-empty matrix output")
    {
        adj_mat(0, 1) = true;
        adj_mat(2, 3) = adj_mat(3, 2) = true;
        out << adj_mat;

        std::ostringstream expected;
        expected << "\
Adjacency Matrix\n\
0010\n\
0000\n\
1000\n\
0100\
";
        REQUIRE(out.str() == expected.str());
    }
}
