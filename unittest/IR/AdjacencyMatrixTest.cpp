#include "catch.hpp"

#include "IR/QueryGraph.hpp"
#include "catalog/Schema.hpp"
#include "catalog/Type.hpp"
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "testutil.hpp"
#include "util/ADT.hpp"
#include "util/StringPool.hpp"

using namespace db;

/*======================================================================================================================
 * Helper funtctions for test setup.
 *====================================================================================================================*/

namespace {

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

}

/*======================================================================================================================
 * Test Adjacency Matrix.
 *====================================================================================================================*/

TEST_CASE("AdjacencyMatrix/Standalone Matrix", "[unit]")
{
    AdjacencyMatrix adj_mat;

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
    adj_mat.set(0, 2);
    adj_mat.set(2, 0);

    adj_mat.set(0, 3);
    adj_mat.set(3, 0);

    adj_mat.set_bidirectional(1, 3);

    adj_mat.set_bidirectional(2, 3);

    SECTION("check edges")
    {
        /* Positive checks. */
        REQUIRE(adj_mat.get(0, 2));
        REQUIRE(adj_mat.get(2, 0));

        REQUIRE(adj_mat.get(0, 3));
        REQUIRE(adj_mat.get(3, 0));

        REQUIRE(adj_mat.get(1, 3));
        REQUIRE(adj_mat.get(3, 1));

        REQUIRE(adj_mat.get(2, 3));
        REQUIRE(adj_mat.get(3, 2));

        /* Negative checks. */
        REQUIRE_FALSE(adj_mat.get(0, 1));
        REQUIRE_FALSE(adj_mat.get(13, 9));
        REQUIRE_FALSE(adj_mat.get(63, 2));
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

TEST_CASE("AdjacencyMatrix/QueryGraph Matrix", "[unit]")
{
    /* Get Catalog and create new database to use for unit testing. */
    Catalog::Clear();
    Catalog &Cat = Catalog::Get();
    auto &db = Cat.add_database("mydb");
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

    /* Create adjacency matrix. */
    const char *query = "SELECT * \
                         FROM A, B, C, D \
                         WHERE A.id = C.aid AND A.id = D.aid AND B.id = D.bid AND C.id = D.cid;";
    auto stmt = as<const SelectStmt>(get_Stmt(query));
    auto query_graph = QueryGraph::Build(*stmt);

    AdjacencyMatrix adj_mat(*query_graph);

    /* Mapping of relation name to SmallBitset of data source ID. */
    std::unordered_map<const char *, SmallBitset> map;
    for (auto ds : query_graph->sources()) {
        auto tbl = cast<const BaseTable>(ds);
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

    delete stmt;
}

TEST_CASE("AdjacencyMatrix", "[unit]")
{
    AdjacencyMatrix M;

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
    M.set_bidirectional(0, 2);
    M.set_bidirectional(0, 3);

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
