#include "catch.hpp"

#include "IR/QueryGraph.hpp"
#include "IR/QueryGraph.cpp"
#include "catalog/Schema.hpp"
#include "catalog/Type.hpp"
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "testutil.hpp"
#include "util/ADT.hpp"

using namespace db;

/*======================================================================================================================
 * Helper funtctions for test setup.
 *====================================================================================================================*/

namespace {

    Stmt *get_Stmt(const char *sql) {
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

    bool find_Expr(const std::vector<const Expr *> &vec, const Expr &expr) {
        auto end = vec.end();
        for (auto begin = vec.begin(); begin != end; ++begin) {
            if (expr.operator==(**begin))
                return true;
        }
        return false;
    }

    bool find_Proj(const std::vector<std::pair<const Expr *, const char *>> &vec,
                   std::pair<const Expr *, const char*> &p) {
        auto end = vec.end();
        for (auto begin = vec.begin(); begin != end; ++begin) {
            auto pair = *begin;
            if (p.first->operator==(*(pair.first)) and p.second == pair.second)
                return true;
        }
        return false;
    }

    DataSource *find_Source(const std::vector<DataSource *> &vec, const char * alias) {
        auto end = vec.end();
        for (auto begin = vec.begin(); begin != end; ++begin) {
            auto source = *begin;
            if (source->alias() == alias)
                return source;
        }
        return nullptr;
    }

    Join *find_Join(const std::vector<Join *> &vec, const char * alias1, const char * alias2) {
        auto end = vec.end();
        for (auto begin = vec.begin(); begin != end; ++begin) {
            auto join = *begin;
            if (find_Source(join->sources(), alias1) and find_Source(join->sources(), alias2))
                return join;
        }
        return nullptr;
    }

    bool find_OrderBy(const std::vector<std::pair<const Expr *, bool>> &vec, std::pair<const Expr *, bool> &p) {
        auto end = vec.end();
        for (auto begin = vec.begin(); begin != end; ++begin) {
            auto pair = *begin;
            if (p.first->operator==(*(pair.first)) and p.second == pair.second)
                return true;
        }
        return false;
    }

}

/*======================================================================================================================
 * Test DataSource.
 *====================================================================================================================*/

TEST_CASE("DataSource", "[core][IR][unit]")
{
    DataSource ds(1, "one");

    Position pos("test");
    Designator DA(Token(pos, "A", TK_IDENTIFIER));
    Designator DB(Token(pos, "B", TK_IDENTIFIER));

    cnf::Predicate PA = cnf::Predicate::Positive(&DA);
    cnf::Predicate PB = cnf::Predicate::Positive(&DB);

    cnf::Clause CA({PA});
    cnf::Clause CB({PB});

    cnf::CNF A({CA}); // A
    cnf::CNF B({CB}); // B

    SECTION("check initial values") {
        REQUIRE(ds.id() == 1);
        REQUIRE(streq(ds.alias(),"one"));
        REQUIRE_FALSE(contains(ds.filter(), CA));
        REQUIRE_FALSE(contains(ds.filter(), CB));
        REQUIRE(ds.joins().empty());
    }

    SECTION("check added filter and join")
    {
        ds.update_filter(A);
        db::Join joi(A, std::vector<DataSource(*)>());
        ds.add_join(&joi);
        REQUIRE(contains(ds.filter(), CA));
        REQUIRE_FALSE(contains(ds.filter(), CB));
        REQUIRE(ds.joins().size() == 1);
    }

    SECTION("check two added filters")
    {
        ds.update_filter(A);
        ds.update_filter(B);
        REQUIRE(contains(ds.filter(), CA));
        REQUIRE(contains(ds.filter(), CB));
    }
}

/*======================================================================================================================
 * Test AdjacencyMatrix.
 *====================================================================================================================*/

TEST_CASE("AdjacencyMatrix/Standalone Matrix", "[core][IR][unit]")
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

TEST_CASE("AdjacencyMatrix/QueryGraph Matrix", "[core][IR][unit]")
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

TEST_CASE("AdjacencyMatrix", "[core][IR][unit]")
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

TEST_CASE("AdjacencyMatrix/QueryGraph Matrix Negative", "[core][IR][unit]")
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
        auto stmt = as<const SelectStmt>(get_Stmt(query));
        auto query_graph = QueryGraph::Build(*stmt);

        delete stmt;

        REQUIRE_THROWS_AS(AdjacencyMatrix(*query_graph), std::invalid_argument);
    }

    SECTION("create no join")
    {
        /* Create adjacency matrix. */
        const char *query = "SELECT * \
                         FROM A \
                         WHERE A.id = 0;";
        auto stmt = as<const SelectStmt>(get_Stmt(query));
        auto query_graph = QueryGraph::Build(*stmt);
        AdjacencyMatrix adj_mat(*query_graph);

        delete stmt;

        REQUIRE_FALSE(adj_mat.get(0, 0));
    }
}

TEST_CASE("AdjacencyMatrix/Matrix output", "[core][IR][unit]")
{
    AdjacencyMatrix adj_mat;
    std::stringstream out;

    SECTION("empty matrix output")
    {
        out << adj_mat;

        std::string out_str("Adjacency Matrix");
        for (size_t i = 0; i < SmallBitset::CAPACITY; ++i)
            out_str += '\n' + std::string(SmallBitset::CAPACITY, '0');

        REQUIRE(out.str() == out_str);
    }

    SECTION("non-empty matrix output")
    {
        adj_mat.set(0,1);
        adj_mat.set_bidirectional(3,2);
        out << adj_mat;

        std::string out_str("Adjacency Matrix");
        out_str += '\n' + std::string(SmallBitset::CAPACITY - 4, '0') + "0010";
        out_str += '\n' + std::string(SmallBitset::CAPACITY - 4, '0') + "0000";
        out_str += '\n' + std::string(SmallBitset::CAPACITY - 4, '0') + "1000";
        out_str += '\n' + std::string(SmallBitset::CAPACITY - 4, '0') + "0100";
        for (size_t i = 4; i < SmallBitset::CAPACITY; ++i)
            out_str += '\n' + std::string(SmallBitset::CAPACITY, '0');

        REQUIRE(out.str() == out_str);
    }
}

/*======================================================================================================================
 * Test GetTables.
 *====================================================================================================================*/

TEST_CASE("GetTables", "[core][IR][unit]")
{
    Position pos("test");
    Token dot(pos, ".", TK_DOT);
    Designator* A = new Designator(dot, Token(pos, "A", TK_IDENTIFIER),
                                  Token(pos, "id", TK_IDENTIFIER));
    Designator* B = new Designator(dot, Token(pos, "B", TK_IDENTIFIER),
                                  Token(pos, "id", TK_IDENTIFIER));

    SECTION("test Designator")
    {
        cnf::Predicate PA = cnf::Predicate::Positive(A);
        cnf::Predicate PB = cnf::Predicate::Positive(B);

        cnf::Clause CEmpty({});
        cnf::Clause CA({PA});
        cnf::Clause CAB({PA, PB});

        auto tableEmpty = get_tables(CEmpty);
        auto tableA = get_tables(CA);
        auto tablesAB = get_tables(CAB);

        delete A;
        delete B;

        REQUIRE(tableEmpty.empty());

        REQUIRE(tableA.size() == 1);
        REQUIRE(tableA.find("A") != tableA.end());

        REQUIRE(tablesAB.size() == 2);
        REQUIRE(tablesAB.find("A") != tablesAB.end());
        REQUIRE(tablesAB.find("B") != tablesAB.end());
    }

    SECTION("test FnApplicationExpr")
    {
        Designator* min = new Designator(dot, Token(pos, "EOF", TK_EOF),
                                         Token(pos, "MIN", TK_IDENTIFIER));
        Token lpar(pos, "(", TK_LPAR);

        SECTION("test FnApplicationExpr with no argument")
        {
            FnApplicationExpr aggEmpty(lpar, min, std::vector<Expr*>());
            cnf::Clause CEmpty({cnf::Predicate::Positive(&aggEmpty)});
            auto tableEmpty = get_tables(CEmpty);

            delete A;
            delete B;
            //delete min;

            REQUIRE(tableEmpty.size() == 1);
            REQUIRE(tableEmpty.find("EOF") != tableEmpty.end());
        }

        SECTION("test FnApplicationExpr with one argument")
        {
            FnApplicationExpr aggA(lpar, min, std::vector<Expr*>{A});
            cnf::Clause CA({cnf::Predicate::Positive(&aggA)});
            auto tableA = get_tables(CA);

            //delete A;
            delete B;
            //delete min;

            REQUIRE(tableA.size() == 2);
            REQUIRE(tableA.find("A") != tableA.end());
            REQUIRE(tableA.find("EOF") != tableA.end());
        }

        SECTION("test FnApplicationExpr with two arguments") {
            FnApplicationExpr aggAB(lpar, min, std::vector<Expr *>{A, B});
            cnf::Clause CAB({cnf::Predicate::Positive(&aggAB)});
            auto tablesAB = get_tables(CAB);

            //delete A;
            //delete B;
            //delete min;

            REQUIRE(tablesAB.size() == 3);
            REQUIRE(tablesAB.find("A") != tablesAB.end());
            REQUIRE(tablesAB.find("B") != tablesAB.end());
            REQUIRE(tablesAB.find("EOF") != tablesAB.end());
        }
    }

    SECTION("test composed expression types")
    {
        Constant* const0 = new Constant(Token(pos, "0", TK_Int));
        Constant* const1 = new Constant(Token(pos, "1", TK_Int));
        Token plus(pos, "+", TK_PLUS);

        SECTION("test UnaryExpr") {
            Token min(pos, "-", TK_MINUS);

            UnaryExpr unaryConst(min, const0);
            UnaryExpr unaryA(min, A);

            cnf::Clause CEmpty({cnf::Predicate::Positive(&unaryConst)});
            cnf::Clause CA({cnf::Predicate::Positive(&unaryA)});

            auto tableEmpty = get_tables(CEmpty);
            auto tableA = get_tables(CA);

            delete B;
            delete const1;

            REQUIRE(tableEmpty.empty());

            REQUIRE(tableA.size() == 1);
            REQUIRE(tableA.find("A") != tableA.end());
        }

        SECTION("test BinaryExpr with constants") {
            BinaryExpr binaryEmpty(plus, const0, const1);
            cnf::Clause CEmpty({cnf::Predicate::Positive(&binaryEmpty)});
            auto tableEmpty = get_tables(CEmpty);

            delete A,
            delete B;

            REQUIRE(tableEmpty.empty());
        }

        SECTION("test BinaryExpr with one table") {
            BinaryExpr binaryA(plus, A, const0);
            cnf::Clause CA({cnf::Predicate::Positive(&binaryA)});
            auto tableA = get_tables(CA);

            delete B;
            delete const1;

            REQUIRE(tableA.size() == 1);
            REQUIRE(tableA.find("A") != tableA.end());
        }

        SECTION("test BinaryExpr with two tables") {
            BinaryExpr binaryAB(plus, A, B);
            cnf::Clause CAB({cnf::Predicate::Positive(&binaryAB)});
            auto tablesAB = get_tables(CAB);

            delete const0;
            delete const1;

            REQUIRE(tablesAB.size() == 2);
            REQUIRE(tablesAB.find("A") != tablesAB.end());
            REQUIRE(tablesAB.find("B") != tablesAB.end());
        }
    }
}

/*======================================================================================================================
 * Test GetAggregates.
 *====================================================================================================================*/

TEST_CASE("GetAggregates", "[core][IR][unit]")
{
    Catalog &C = Catalog::Get();
    auto c_max = C.pool("MAX");
    auto c_sum = C.pool("SUM");
    auto c_A = C.pool("A");
    auto c_id = C.pool("id");
    auto c_val = C.pool("val");
    auto c_bool = C.pool("bool");
    auto c_pos = C.pool("pos");
    auto c_dot = C.pool(".");
    auto c_lpar = C.pool("(");

    // create dummy db with table A and attributes A.id, A.val and A.bool
    auto &DB = C.add_database("GetAggregates_DB");
    C.set_database_in_use(DB);
    auto &table = DB.add_table(c_A);
    table.push_back(c_id, Type::Get_Integer(Type::TY_Vector, 4));
    table.push_back(c_val, Type::Get_Integer(Type::TY_Vector, 4));
    table.push_back(c_bool, Type::Get_Boolean(Type::TY_Vector));

    Position pos(c_pos);
    Token dot(pos, c_dot, TK_DOT);
    Designator* max = new Designator(Token(pos, c_max, TK_IDENTIFIER));
    Designator* sum = new Designator(Token(pos, c_sum, TK_IDENTIFIER));
    Designator* A_id_1 = new Designator(dot, Token(pos, c_A, TK_IDENTIFIER),
                                       Token(pos, c_id, TK_IDENTIFIER));
    Designator* A_id_2 = new Designator(dot, Token(pos, c_A, TK_IDENTIFIER),
                                       Token(pos, c_id, TK_IDENTIFIER));
    Token lpar(pos, c_lpar, TK_LPAR);
    FnApplicationExpr max_A_id(lpar, max, std::vector<Expr *>{A_id_1});
    FnApplicationExpr sum_A_id(lpar, sum, std::vector<Expr *>{A_id_2});

    SECTION("test SelectStmt")
    {
        SECTION("test SelectClause without aggregates") {
            const char *query = "SELECT A.id \
                                 FROM A;";
            auto stmt = get_Stmt(query);
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.empty());

            delete stmt;
        }

        SECTION("test SelectClause with one aggregate") {
            const char *query = "SELECT MAX(A.id) \
                                 FROM A;";
            auto stmt = get_Stmt(query);
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.size() == 1);
            REQUIRE(find_Expr(aggregates, max_A_id));

            delete stmt;
        }

        SECTION("test SelectClause with two aggregates") {
            const char *query = "SELECT MAX(A.id), SUM(A.id) \
                                 FROM A;";
            auto stmt = get_Stmt(query);
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.size() == 2);
            REQUIRE(find_Expr(aggregates, max_A_id));
            REQUIRE(find_Expr(aggregates, sum_A_id));

            delete stmt;
        }

        SECTION("test HavingClause without aggregates") {
            const char *query = "SELECT 1 \
                                 FROM A \
                                 GROUP BY A.bool \
                                 HAVING NOT A.bool;";
            auto stmt = get_Stmt(query);
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.empty());

            delete stmt;
        }

        SECTION("test HavingClause with one aggregate") {
            const char *query = "SELECT 1 \
                                 FROM A \
                                 GROUP BY A.bool \
                                 HAVING MAX(A.id) = 1;";
            auto stmt = get_Stmt(query);
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.size() == 1);
            REQUIRE(find_Expr(aggregates, max_A_id));

            delete stmt;
        }

        SECTION("test HavingClause with two aggregates") {
            const char *query = "SELECT 1 \
                                 FROM A \
                                 GROUP BY A.bool \
                                 HAVING MAX(A.id) = 1 AND SUM(A.id) != 1;";
            auto stmt = get_Stmt(query);
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.size() == 2);
            REQUIRE(find_Expr(aggregates, max_A_id));
            REQUIRE(find_Expr(aggregates, sum_A_id));

            delete stmt;
        }

        SECTION("test OrderByClause without aggregates") {
            const char *query = "SELECT * \
                                 FROM A \
                                 GROUP BY A.val \
                                 ORDER BY A.val;";
            auto stmt = get_Stmt(query);
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.empty());

            delete stmt;
        }

        SECTION("test OrderByClause with one aggregate") {
            const char *query = "SELECT * \
                                 FROM A \
                                 GROUP BY A.val \
                                 ORDER BY MAX(A.id);";
            auto stmt = get_Stmt(query);
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.size() == 1);
            REQUIRE(find_Expr(aggregates, max_A_id));

            delete stmt;
        }

        SECTION("test OrderByClause with two aggregates") {
            const char *query = "SELECT * \
                                 FROM A \
                                 GROUP BY A.val \
                                 ORDER BY MAX(A.id) ASC, SUM(A.id) DESC;";
            auto stmt = get_Stmt(query);
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.size() == 2);
            REQUIRE(find_Expr(aggregates, max_A_id));
            REQUIRE(find_Expr(aggregates, sum_A_id));

            delete stmt;
        }

        SECTION("test no aggregate function") {
            const char *query = "SELECT ISNULL(A.val) \
                                 FROM A;";
            auto stmt = get_Stmt(query);
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.empty());

            delete stmt;
        }

        SECTION("test duplicate aggregate") {
            const char *query = "SELECT MAX(A.id), MAX(A.id) \
                                 FROM A;";
            auto stmt = get_Stmt(query);
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.size() == 1);
            REQUIRE(find_Expr(aggregates, max_A_id));

            delete stmt;
        }
    }

    Catalog::Clear();
}

TEST_CASE("GetAggregates/no aggregate possible", "[IR][unit]")
{
    SECTION("test with database")
    {
        Catalog &C = Catalog::Get();
        auto c_A = C.pool("A");
        auto c_id = C.pool("id");

        // create dummy db with table A and attribute A.id
        auto &DB = C.add_database("GetAggregates_DB");
        C.set_database_in_use(DB);
        auto &table = DB.add_table(c_A);
        table.push_back(c_id, Type::Get_Integer(Type::TY_Vector, 4));

        SECTION("test FromClause") {
            const char *query = "SELECT * \
                                 FROM A;";
            auto stmt = get_Stmt(query);
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.empty());

            delete stmt;
        }

        SECTION("test WhereClause") {
            const char *query = "SELECT * \
                                 FROM A \
                                 WHERE A.id = 1;";
            auto stmt = get_Stmt(query);
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.empty());

            delete stmt;
        }

        SECTION("test GroupByClause") {
            const char *query = "SELECT * \
                                 FROM A \
                                 GROUP BY A.id;";
            auto stmt = get_Stmt(query);
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.empty());

            delete stmt;
        }

        SECTION("test WhereClause") {
            const char *query = "SELECT * \
                                 FROM A \
                                 LIMIT 5;";
            auto stmt = get_Stmt(query);
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.empty());

            delete stmt;
        }

        SECTION("test InsertStmt") {
            const char *query = "INSERT INTO A VALUES (1);";
            auto stmt = get_Stmt(query);
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.empty());

            delete stmt;
        }

        SECTION("test UpdateStmt") {
            const char *query = "UPDATE A SET id = 1 \
                                 WHERE A.id != 1;";
            auto stmt = get_Stmt(query);
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.empty());

            delete stmt;
        }

        SECTION("test DeleteStmt") {
            const char *query = "DELETE FROM A \
                                 WHERE A.id = 1;";
            auto stmt = get_Stmt(query);
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.empty());

            delete stmt;
        }
    }

    SECTION("test EmptyStmt") {
        const char *query = ";";
        auto stmt = get_Stmt(query);
        auto aggregates = get_aggregates(*stmt);

        REQUIRE(aggregates.empty());

        delete stmt;
    }

    SECTION("test CreateDatabaseStmt") {
        const char *query = "CREATE DATABASE GetAggregates_DB;";
        auto stmt = get_Stmt(query);
        auto aggregates = get_aggregates(*stmt);

        REQUIRE(aggregates.empty());

        delete stmt;
    }

    SECTION("test UseDatabaseStmt") {
        Catalog &C = Catalog::Get();
        C.add_database("GetAggregates_DB");

        const char *query = "USE GetAggregates_DB;";
        auto stmt = get_Stmt(query);
        auto aggregates = get_aggregates(*stmt);

        REQUIRE(aggregates.empty());

        delete stmt;
    }

    SECTION("test CreateTableStmt") {
        Catalog &C = Catalog::Get();
        auto &DB = C.add_database("GetAggregates_DB");
        C.set_database_in_use(DB);

        const char *query = "CREATE TABLE A ( \
                                 id INT(4) \
                             );";
        auto stmt = get_Stmt(query);
        auto aggregates = get_aggregates(*stmt);

        REQUIRE(aggregates.empty());

        delete stmt;
    }

    Catalog::Clear();
}

/*======================================================================================================================
 * Test GraphBuilder.
 *====================================================================================================================*/

TEST_CASE("GraphBuilder/SelectStmt", "[core][IR][unit]")
{
    Catalog &C = Catalog::Get();
    auto c_avg = C.pool("AVG");
    auto c_min = C.pool("MIN");
    auto c_A = C.pool("A");
    auto c_B = C.pool("B");
    auto c_C = C.pool("C");
    auto c_tbl = C.pool("tbl");
    auto c_id = C.pool("id");
    auto c_val = C.pool("val");
    auto c_bool = C.pool("bool");
    auto c_A_id = C.pool("A_id");
    auto c_A_val = C.pool("A_val");
    auto c_pos = C.pool("pos");
    auto c_dot = C.pool(".");
    auto c_eq = C.pool("==");
    auto c_no = C.pool("NOT");
    auto c_lpar = C.pool("(");
    auto c_0 = C.pool("0");
    auto c_1 = C.pool("1");

    // create dummy db with tables A, B and C and attributes id, val and bool
    auto &DB = C.add_database("GraphBuilder_DB");
    C.set_database_in_use(DB);

    auto &tableA = DB.add_table(c_A);
    tableA.push_back(c_id, Type::Get_Integer(Type::TY_Vector, 4));
    tableA.push_back(c_val, Type::Get_Integer(Type::TY_Vector, 4));
    tableA.push_back(c_bool, Type::Get_Boolean(Type::TY_Vector));

    auto &tableB = DB.add_table(c_B);
    tableB.push_back(c_id, Type::Get_Integer(Type::TY_Vector, 4));
    tableB.push_back(c_val, Type::Get_Integer(Type::TY_Vector, 4));
    tableB.push_back(c_bool, Type::Get_Boolean(Type::TY_Vector));

    auto &tableC = DB.add_table(c_C);
    tableC.push_back(c_id, Type::Get_Integer(Type::TY_Vector, 4));
    tableC.push_back(c_val, Type::Get_Integer(Type::TY_Vector, 4));
    tableC.push_back(c_bool, Type::Get_Boolean(Type::TY_Vector));

    // create common objects
    Position pos(c_pos);
    Token dot(pos, c_dot, TK_DOT);

    SECTION("test projections")
    {
        Designator A_id(dot, Token(pos, c_A, TK_IDENTIFIER),
                        Token(pos, c_id, TK_IDENTIFIER));
        Designator A_val(dot, Token(pos, c_A, TK_IDENTIFIER),
                        Token(pos, c_val, TK_IDENTIFIER));
        Constant const0(Token(pos, c_0, TK_Int));
        std::pair<const Expr *, const char *> p_id_empty{&A_id, NULL};
        std::pair<const Expr *, const char *> p_val_empty{&A_val, NULL};
        std::pair<const Expr *, const char *> p_id_Aid{&A_id, c_A_id};
        std::pair<const Expr *, const char *> p_val_Aval{&A_val, c_A_val};
        std::pair<const Expr *, const char *> p_const0_empty{&const0, NULL};

        SECTION("test constant projection")
        {
            const char *query = "SELECT 0;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto projections = graph->projections();

            REQUIRE(graph->sources().empty());
            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);

            REQUIRE(projections.size() == 1);
            REQUIRE(find_Proj(projections, p_const0_empty));
            REQUIRE_FALSE(graph->projection_is_anti());

            delete stmt;
        }

        SECTION("test one projection")
        {
            const char *query = "SELECT A.id \
                                 FROM A;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();
            auto projections = graph->projections();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(projections.size() == 1);
            REQUIRE(find_Proj(projections, p_id_empty));
            REQUIRE_FALSE(graph->projection_is_anti());

            delete stmt;
        }

        SECTION("test one projection renamed")
        {
            const char *query = "SELECT A.id AS A_id\
                                 FROM A;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();
            auto projections = graph->projections();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(projections.size() == 1);
            REQUIRE(find_Proj(projections, p_id_Aid));
            REQUIRE_FALSE(graph->projection_is_anti());

            delete stmt;
        }

        SECTION("test two projections")
        {
            const char *query = "SELECT A.id, A.val \
                                 FROM A;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();
            auto projections = graph->projections();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(projections.size() == 2);
            REQUIRE(find_Proj(projections, p_id_empty));
            REQUIRE(find_Proj(projections, p_val_empty));
            REQUIRE_FALSE(graph->projection_is_anti());

            delete stmt;
        }

        SECTION("test two projections renamed")
        {
            const char *query = "SELECT A.id AS A_id, A.val AS A_val\
                                 FROM A;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();
            auto projections = graph->projections();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(projections.size() == 2);
            REQUIRE(find_Proj(projections, p_id_Aid));
            REQUIRE(find_Proj(projections, p_val_Aval));
            REQUIRE_FALSE(graph->projection_is_anti());

            delete stmt;
        }

        SECTION("test projection with star")
        {
            const char *query = "SELECT * \
                                 FROM A;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();
            auto projections = graph->projections();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(projections.empty());
            REQUIRE(graph->projection_is_anti());

            delete stmt;
        }
    }

    SECTION("test sources and joins") {
        auto stmt_A_id_eq_B_id = as<SelectStmt>(get_Stmt("SELECT * FROM A, B WHERE A.id = B.id;"));
        auto cnf_A_id_eq_B_id = cnf::to_CNF(*as<WhereClause>(stmt_A_id_eq_B_id->where)->where);

        auto stmt_B_val_eq_C_val = as<SelectStmt>(get_Stmt("SELECT * FROM B, C WHERE B.val = C.val;"));
        auto cnf_B_val_eq_C_val = cnf::to_CNF(*as<WhereClause>(stmt_B_val_eq_C_val->where)->where);

        auto stmt_A_id_eq_C_id = as<SelectStmt>(get_Stmt("SELECT * FROM A, C WHERE A.id = C.id;"));
        auto cnf_A_id_eq_C_id = cnf::to_CNF(*as<WhereClause>(stmt_A_id_eq_C_id->where)->where);

        auto stmt_A_val_eq_const0 = as<SelectStmt>(get_Stmt("SELECT * FROM A WHERE A.val = 0;"));
        auto cnf_A_val_eq_const0 = cnf::to_CNF(*as<WhereClause>(stmt_A_val_eq_const0->where)->where);

        auto stmt_tbl_val_eq_const1 = as<SelectStmt>(get_Stmt("SELECT * FROM A AS tbl WHERE tbl.val = 1;"));
        auto cnf_tbl_val_eq_const1 = cnf::to_CNF(*as<WhereClause>(stmt_tbl_val_eq_const1->where)->where);

        auto stmt_not_A_bool = as<SelectStmt>(get_Stmt("SELECT * FROM A WHERE NOT A.bool;"));
        auto cnf_not_A_bool = cnf::to_CNF(*as<WhereClause>(stmt_not_A_bool->where)->where);

        auto stmt_constTrue = as<SelectStmt>(get_Stmt("SELECT * FROM A WHERE TRUE;"));
        auto cnf_constTrue = cnf::to_CNF(*as<WhereClause>(stmt_constTrue->where)->where);

        SECTION("test one source without filter")
        {
            const char *query = "SELECT * \
                                 FROM A;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().empty());
            REQUIRE(graph->projection_is_anti());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            delete stmt;
        }

        SECTION("test one source renamed without filter")
        {
            const char *query = "SELECT * \
                                 FROM A AS tbl;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().empty());
            REQUIRE(graph->projection_is_anti());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_tbl);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            delete stmt;
        }

        SECTION("test one source with one filter")
        {
            const char *query = "SELECT * \
                                 FROM A \
                                 WHERE A.val = 0;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().empty());
            REQUIRE(graph->projection_is_anti());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter() == cnf_A_val_eq_const0);

            delete stmt;
        }

        SECTION("test one source renamed with one filter")
        {
            const char *query = "SELECT * \
                                 FROM A AS tbl \
                                 WHERE tbl.val = 1;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().empty());
            REQUIRE(graph->projection_is_anti());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_tbl);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter() == cnf_tbl_val_eq_const1);

            delete stmt;
        }

        SECTION("test one source with two filters")
        {
            const char *query = "SELECT * \
                                 FROM A \
                                 WHERE A.val = 0 AND NOT A.bool;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().empty());
            REQUIRE(graph->projection_is_anti());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter() == cnf::operator&&(cnf_A_val_eq_const0, cnf_not_A_bool));

            delete stmt;
        }

        SECTION("test two sources with one binary join")
        {
            const char *query = "SELECT * \
                                 FROM A, B \
                                 WHERE A.id = B.id;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();
            auto joins = graph->joins();

            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().empty());
            REQUIRE(graph->projection_is_anti());

            REQUIRE(sources.size() == 2);

            auto sourceA = find_Source(sources, c_A);
            auto sourceB = find_Source(sources, c_B);

            REQUIRE(sourceA);
            REQUIRE(sourceB);
            REQUIRE(sourceA->joins().size() == 1);
            REQUIRE(sourceB->joins().size() == 1);
            REQUIRE(sourceA->filter().empty());
            REQUIRE(sourceB->filter().empty());

            REQUIRE(joins.size() == 1);
            REQUIRE(find_Source(joins[0]->sources(), c_A));
            REQUIRE(find_Source(joins[0]->sources(), c_B));

            REQUIRE(joins[0]->condition() == cnf_A_id_eq_B_id);

            delete stmt;
        }

        SECTION("test three sources with three binary joins")
        {
            const char *query = "SELECT * \
                                 FROM A, B, C \
                                 WHERE A.id = B.id AND B.val = C.val AND A.id = C.id;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();
            auto joins = graph->joins();

            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().empty());
            REQUIRE(graph->projection_is_anti());

            REQUIRE(sources.size() == 3);

            auto sourceA = find_Source(sources, c_A);
            auto sourceB = find_Source(sources, c_B);
            auto sourceC = find_Source(sources, c_C);

            REQUIRE(sourceA);
            REQUIRE(sourceB);
            REQUIRE(sourceC);
            REQUIRE(sourceA->joins().size() == 2);
            REQUIRE(sourceB->joins().size() == 2);
            REQUIRE(sourceC->joins().size() == 2);
            REQUIRE(sourceA->filter().empty());
            REQUIRE(sourceB->filter().empty());
            REQUIRE(sourceC->filter().empty());

            REQUIRE(joins.size() == 3);

            auto joinAB = find_Join(joins, c_A, c_B);
            auto joinBC = find_Join(joins, c_B, c_C);
            auto joinAC = find_Join(joins, c_A, c_C);

            REQUIRE(joinAB);
            REQUIRE(joinBC);
            REQUIRE(joinAC);
            REQUIRE(joinAB->condition() == cnf_A_id_eq_B_id);
            REQUIRE(joinBC->condition() == cnf_B_val_eq_C_val);
            REQUIRE(joinAC->condition() == cnf_A_id_eq_C_id);

            delete stmt;
        }

        SECTION("test one source with constant filter")
        {
            const char *query = "SELECT * \
                                 FROM A \
                                 WHERE TRUE;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().empty());
            REQUIRE(graph->projection_is_anti());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter() == cnf_constTrue);

            delete stmt;
        }

        SECTION("test two sources with constant filter")
        {
            const char *query = "SELECT * \
                                 FROM A, B \
                                 WHERE A.id = B.id AND TRUE;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();
            auto joins = graph->joins();

            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().empty());
            REQUIRE(graph->projection_is_anti());

            REQUIRE(sources.size() == 2);

            auto sourceA = find_Source(sources, c_A);
            auto sourceB = find_Source(sources, c_B);

            REQUIRE(sourceA);
            REQUIRE(sourceB);
            REQUIRE(sourceA->joins().size() == 1);
            REQUIRE(sourceB->joins().size() == 1);
            REQUIRE(sourceA->filter() == cnf_constTrue);
            REQUIRE(sourceB->filter() == cnf_constTrue);

            REQUIRE(joins.size() == 1);

            auto joinAB = find_Join(joins, c_A, c_B);

            REQUIRE(joinAB);
            REQUIRE(joinAB->condition() == cnf_A_id_eq_B_id);

            delete stmt;
        }

        delete stmt_A_id_eq_B_id;
        delete stmt_B_val_eq_C_val;
        delete stmt_A_id_eq_C_id;
        delete stmt_A_val_eq_const0;
        delete stmt_tbl_val_eq_const1;
        delete stmt_not_A_bool;
        delete stmt_constTrue;
    }

    SECTION("test group by and aggregates")
    {
        Token lpar(pos, c_lpar, TK_LPAR);
        Designator* avg = new Designator(Token(pos, c_avg, TK_IDENTIFIER));
        Designator* min = new Designator(Token(pos, c_min, TK_IDENTIFIER));
        Designator *A_id = new Designator(dot, Token(pos, c_A, TK_IDENTIFIER),
                                          Token(pos, c_id, TK_IDENTIFIER));
        Designator *A_id_2 = new Designator(dot, Token(pos, c_A, TK_IDENTIFIER),
                                            Token(pos, c_id, TK_IDENTIFIER));
        Designator A_val(dot, Token(pos, c_A, TK_IDENTIFIER),
                         Token(pos, c_val, TK_IDENTIFIER));
        FnApplicationExpr min_A_id(lpar, min, std::vector<Expr *>{A_id_2});
        FnApplicationExpr avg_A_id(lpar, avg, std::vector<Expr *>{A_id});

        auto stmt_A_val_eq_const0 = as<SelectStmt>(get_Stmt("SELECT * FROM A GROUP BY A.val HAVING A.val = 0;"));
        auto cnf_A_val_eq_const0 = cnf::to_CNF(*as<HavingClause>(stmt_A_val_eq_const0->having)->having);

        auto stmt_min_A_id_eq_const1 = as<SelectStmt>(
                get_Stmt("SELECT * FROM A GROUP BY A.val HAVING MIN(A.id) = 1;"));
        auto cnf_min_A_id_eq_const1 = cnf::to_CNF(*as<HavingClause>(stmt_min_A_id_eq_const1->having)->having);

        SECTION("test no grouping with aggregate")
        {
            const char *query = "SELECT AVG(A.id) \
                                 FROM A;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();
            auto group_by = graph->group_by();
            auto aggregates = graph->aggregates();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == 1);
            REQUIRE_FALSE(graph->projection_is_anti());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(group_by.empty());

            REQUIRE(aggregates.size() == 1);
            REQUIRE(find_Expr(aggregates, avg_A_id));

            delete stmt;
        }

        SECTION("test grouping with no aggregate")
        {
            const char *query = "SELECT * \
                                 FROM A \
                                 GROUP BY A.val;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();
            auto group_by = graph->group_by();
            auto aggregates = graph->aggregates();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().empty());
            REQUIRE(graph->projection_is_anti());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(group_by.size() == 1);
            REQUIRE(find_Expr(group_by, A_val));

            REQUIRE(aggregates.empty());

            delete stmt;
        }

        SECTION("test grouping with aggregate")
        {
            const char *query = "SELECT AVG(A.id) \
                                 FROM A \
                                 GROUP BY A.val;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();
            auto group_by = graph->group_by();
            auto aggregates = graph->aggregates();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == 1);
            REQUIRE_FALSE(graph->projection_is_anti());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(group_by.size() == 1);
            REQUIRE(find_Expr(group_by, A_val));

            REQUIRE(aggregates.size() == 1);
            REQUIRE(find_Expr(aggregates, avg_A_id));

            delete stmt;
        }

        SECTION("test grouping with HAVING and no aggregate")
        {
            const char *query = "SELECT * \
                                 FROM A \
                                 GROUP BY A.val \
                                 HAVING A.val = 0;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            REQUIRE(graph->sources().size() == 1);
            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().empty());
            REQUIRE(graph->projection_is_anti());

            auto having = as<const Query>(graph->sources()[0]);
            REQUIRE(having);

            REQUIRE(having->joins().empty());
            REQUIRE(having->filter() == cnf_A_val_eq_const0);

            auto group_by = having->query_graph()->group_by();
            auto aggregates = having->query_graph()->aggregates();

            REQUIRE(group_by.size() == 1);
            REQUIRE(find_Expr(group_by, A_val));

            REQUIRE(aggregates.empty());

            delete stmt;
        }

        SECTION("test grouping with HAVING and aggregate")
        {
            const char *query = "SELECT * \
                                 FROM A \
                                 GROUP BY A.val \
                                 HAVING MIN(A.id) = 1;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            REQUIRE(graph->sources().size() == 1);
            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().empty());
            REQUIRE(graph->projection_is_anti());

            auto having = as<const Query>(graph->sources()[0]);
            REQUIRE(having);

            REQUIRE(having->joins().empty());
            REQUIRE(having->filter() == cnf_min_A_id_eq_const1);

            auto group_by = having->query_graph()->group_by();
            auto aggregates = having->query_graph()->aggregates();

            REQUIRE(group_by.size() == 1);
            REQUIRE(find_Expr(group_by, A_val));

            REQUIRE(aggregates.size() == 1);
            REQUIRE(find_Expr(aggregates, min_A_id));

            delete stmt;
        }

        delete stmt_A_val_eq_const0;
        delete stmt_min_A_id_eq_const1;
    }

    SECTION("test order by") {
        Designator A_id(dot, Token(pos, c_A, TK_IDENTIFIER),
                                          Token(pos, c_id, TK_IDENTIFIER));
        Designator A_val(dot, Token(pos, c_A, TK_IDENTIFIER),
                         Token(pos, c_val, TK_IDENTIFIER));
        std::pair<const Expr *, bool> p_id_ASC{&A_id, true};
        std::pair<const Expr *, bool> p_id_DESC{&A_id, false};
        std::pair<const Expr *, bool> p_val_ASC{&A_val, true};

        SECTION("test no order by") {
            const char *query = "SELECT * \
                                 FROM A;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();
            auto order_by = graph->order_by();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().empty());
            REQUIRE(graph->projection_is_anti());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(order_by.empty());

            delete stmt;
        }

        SECTION("test order by ASC implicit") {
            const char *query = "SELECT * \
                                 FROM A \
                                 ORDER BY A.id;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();
            auto order_by = graph->order_by();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().empty());
            REQUIRE(graph->projection_is_anti());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(order_by.size() == 1);
            REQUIRE(find_OrderBy(order_by, p_id_ASC));

            delete stmt;
        }

        SECTION("test order by ASC explicit") {
            const char *query = "SELECT * \
                                 FROM A \
                                 ORDER BY A.id ASC;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();
            auto order_by = graph->order_by();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().empty());
            REQUIRE(graph->projection_is_anti());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(order_by.size() == 1);
            REQUIRE(find_OrderBy(order_by, p_id_ASC));

            delete stmt;
        }

        SECTION("test order by DESC") {
            const char *query = "SELECT * \
                                 FROM A \
                                 ORDER BY A.id DESC;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();
            auto order_by = graph->order_by();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().empty());
            REQUIRE(graph->projection_is_anti());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(order_by.size() == 1);
            REQUIRE(find_OrderBy(order_by, p_id_DESC));

            delete stmt;
        }

        SECTION("test order by multiple") {
            const char *query = "SELECT * \
                                 FROM A \
                                 ORDER BY A.id DESC, A.val ASC;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();
            auto order_by = graph->order_by();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().empty());
            REQUIRE(graph->projection_is_anti());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(order_by.size() == 2);
            REQUIRE(find_OrderBy(order_by, p_id_DESC));
            REQUIRE(find_OrderBy(order_by, p_val_ASC));

            delete stmt;
        }
    }

    SECTION("test LIMIT clause")
    {
        SECTION("test limit without offset") {
            const char *query = "SELECT * \
                                 FROM A \
                                 LIMIT 5;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->projections().empty());
            REQUIRE(graph->projection_is_anti());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(graph->limit().limit == 5);
            REQUIRE(graph->limit().offset == 0);

            delete stmt;
        }

        SECTION("test limit with offset")
        {
            const char *query = "SELECT * \
                                 FROM A \
                                 LIMIT 5 OFFSET 10;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->projections().empty());
            REQUIRE(graph->projection_is_anti());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(graph->limit().limit == 5);
            REQUIRE(graph->limit().offset == 10);

            delete stmt;
        }
    }

    SECTION("test subqueries in FROM clause") {
        Token eq(pos, c_eq, TK_EQUAL);
        Token no(pos, c_no, TK_Not);
        Designator *A_id = new Designator(dot, Token(pos, c_A, TK_IDENTIFIER),
                                          Token(pos, c_id, TK_IDENTIFIER));
        Designator *A_id_2 = new Designator(dot, Token(pos, c_A, TK_IDENTIFIER),
                                            Token(pos, c_id, TK_IDENTIFIER));
        Designator *A_val = new Designator(dot, Token(pos, c_A, TK_IDENTIFIER),
                                           Token(pos, c_val, TK_IDENTIFIER));
        Designator *A_bool = new Designator(dot, Token(pos, c_A, TK_IDENTIFIER),
                                            Token(pos, c_bool, TK_IDENTIFIER));
        Designator *B_id = new Designator(dot, Token(pos, c_B, TK_IDENTIFIER),
                                          Token(pos, c_id, TK_IDENTIFIER));
        Designator *B_val = new Designator(dot, Token(pos, c_B, TK_IDENTIFIER),
                                           Token(pos, c_val, TK_IDENTIFIER));
        Designator *C_id = new Designator(dot, Token(pos, c_C, TK_IDENTIFIER),
                                          Token(pos, c_id, TK_IDENTIFIER));
        Designator *C_val = new Designator(dot, Token(pos, c_C, TK_IDENTIFIER),
                                           Token(pos, c_val, TK_IDENTIFIER));
        Designator *tbl_val = new Designator(dot, Token(pos, c_tbl, TK_IDENTIFIER),
                                             Token(pos, c_val, TK_IDENTIFIER));
        Constant *const0 = new Constant(Token(pos, c_0, TK_Int));
        Constant *const1 = new Constant(Token(pos, c_1, TK_Int));
        BinaryExpr A_id_eq_B_id(eq, A_id, B_id);
        BinaryExpr B_val_eq_C_val(eq, B_val, C_val);
        BinaryExpr A_id_eq_C_id(eq, A_id_2, C_id);
        BinaryExpr A_val_eq_const0(eq, A_val, const0);
        BinaryExpr tbl_val_eq_const1(eq, tbl_val, const1);
        UnaryExpr not_A_bool(no, A_bool);

        SECTION("test simple non-correlated subquery") {
            const char *query = "SELECT * \
                                 FROM (SELECT * \
                                       FROM A) AS tbl;";
            auto stmt = get_Stmt(query);
            auto graph = QueryGraph::Build(*stmt);

            auto sources = graph->sources();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().empty());
            REQUIRE(graph->projection_is_anti());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_tbl);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(is<const Query>(sources[0]));

            delete stmt;

        }
    }

    Catalog::Clear();
}