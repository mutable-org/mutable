#include "catch2/catch.hpp"

#include "IR/QueryGraph.hpp"
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "testutil.hpp"
#include <mutable/catalog/Catalog.hpp>
#include <mutable/catalog/Type.hpp>
#include <mutable/mutable.hpp>
#include <mutable/mutable.hpp>
#include <mutable/util/ADT.hpp>
#include <mutable/util/fn.hpp>
#include <set>
#include <unordered_set>
#include <vector>


using namespace m;
using namespace m::ast;


/*======================================================================================================================
 * Forward declarations
 *====================================================================================================================*/

std::vector<std::reference_wrapper<const ast::FnApplicationExpr>>
get_aggregates(const SelectStmt &stmt);


/*======================================================================================================================
 * Helper functions for test setup.
 *====================================================================================================================*/

namespace {

bool find_Expr(const std::vector<QueryGraph::group_type> &vec, const Expr &expr) {
    for (auto it = vec.begin(), end = vec.end(); it != end; ++it) {
        if (expr == it->first.get())
            return true;
    }
    return false;
}

bool find_Expr(const std::vector<std::reference_wrapper<const FnApplicationExpr>> &vec, const Expr &expr) {
    for (auto it = vec.begin(), end = vec.end(); it != end; ++it) {
        if (expr == it->get())
            return true;
    }
    return false;
}

bool find_Proj(const std::vector<std::pair<std::reference_wrapper<const Expr>, const char*>> &haystack,
               const std::pair<std::reference_wrapper<const Expr>, const char*> &needle)
{
    auto [needle_expr, needle_name] = needle;
    for (auto it = haystack.begin(), end = haystack.end(); it != end; ++it) {
        auto [expr, expr_name] = *it;
        if (expr_name == needle_name and expr.get() == needle_expr.get())
            return true;
    }
    return false;
}

DataSource * find_Source(const std::vector<std::reference_wrapper<DataSource>> &vec, const char *alias) {
    auto end = vec.end();
    for (auto it = vec.begin(); it != end; ++it) {
        auto &source = it->get();
        if (source.name() == alias)
            return &source;
    }
    return nullptr;
}

DataSource * find_Source(const std::vector<std::unique_ptr<DataSource>> &vec, const char *alias) {
    auto end = vec.end();
    for (auto it = vec.begin(); it != end; ++it) {
        auto source = it->get();
        if (source->name() == alias)
            return source;
    }
    return nullptr;
}

Join * find_Join(const std::vector<std::reference_wrapper<Join>> &vec, const char * alias1, const char * alias2) {
    auto end = vec.end();
    for (auto it = vec.begin(); it != end; ++it) {
        auto &join = it->get();
        if (find_Source(join.sources(), alias1) and find_Source(join.sources(), alias2))
            return &join;
    }
    return nullptr;
}

Join * find_Join(const std::vector<std::unique_ptr<Join>> &vec, const char * alias1, const char * alias2) {
    auto end = vec.end();
    for (auto it = vec.begin(); it != end; ++it) {
        auto join = it->get();
        if (find_Source(join->sources(), alias1) and find_Source(join->sources(), alias2))
            return join;
    }
    return nullptr;
}

bool find_OrderBy(const std::vector<std::pair<std::reference_wrapper<const ast::Expr>, bool>> &vec,
                  std::pair<std::reference_wrapper<const ast::Expr>, bool> &needle)
{
    auto [needle_expr, needle_direction] = needle;
    for (auto it = vec.begin(), end = vec.end(); it != end; ++it) {
        auto [expr, direction] = *it;
        if (needle_expr.get() == expr.get() and needle_direction == direction)
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
    auto graph = new QueryGraph();
    auto &ds = graph->add_source("one", Table("tbl"));

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
        REQUIRE(ds.id() == 0);
        REQUIRE(streq(ds.alias(),"one"));
        REQUIRE_FALSE(contains(ds.filter(), CA));
        REQUIRE_FALSE(contains(ds.filter(), CB));
        REQUIRE(ds.joins().empty());
    }

    SECTION("check added filter and join")
    {
        ds.update_filter(A);
        m::Join joi(A, std::vector<std::reference_wrapper<DataSource>>());
        ds.add_join(joi);
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

    delete graph;
}


TEST_CASE("get_tables", "[core][IR][unit]")
{
    Position pos("test");
    Token dot(pos, ".", TK_DOT);
    auto A = std::make_unique<Designator>(dot, Token(pos, "A", TK_IDENTIFIER), Token(pos, "id", TK_IDENTIFIER));
    auto B = std::make_unique<Designator>(dot, Token(pos, "B", TK_IDENTIFIER), Token(pos, "id", TK_IDENTIFIER));

    SECTION("Designator")
    {
        cnf::Predicate PA = cnf::Predicate::Positive(A.get());
        cnf::Predicate PB = cnf::Predicate::Positive(B.get());

        cnf::Clause CEmpty({});
        cnf::Clause CA({PA});
        cnf::Clause CAB({PA, PB});

        auto tableEmpty = ClauseInfo(CEmpty).data_sources;
        auto tableA = ClauseInfo(CA).data_sources;
        auto tablesAB = ClauseInfo(CAB).data_sources;

        REQUIRE(tableEmpty.empty());

        REQUIRE(tableA.size() == 1);
        REQUIRE(tableA.find("A") != tableA.end());

        REQUIRE(tablesAB.size() == 2);
        REQUIRE(tablesAB.find("A") != tablesAB.end());
        REQUIRE(tablesAB.find("B") != tablesAB.end());
    }

    SECTION("FnApplicationExpr")
    {
        auto min = std::make_unique<Designator>(Token(pos, "MIN", TK_IDENTIFIER));
        Token lpar(pos, "(", TK_LPAR);

        SECTION("FnApplicationExpr with no argument")
        {
            FnApplicationExpr aggEmpty(lpar, std::move(min), std::vector<std::unique_ptr<Expr>>());
            cnf::Clause CEmpty({cnf::Predicate::Positive(&aggEmpty)});
            auto tableEmpty = ClauseInfo(CEmpty).data_sources;

            REQUIRE(tableEmpty.size() == 0);
        }

        SECTION("FnApplicationExpr with one argument")
        {
            std::vector<std::unique_ptr<Expr>> args;
            args.emplace_back(std::move(A));
            FnApplicationExpr aggA(lpar, std::move(min), std::move(args));
            cnf::Clause CA({cnf::Predicate::Positive(&aggA)});
            auto tableA = ClauseInfo(CA).data_sources;

            REQUIRE(tableA.size() == 1);
            REQUIRE(tableA.find("A") != tableA.end());
        }

        SECTION("FnApplicationExpr with two arguments") {
            std::vector<std::unique_ptr<Expr>> args;
            args.emplace_back(std::move(A));
            args.emplace_back(std::move(B));
            FnApplicationExpr aggAB(lpar, std::move(min), std::move(args));
            cnf::Clause CAB({cnf::Predicate::Positive(&aggAB)});
            auto tablesAB = ClauseInfo(CAB).data_sources;

            REQUIRE(tablesAB.size() == 2);
            REQUIRE(tablesAB.find("A") != tablesAB.end());
            REQUIRE(tablesAB.find("B") != tablesAB.end());
        }
    }

    SECTION("composed expression types")
    {
        auto const0 = std::make_unique<Constant>(Token(pos, "0", TK_Int));
        auto const1 = std::make_unique<Constant>(Token(pos, "1", TK_Int));
        Token plus(pos, "+", TK_PLUS);

        SECTION("UnaryExpr") {
            Token min(pos, "-", TK_MINUS);

            auto unaryConst = std::make_unique<UnaryExpr>(min, std::move(const0));
            auto unaryA = std::make_unique<UnaryExpr>(min, std::move(A));

            cnf::Clause CEmpty({cnf::Predicate::Positive(unaryConst.get())});
            cnf::Clause CA({cnf::Predicate::Positive(unaryA.get())});

            auto tableEmpty = ClauseInfo(CEmpty).data_sources;
            auto tableA = ClauseInfo(CA).data_sources;

            REQUIRE(tableEmpty.empty());

            REQUIRE(tableA.size() == 1);
            REQUIRE(tableA.find("A") != tableA.end());
        }

        SECTION("BinaryExpr with constants") {
            auto binaryEmpty = std::make_unique<BinaryExpr>(plus, std::move(const0), std::move(const1));
            cnf::Clause CEmpty({cnf::Predicate::Positive(binaryEmpty.get())});
            auto tableEmpty = ClauseInfo(CEmpty).data_sources;

            REQUIRE(tableEmpty.empty());
        }

        SECTION("BinaryExpr with one table") {
            auto binaryA = std::make_unique<BinaryExpr>(plus, std::move(A), std::move(const0));
            cnf::Clause CA({cnf::Predicate::Positive(binaryA.get())});
            auto tableA = ClauseInfo(CA).data_sources;

            REQUIRE(tableA.size() == 1);
            REQUIRE(tableA.find("A") != tableA.end());
        }

        SECTION("BinaryExpr with two tables") {
            auto binaryAB = std::make_unique<BinaryExpr>(plus, std::move(A), std::move(B));
            cnf::Clause CAB({cnf::Predicate::Positive(binaryAB.get())});
            auto tablesAB = ClauseInfo(CAB).data_sources;

            REQUIRE(tablesAB.size() == 2);
            REQUIRE(tablesAB.find("A") != tablesAB.end());
            REQUIRE(tablesAB.find("B") != tablesAB.end());
        }
    }
}

TEST_CASE("get_aggregates", "[core][IR][unit]")
{
    Catalog::Clear();
    Catalog &C = Catalog::Get();
    Diagnostic diag(false, std::cout, std::cerr);

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
    auto max = std::make_unique<Designator>(Token(pos, c_max, TK_IDENTIFIER));
    auto sum = std::make_unique<Designator>(Token(pos, c_sum, TK_IDENTIFIER));
    auto A_id_1 = std::make_unique<Designator>(dot, Token(pos, c_A, TK_IDENTIFIER), Token(pos, c_id, TK_IDENTIFIER));
    auto A_id_2 = std::make_unique<Designator>(dot, Token(pos, c_A, TK_IDENTIFIER), Token(pos, c_id, TK_IDENTIFIER));
    Token lpar(pos, c_lpar, TK_LPAR);

    std::vector<std::unique_ptr<Expr>> max_args;
    max_args.emplace_back(std::move(A_id_1));
    auto max_A_id = std::make_unique<FnApplicationExpr>(lpar, std::move(max), std::move(max_args));

    std::vector<std::unique_ptr<Expr>> sum_args;
    sum_args.emplace_back(std::move(A_id_2));
    auto sum_A_id = std::make_unique<FnApplicationExpr>(lpar, std::move(sum), std::move(sum_args));

    SECTION("test SelectStmt")
    {
        SECTION("test SelectClause without aggregates") {
            const char *query = "SELECT A.id \
                                 FROM A;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.empty());
        }

        SECTION("test SelectClause with one aggregate") {
            const char *query = "SELECT MAX(A.id) \
                                 FROM A;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.size() == 1);
            REQUIRE(find_Expr(aggregates, *max_A_id));
        }

        SECTION("test SelectClause with two aggregates") {
            const char *query = "SELECT MAX(A.id), SUM(A.id) \
                                 FROM A;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.size() == 2);
            REQUIRE(find_Expr(aggregates, *max_A_id));
            REQUIRE(find_Expr(aggregates, *sum_A_id));
        }

        SECTION("test HavingClause without aggregates") {
            const char *query = "SELECT 1 \
                                 FROM A \
                                 GROUP BY A.bool AS b \
                                 HAVING NOT b;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.empty());
        }

        SECTION("test HavingClause with one aggregate") {
            const char *query = "SELECT 1 \
                                 FROM A \
                                 GROUP BY A.bool \
                                 HAVING MAX(A.id) = 1;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.size() == 1);
            REQUIRE(find_Expr(aggregates, *max_A_id));
        }

        SECTION("test HavingClause with two aggregates") {
            const char *query = "SELECT 1 \
                                 FROM A \
                                 GROUP BY A.bool \
                                 HAVING MAX(A.id) = 1 AND SUM(A.id) != 1;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.size() == 2);
            REQUIRE(find_Expr(aggregates, *max_A_id));
            REQUIRE(find_Expr(aggregates, *sum_A_id));
        }

        SECTION("test OrderByClause without aggregates") {
            const char *query = "SELECT * \
                                 FROM A \
                                 GROUP BY A.val AS v \
                                 ORDER BY v;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.empty());
        }

        SECTION("test OrderByClause with one aggregate") {
            const char *query = "SELECT * \
                                 FROM A \
                                 GROUP BY A.val \
                                 ORDER BY MAX(A.id);";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.size() == 1);
            REQUIRE(find_Expr(aggregates, *max_A_id));
        }

        SECTION("test OrderByClause with two aggregates") {
            const char *query = "SELECT * \
                                 FROM A \
                                 GROUP BY A.val \
                                 ORDER BY MAX(A.id) ASC, SUM(A.id) DESC;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.size() == 2);
            REQUIRE(find_Expr(aggregates, *max_A_id));
            REQUIRE(find_Expr(aggregates, *sum_A_id));
        }

        SECTION("test no aggregate function") {
            const char *query = "SELECT ISNULL(A.val) \
                                 FROM A;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.empty());
        }

        SECTION("test duplicate aggregate") {
            const char *query = "SELECT MAX(A.id), MAX(A.id) \
                                 FROM A;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.size() == 1);
            REQUIRE(find_Expr(aggregates, *max_A_id));
        }
    }
}

TEST_CASE("get_aggregates/no aggregate possible", "[IR][unit]")
{
    Catalog::Clear();
    Diagnostic diag(false, std::cout, std::cerr);

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
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.empty());
        }

        SECTION("test WhereClause") {
            const char *query = "SELECT * \
                                 FROM A \
                                 WHERE A.id = 1;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.empty());
        }

        SECTION("test GroupByClause") {
            const char *query = "SELECT * \
                                 FROM A \
                                 GROUP BY A.id;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.empty());
        }

        SECTION("test WhereClause") {
            const char *query = "SELECT * \
                                 FROM A \
                                 LIMIT 5;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto aggregates = get_aggregates(*stmt);

            REQUIRE(aggregates.empty());
        }
    }
}

/*======================================================================================================================
 * Test GraphBuilder.
 *====================================================================================================================*/

TEST_CASE("GraphBuilder/SelectStmt", "[core][IR][unit]")
{
    Catalog::Clear();
    Catalog &C = Catalog::Get();
    Diagnostic diag(false, std::cout, std::cerr);

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
    auto c_eq = C.pool("=");
    auto c_neq = C.pool("!=");
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
    tableA.add_primary_key(c_id);

    auto &tableB = DB.add_table(c_B);
    tableB.push_back(c_id, Type::Get_Integer(Type::TY_Vector, 4));
    tableB.push_back(c_val, Type::Get_Integer(Type::TY_Vector, 4));
    tableB.push_back(c_bool, Type::Get_Boolean(Type::TY_Vector));
    tableB.add_primary_key(c_id);

    auto &tableC = DB.add_table(c_C);
    tableC.push_back(c_id, Type::Get_Integer(Type::TY_Vector, 4));
    tableC.push_back(c_val, Type::Get_Integer(Type::TY_Vector, 4));
    tableC.push_back(c_bool, Type::Get_Boolean(Type::TY_Vector));
    tableC.add_primary_key(c_id);

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
        std::pair<std::reference_wrapper<const Expr>, const char *> p_id_empty{A_id, NULL};
        std::pair<std::reference_wrapper<const Expr>, const char *> p_val_empty{A_val, NULL};
        std::pair<std::reference_wrapper<const Expr>, const char *> p_id_Aid{A_id, c_A_id};
        std::pair<std::reference_wrapper<const Expr>, const char *> p_val_Aval{A_val, c_A_val};
        std::pair<std::reference_wrapper<const Expr>, const char *> p_const0_empty{const0, NULL};

        SECTION("test constant projection")
        {
            const char *query = "SELECT 0;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &projections = graph->projections();

            REQUIRE(graph->sources().empty());
            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);

            REQUIRE(projections.size() == 1);
            REQUIRE(find_Proj(projections, p_const0_empty));
        }

        SECTION("test one projection")
        {
            const char *query = "SELECT A.id \
                                 FROM A;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();
            auto projections = graph->projections();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(projections.size() == 1);
            REQUIRE(find_Proj(projections, p_id_empty));
        }

        SECTION("test one projection renamed")
        {
            const char *query = "SELECT A.id AS A_id\
                                 FROM A;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();
            auto projections = graph->projections();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(projections.size() == 1);
            REQUIRE(find_Proj(projections, p_id_Aid));
        }

        SECTION("test two projections")
        {
            const char *query = "SELECT A.id, A.val \
                                 FROM A;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();
            auto projections = graph->projections();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(projections.size() == 2);
            REQUIRE(find_Proj(projections, p_id_empty));
            REQUIRE(find_Proj(projections, p_val_empty));
        }

        SECTION("test two projections renamed")
        {
            const char *query = "SELECT A.id AS A_id, A.val AS A_val\
                                 FROM A;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();
            auto projections = graph->projections();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(projections.size() == 2);
            REQUIRE(find_Proj(projections, p_id_Aid));
            REQUIRE(find_Proj(projections, p_val_Aval));
        }

        SECTION("test projection with star")
        {
            const char *query = "SELECT * \
                                 FROM A;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();
            auto projections = graph->projections();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(graph->projections().size() == tableA.num_attrs());
        }
    }

    SECTION("test sources and joins") {
        auto stmt_A_id_eq_B_id = as<SelectStmt>(m::statement_from_string(diag, "SELECT * FROM A, B WHERE A.id = B.id;"));
        auto cnf_A_id_eq_B_id = cnf::to_CNF(*as<WhereClause>(stmt_A_id_eq_B_id->where.get())->where);

        auto stmt_B_val_eq_C_val = as<SelectStmt>(m::statement_from_string(diag, "SELECT * FROM B, C WHERE B.val = C.val;"));
        auto cnf_B_val_eq_C_val = cnf::to_CNF(*as<WhereClause>(stmt_B_val_eq_C_val->where.get())->where);

        auto stmt_A_id_eq_C_id = as<SelectStmt>(m::statement_from_string(diag, "SELECT * FROM A, C WHERE A.id = C.id;"));
        auto cnf_A_id_eq_C_id = cnf::to_CNF(*as<WhereClause>(stmt_A_id_eq_C_id->where.get())->where);

        auto stmt_A_val_eq_const0 = as<SelectStmt>(m::statement_from_string(diag, "SELECT * FROM A WHERE A.val = 0;"));
        auto cnf_A_val_eq_const0 = cnf::to_CNF(*as<WhereClause>(stmt_A_val_eq_const0->where.get())->where);

        auto stmt_tbl_val_eq_const1 = as<SelectStmt>(m::statement_from_string(diag, "SELECT * FROM A AS tbl WHERE tbl.val = 1;"));
        auto cnf_tbl_val_eq_const1 = cnf::to_CNF(*as<WhereClause>(stmt_tbl_val_eq_const1->where.get())->where);

        auto stmt_not_A_bool = as<SelectStmt>(m::statement_from_string(diag, "SELECT * FROM A WHERE NOT A.bool;"));
        auto cnf_not_A_bool = cnf::to_CNF(*as<WhereClause>(stmt_not_A_bool->where.get())->where);

        auto stmt_constTrue = as<SelectStmt>(m::statement_from_string(diag, "SELECT * FROM A WHERE TRUE;"));
        auto cnf_constTrue = cnf::to_CNF(*as<WhereClause>(stmt_constTrue->where.get())->where);

        SECTION("test one source without filter")
        {
            const char *query = "SELECT * \
                                 FROM A;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == tableA.num_attrs());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());
        }

        SECTION("test one source renamed without filter")
        {
            const char *query = "SELECT * \
                                 FROM A AS tbl;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == tableA.num_attrs());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_tbl);
            REQUIRE(sources[0]->name() == c_tbl);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());
        }

        SECTION("test one source with one filter")
        {
            const char *query = "SELECT * \
                                 FROM A \
                                 WHERE A.val = 0;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == tableA.num_attrs());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter() == cnf_A_val_eq_const0);
        }

        SECTION("test one source renamed with one filter")
        {
            const char *query = "SELECT * \
                                 FROM A AS tbl \
                                 WHERE tbl.val = 1;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == tableA.num_attrs());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_tbl);
            REQUIRE(sources[0]->name() == c_tbl);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter() == cnf_tbl_val_eq_const1);
        }

        SECTION("test one source with two filters")
        {
            const char *query = "SELECT * \
                                 FROM A \
                                 WHERE A.val = 0 AND NOT A.bool;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == tableA.num_attrs());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter() == cnf::operator&&(cnf_A_val_eq_const0, cnf_not_A_bool));
        }

        SECTION("test two sources with one binary join")
        {
            const char *query = "SELECT * \
                                 FROM A, B \
                                 WHERE A.id = B.id;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();
            auto &joins = graph->joins();

            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == tableA.num_attrs() + tableB.num_attrs());

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
        }

        SECTION("test three sources with three binary joins")
        {
            const char *query = "SELECT * \
                                 FROM A, B, C \
                                 WHERE A.id = B.id AND B.val = C.val AND A.id = C.id;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();
            auto &joins = graph->joins();

            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == tableA.num_attrs() + tableB.num_attrs() + tableC.num_attrs());

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
        }

        SECTION("test one source with constant filter")
        {
            const char *query = "SELECT * \
                                 FROM A \
                                 WHERE TRUE;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == tableA.num_attrs());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter() == cnf_constTrue);
        }

        SECTION("test two sources with constant filter")
        {
            const char *query = "SELECT * \
                                 FROM A, B \
                                 WHERE A.id = B.id AND TRUE;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();
            auto &joins = graph->joins();

            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == tableA.num_attrs() + tableB.num_attrs());

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
        }
    }

    SECTION("test group by and aggregates")
    {
        Token lpar(pos, c_lpar, TK_LPAR);
        auto avg = std::make_unique<Designator>(Token(pos, c_avg, TK_IDENTIFIER));
        auto min = std::make_unique<Designator>(Token(pos, c_min, TK_IDENTIFIER));
        auto A_id = std::make_unique<Designator>(dot, Token(pos, c_A, TK_IDENTIFIER), Token(pos, c_id, TK_IDENTIFIER));
        auto A_id_2 = std::make_unique<Designator>(dot, Token(pos, c_A, TK_IDENTIFIER), Token(pos, c_id, TK_IDENTIFIER));
        auto A_val = std::make_unique<Designator>(dot, Token(pos, c_A, TK_IDENTIFIER), Token(pos, c_val, TK_IDENTIFIER));

        std::vector<std::unique_ptr<Expr>> min_args;
        min_args.emplace_back(std::move(A_id_2));
        auto min_A_id = std::make_unique<FnApplicationExpr>(lpar, std::move(min), std::move(min_args));

        std::vector<std::unique_ptr<Expr>> avg_args;
        avg_args.emplace_back(std::move(A_id));
        auto avg_A_id = std::make_unique<FnApplicationExpr>(lpar, std::move(avg), std::move(avg_args));

        auto stmt_A_val_eq_const0 = as<SelectStmt>(m::statement_from_string(diag, "SELECT * FROM A GROUP BY A.val AS v HAVING v = 0;"));
        auto cnf_A_val_eq_const0 = cnf::to_CNF(*as<HavingClause>(stmt_A_val_eq_const0->having.get())->having);

        auto stmt_min_A_id_eq_const1 = as<SelectStmt>(
            m::statement_from_string(diag, "SELECT * FROM A GROUP BY A.val HAVING MIN(A.id) = 1;"));
        auto cnf_min_A_id_eq_const1 = cnf::to_CNF(*as<HavingClause>(stmt_min_A_id_eq_const1->having.get())->having);

        SECTION("test no grouping with aggregate")
        {
            const char *query = "SELECT AVG(A.id) \
                                 FROM A;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();
            auto group_by = graph->group_by();
            auto aggregates = graph->aggregates();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == 1);

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(group_by.empty());

            REQUIRE(aggregates.size() == 1);
            REQUIRE(find_Expr(aggregates, *avg_A_id));
        }

        SECTION("test grouping with no aggregate")
        {
            const char *query = "SELECT * \
                                 FROM A \
                                 GROUP BY A.val;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();
            auto group_by = graph->group_by();
            auto aggregates = graph->aggregates();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == 1);

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(group_by.size() == 1);
            REQUIRE(find_Expr(group_by, *A_val));

            REQUIRE(aggregates.empty());
        }

        SECTION("test grouping with aggregate")
        {
            const char *query = "SELECT AVG(A.id) \
                                 FROM A \
                                 GROUP BY A.val;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();
            auto group_by = graph->group_by();
            auto aggregates = graph->aggregates();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == 1);

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(group_by.size() == 1);
            REQUIRE(find_Expr(group_by, *A_val));

            REQUIRE(aggregates.size() == 1);
            REQUIRE(find_Expr(aggregates, *avg_A_id));
        }

        SECTION("test grouping with HAVING and no aggregate")
        {
            const char *query = "SELECT * \
                                 FROM A \
                                 GROUP BY A.val AS v \
                                 HAVING v = 0;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            REQUIRE(graph->sources().size() == 1);
            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == 1); // 1 grouping key

            auto having = as<const Query>(graph->sources()[0].get());
            REQUIRE(having);

            REQUIRE(having->joins().empty());
            REQUIRE(having->filter() == cnf_A_val_eq_const0);

            auto group_by = having->query_graph().group_by();
            auto aggregates = having->query_graph().aggregates();

            REQUIRE(group_by.size() == 1);
            REQUIRE(find_Expr(group_by, *A_val));

            REQUIRE(aggregates.empty());
        }

        SECTION("test grouping with HAVING and aggregate")
        {
            const char *query = "SELECT * \
                                 FROM A \
                                 GROUP BY A.val \
                                 HAVING MIN(A.id) = 1;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            REQUIRE(graph->sources().size() == 1);
            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == 1); // 1 grouping key

            auto having = as<const Query>(graph->sources()[0].get());
            REQUIRE(having);

            REQUIRE(having->joins().empty());
            REQUIRE(having->filter() == cnf_min_A_id_eq_const1);

            auto group_by = having->query_graph().group_by();
            auto aggregates = having->query_graph().aggregates();

            REQUIRE(group_by.size() == 1);
            REQUIRE(find_Expr(group_by, *A_val));

            REQUIRE(aggregates.size() == 1);
            REQUIRE(find_Expr(aggregates, *min_A_id));
        }
    }

    SECTION("test order by") {
        Designator A_id(dot, Token(pos, c_A, TK_IDENTIFIER),
                                          Token(pos, c_id, TK_IDENTIFIER));
        Designator A_val(dot, Token(pos, c_A, TK_IDENTIFIER),
                         Token(pos, c_val, TK_IDENTIFIER));
        std::pair<std::reference_wrapper<const Expr>, bool> p_id_ASC{A_id, true};
        std::pair<std::reference_wrapper<const Expr>, bool> p_id_DESC{A_id, false};
        std::pair<std::reference_wrapper<const Expr>, bool> p_val_ASC{A_val, true};

        SECTION("test no order by") {
            const char *query = "SELECT * \
                                 FROM A;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();
            auto order_by = graph->order_by();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == tableA.num_attrs());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(order_by.empty());
        }

        SECTION("test order by ASC implicit") {
            const char *query = "SELECT * \
                                 FROM A \
                                 ORDER BY A.id;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();
            auto &order_by = graph->order_by();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == tableA.num_attrs());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(order_by.size() == 1);
            REQUIRE(find_OrderBy(order_by, p_id_ASC));
        }

        SECTION("test order by ASC explicit") {
            const char *query = "SELECT * \
                                 FROM A \
                                 ORDER BY A.id ASC;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();
            auto order_by = graph->order_by();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == tableA.num_attrs());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(order_by.size() == 1);
            REQUIRE(find_OrderBy(order_by, p_id_ASC));
        }

        SECTION("test order by DESC") {
            const char *query = "SELECT * \
                                 FROM A \
                                 ORDER BY A.id DESC;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();
            auto order_by = graph->order_by();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == tableA.num_attrs());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(order_by.size() == 1);
            REQUIRE(find_OrderBy(order_by, p_id_DESC));
        }

        SECTION("test order by multiple") {
            const char *query = "SELECT * \
                                 FROM A \
                                 ORDER BY A.id DESC, A.val ASC;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();
            auto order_by = graph->order_by();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == tableA.num_attrs());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(order_by.size() == 2);
            REQUIRE(find_OrderBy(order_by, p_id_DESC));
            REQUIRE(find_OrderBy(order_by, p_val_ASC));
        }
    }

    SECTION("test LIMIT clause")
    {
        SECTION("test limit without offset") {
            const char *query = "SELECT * \
                                 FROM A \
                                 LIMIT 5;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->projections().size() == tableA.num_attrs());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(graph->limit().limit == 5);
            REQUIRE(graph->limit().offset == 0);
        }

        SECTION("test limit with offset")
        {
            const char *query = "SELECT * \
                                 FROM A \
                                 LIMIT 5 OFFSET 10;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->projections().size() == tableA.num_attrs());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(graph->limit().limit == 5);
            REQUIRE(graph->limit().offset == 10);
        }
    }

    SECTION("test nested queries in FROM clause") {
        Token eq(pos, c_eq, TK_EQUAL);
        Token no(pos, c_no, TK_Not);
        auto A_id = std::make_unique<Designator>(dot, Token(pos, c_A, TK_IDENTIFIER), Token(pos, c_id, TK_IDENTIFIER));
        auto A_id_2 = std::make_unique<Designator>(dot, Token(pos, c_A, TK_IDENTIFIER), Token(pos, c_id, TK_IDENTIFIER));
        auto A_val = std::make_unique<Designator>(dot, Token(pos, c_A, TK_IDENTIFIER), Token(pos, c_val, TK_IDENTIFIER));
        auto A_bool = std::make_unique<Designator>(dot, Token(pos, c_A, TK_IDENTIFIER), Token(pos, c_bool, TK_IDENTIFIER));
        auto B_id = std::make_unique<Designator>(dot, Token(pos, c_B, TK_IDENTIFIER), Token(pos, c_id, TK_IDENTIFIER));
        auto B_val = std::make_unique<Designator>(dot, Token(pos, c_B, TK_IDENTIFIER), Token(pos, c_val, TK_IDENTIFIER));
        auto C_id = std::make_unique<Designator>(dot, Token(pos, c_C, TK_IDENTIFIER), Token(pos, c_id, TK_IDENTIFIER));
        auto C_val = std::make_unique<Designator>(dot, Token(pos, c_C, TK_IDENTIFIER), Token(pos, c_val, TK_IDENTIFIER));
        auto tbl_val = std::make_unique<Designator>(dot, Token(pos, c_tbl, TK_IDENTIFIER), Token(pos, c_val, TK_IDENTIFIER));
        auto const0 = std::make_unique<Constant>(Token(pos, c_0, TK_Int));
        auto const1 = std::make_unique<Constant>(Token(pos, c_1, TK_Int));
        BinaryExpr A_id_eq_B_id(eq, std::move(A_id), std::move(B_id));
        BinaryExpr B_val_eq_C_val(eq, std::move(B_val), std::move(C_val));
        BinaryExpr A_id_eq_C_id(eq, std::move(A_id_2), std::move(C_id));
        BinaryExpr A_val_eq_const0(eq, std::move(A_val), std::move(const0));
        BinaryExpr tbl_val_eq_const1(eq, std::move(tbl_val), std::move(const1));
        UnaryExpr not_A_bool(no, std::move(A_bool));

#if 0
        SECTION("test simple non-correlated subquery") {
            const char *query = "SELECT * \
                                 FROM (SELECT * \
                                       FROM A) AS tbl;";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();

            REQUIRE(graph->joins().empty());
            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == tableA.num_attrs());

            REQUIRE(sources.size() == 1);
            REQUIRE(sources[0]->alias() == c_tbl);
            REQUIRE(sources[0]->name() == c_tbl);
            REQUIRE(sources[0]->joins().empty());
            REQUIRE(sources[0]->filter().empty());

            REQUIRE(is<const Query>(sources[0]));
        }
#endif
    }

#if 0
    SECTION("test nested queries in WHERE clause")
    {
        Token eq(pos, c_eq, TK_EQUAL);
        Token neq(pos, c_neq, TK_BANG_EQUAL);
        Designator A_id(dot, Token(pos, c_A, TK_IDENTIFIER),
                       Token(pos, c_id, TK_IDENTIFIER));
        Designator A_val(dot, Token(pos, c_A, TK_IDENTIFIER),
                         Token(pos, c_val, TK_IDENTIFIER));
        Designator A_bool(dot, Token(pos, c_A, TK_IDENTIFIER),
                          Token(pos, c_bool, TK_IDENTIFIER));
        Designator B_id(dot, Token(pos, c_B, TK_IDENTIFIER),
                        Token(pos, c_id, TK_IDENTIFIER));
        Designator C_id(dot, Token(pos, c_C, TK_IDENTIFIER),
                        Token(pos, c_id, TK_IDENTIFIER));
        Designator C_val(dot, Token(pos, c_C, TK_IDENTIFIER),
                         Token(pos, c_val, TK_IDENTIFIER));

        SECTION("test simple non-correlated subquery")
        {
            const char *query = "SELECT id \
                                 FROM A \
                                 WHERE val = (SELECT MIN(B.val) \
                                              FROM B);";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);
            graph->dump();

            auto &sources = graph->sources();
            auto &joins = graph->joins();

            REQUIRE(graph->group_by().size() == 0);
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == 1);

            REQUIRE(sources.size() == 2);
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->joins().size() == 1);
            REQUIRE(sources[0]->filter().size() == 0);
            REQUIRE(is<const BaseTable>(sources[0]));
            REQUIRE(sources[1]->joins().size() == 1);
            REQUIRE(sources[1]->filter().size() == 0);
            REQUIRE(is<const Query>(sources[1]));

            REQUIRE(joins.size() == 1);
            auto where = cast<const BinaryExpr>(&joins[0]->condition()[0][0].expr());
            REQUIRE(where);
            REQUIRE(*where->lhs == A_val);
            auto q = cast<QueryExpr>(where->rhs.get());
            REQUIRE(q);
        }

        SECTION("test simple correlated subquery with equi-predicate")
        {
            const char *query = "SELECT id \
                                 FROM A \
                                 WHERE val = (SELECT MIN(B.val) \
                                              FROM B \
                                              WHERE A.id = B.id);";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();
            auto &joins = graph->joins();

            REQUIRE(graph->group_by().size() == 0);
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().size() == 0);
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == 1);

            REQUIRE(sources.size() == 2);
            REQUIRE(is<const BaseTable>(sources[0]));
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->filter().empty());
            REQUIRE(sources[0]->joins().size() == 1);
            auto q = cast<const Query>(sources[1].get());
            REQUIRE(q);
            REQUIRE(q->joins().size() == 1);
            REQUIRE(q->query_graph().sources().size() == 1);
            REQUIRE(q->query_graph().sources()[0]->filter().empty());
            REQUIRE(q->query_graph().group_by().size() == 1);
            REQUIRE(q->query_graph().group_by()[0].get() == B_id);
            REQUIRE(q->query_graph().projections().size() == 2);

            REQUIRE(joins.size() == 1);
            REQUIRE(joins[0]->condition().size() == 2);
            auto where_0 = cast<const BinaryExpr>(&joins[0]->condition()[0][0].expr());
            REQUIRE(where_0);
            REQUIRE(*where_0->lhs == A_val);
            auto qExpr = cast<QueryExpr>(where_0->rhs.get());
            REQUIRE(qExpr);
            auto where_1 = cast<const BinaryExpr>(&joins[0]->condition()[1][0].expr());
            REQUIRE(where_1);
            REQUIRE(*where_1->lhs == A_id);
            REQUIRE(where_1->op().type == TK_EQUAL);
            auto des = cast<Designator>(where_1->rhs.get());
            REQUIRE(des);
            REQUIRE(streq(des->attr_name.text, "B.id"));
            REQUIRE(*std::get<const Expr*>(des->target()) == q->query_graph().projections()[1].first.get());
        }

        SECTION("test simple correlated subquery with non-equi-predicate")
        {
            const char *query = "SELECT id \
                                 FROM A \
                                 WHERE val = (SELECT MIN(B.val) AS min \
                                              FROM B \
                                              WHERE A.id != B.id);";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == 1);

            REQUIRE(graph->sources().size() == 1);
            auto q = cast<const Query>(graph->sources()[0].get());
            REQUIRE(q);
            REQUIRE(q->joins().empty());
            auto where_0 = cast<const BinaryExpr>(&q->filter()[0][0].expr());
            REQUIRE(where_0);
            REQUIRE(streq(to_string(*where_0->lhs).c_str(), "A.val"));
            auto qExpr = cast<QueryExpr>(where_0->rhs.get());
            REQUIRE(qExpr);
            auto &q_graph = q->query_graph();
            REQUIRE(q_graph.sources().size() == 2);
            REQUIRE(q_graph.joins().size() == 1);
            auto where_1 = cast<const BinaryExpr>(&q_graph.joins()[0]->condition()[0][0].expr());
            REQUIRE(where_1);
            REQUIRE(*where_1->lhs == A_id);
            REQUIRE(where_1->op().type == TK_BANG_EQUAL);
            auto des = cast<Designator>(where_1->rhs.get());
            REQUIRE(des);
            REQUIRE(des->attr_name.text == c_id);
            REQUIRE(std::get<const Attribute*>(des->target())->table.name == c_B);
            REQUIRE(q_graph.group_by().size() == 2);
            REQUIRE(q_graph.group_by()[0].get() == A_id);
            REQUIRE(q_graph.group_by()[1].get() == A_val);
            REQUIRE(q_graph.aggregates().size() == 1);
            REQUIRE(q_graph.projections().size() == 3);
        }

        SECTION("test expansion of `SELECT *`")
        {
            const char *query = "SELECT * \
                                 FROM A \
                                 WHERE val = (SELECT MIN(B.val) \
                                              FROM B \
                                              WHERE A.id = B.id);";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            REQUIRE(graph->projections().size() == tableA.num_attrs());
            REQUIRE(find_Proj(graph->projections(), {A_id, nullptr}));
            REQUIRE(find_Proj(graph->projections(), {A_val, nullptr}));
            REQUIRE(find_Proj(graph->projections(), {A_bool, nullptr}));
        }

        SECTION("test primary key provisioning")
        {
            const char *query = "SELECT val \
                                 FROM (SELECT val, bool FROM A) AS Q \
                                 WHERE val = (SELECT MIN(B.val) \
                                              FROM B \
                                              WHERE Q.bool != B.bool);";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            REQUIRE(graph->sources().size() == 1);
            auto q = cast<const Query>(graph->sources()[0].get());
            REQUIRE(q);
            auto &q_graph = q->query_graph();
            REQUIRE(q_graph.sources().size() == 2);
            REQUIRE(q_graph.group_by().size() == 2);
            REQUIRE(q_graph.group_by()[0].get() == A_id);
            REQUIRE(q_graph.aggregates().size() == 1);
            REQUIRE(q_graph.projections().size() == 2);
            auto Q = cast<const Query>(q_graph.sources()[1].get());
            REQUIRE(Q);
            REQUIRE(streq(Q->alias(), "Q"));
            REQUIRE(streq(Q->name(), "Q"));
            auto &Q_graph = Q->query_graph();
            REQUIRE(Q_graph.projections().size() == 3);
            REQUIRE(Q_graph.projections()[2].first.get() == A_id);
        }

        SECTION("test HAVING and equi-predicate")
        {
            const char *query = "SELECT id \
                                 FROM A \
                                 WHERE val = (SELECT MIN(B.val) \
                                              FROM B \
                                              WHERE A.id = B.id \
                                              HAVING MAX(B.val) > 1);";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();
            auto &joins = graph->joins();

            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == 1);

            REQUIRE(sources.size() == 2);
            REQUIRE(is<const BaseTable>(sources[0]));
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->filter().empty());
            REQUIRE(sources[0]->joins().size() == 1);
            auto q = cast<const Query>(sources[1].get());
            REQUIRE(q);
            REQUIRE(q->joins().size() == 1);
            REQUIRE(q->query_graph().sources().size() == 1);
            REQUIRE_FALSE(q->query_graph().sources()[0]->filter().empty());
            REQUIRE_FALSE(q->query_graph().grouping());
            REQUIRE(q->query_graph().projections().size() == 2);
            auto having = cast<const Query>(q->query_graph().sources()[0].get());
            REQUIRE(having);
            REQUIRE(having->joins().empty());
            REQUIRE(having->query_graph().sources().size() == 1);
            REQUIRE(having->query_graph().sources()[0]->filter().empty());
            REQUIRE(having->query_graph().group_by().size() == 1);
            REQUIRE(having->query_graph().group_by()[0].get() == B_id);
            REQUIRE(having->query_graph().projections().empty());

            REQUIRE(joins.size() == 1);
            auto where_0 = cast<const BinaryExpr>(&joins[0]->condition()[0][0].expr());
            REQUIRE(where_0);
            REQUIRE(*where_0->lhs == A_val);
            auto qExpr = cast<QueryExpr>(where_0->rhs.get());
            REQUIRE(qExpr);
            auto where_1 = cast<const BinaryExpr>(&joins[0]->condition()[1][0].expr());
            REQUIRE(where_1);
            REQUIRE(*where_1->lhs == A_id);
            REQUIRE(where_1->op().type == TK_EQUAL);
            auto des = cast<Designator>(where_1->rhs.get());
            REQUIRE(des);
            REQUIRE(streq(des->attr_name.text, "B.id"));
            REQUIRE(*std::get<const Expr*>(des->target()) == q->query_graph().projections()[1].first.get());
        }

        SECTION("test HAVING and non-equi-predicate")
        {
            const char *query = "SELECT id \
                                 FROM A \
                                 WHERE val = (SELECT MIN(B.val) AS min \
                                              FROM B \
                                              WHERE A.id != B.id \
                                              HAVING MAX(B.val) > 1);";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == 1);

            REQUIRE(graph->sources().size() == 1);
            auto q = cast<const Query>(graph->sources()[0].get());
            REQUIRE(q);
            REQUIRE(q->joins().empty());
            auto where_0 = cast<const BinaryExpr>(&q->filter()[0][0].expr());
            REQUIRE(where_0);
            REQUIRE(streq(to_string(*where_0->lhs).c_str(), "A.val"));
            auto qExpr = cast<QueryExpr>(where_0->rhs.get());
            REQUIRE(qExpr);
            auto &q_graph = q->query_graph();
            REQUIRE(q_graph.sources().size() == 1);
            REQUIRE(q_graph.joins().empty());
            REQUIRE_FALSE(q_graph.grouping());
            REQUIRE(q_graph.projections().size() == 3);
            auto having = cast<const Query>(q_graph.sources()[0].get());
            REQUIRE(having);
            auto &having_graph = having->query_graph();
            REQUIRE(having_graph.sources().size() == 2);
            REQUIRE(having_graph.joins().size() == 1);
            auto where_1 = cast<const BinaryExpr>(&having_graph.joins()[0]->condition()[0][0].expr());
            REQUIRE(where_1);
            REQUIRE(*where_1->lhs == A_id);
            REQUIRE(where_1->op().type == TK_BANG_EQUAL);
            auto des = cast<Designator>(where_1->rhs.get());
            REQUIRE(des);
            REQUIRE(des->attr_name.text == c_id);
            REQUIRE(std::get<const Attribute*>(des->target())->table.name == c_B);
            REQUIRE(having_graph.group_by().size() == 2);
            REQUIRE(having_graph.group_by()[0].get() == A_id);
            REQUIRE(having_graph.group_by()[1].get() == A_val);
            REQUIRE(having_graph.aggregates().size() == 2);
            REQUIRE(having_graph.projections().empty());
        }

        SECTION("test HAVING with non-equi-predicate")
        {
            const char *query = "SELECT id \
                                 FROM A \
                                 WHERE val = (SELECT MIN(B.val) AS min \
                                              FROM B \
                                              HAVING MAX(B.val) > A.id);";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();
            auto &joins = graph->joins();

            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == 1);

            REQUIRE(sources.size() == 2);
            REQUIRE(is<const BaseTable>(sources[0]));
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->filter().empty());
            REQUIRE(sources[0]->joins().size() == 1);
            auto q = cast<const Query>(sources[1].get());
            REQUIRE(q);
            REQUIRE(q->joins().size() == 1);
            REQUIRE(q->query_graph().sources().size() == 1);
            REQUIRE(q->query_graph().sources()[0]->filter().empty());
            REQUIRE(is<const Query>(q->query_graph().sources()[0]));
            REQUIRE_FALSE(q->query_graph().grouping());
            REQUIRE(q->query_graph().projections().size() == 2);

            REQUIRE(joins.size() == 1);
            auto where_0 = cast<const BinaryExpr>(&joins[0]->condition()[0][0].expr());
            REQUIRE(where_0);
            REQUIRE(*where_0->lhs == A_val);
            auto qExpr = cast<QueryExpr>(where_0->rhs.get());
            REQUIRE(qExpr);
            auto where_1 = cast<const BinaryExpr>(&joins[0]->condition()[1][0].expr());
            REQUIRE(where_1);
            REQUIRE(streq(to_string(*where_1).c_str(), "(MAX(B.val) > A.id)"));
        }

        SECTION("test multiple correlated subquery with equi-predicate")
        {
            const char *query = "SELECT id \
                                 FROM A \
                                 WHERE val = (SELECT MAX(C.val) \
                                              FROM C \
                                              WHERE C.id != (SELECT MIN(B.val) \
                                                             FROM B \
                                                             WHERE A.id = B.id));";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            auto &sources = graph->sources();
            auto &joins = graph->joins();

            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == 1);

            REQUIRE(sources.size() == 2);
            REQUIRE(is<const BaseTable>(sources[0]));
            REQUIRE(sources[0]->alias() == nullptr);
            REQUIRE(sources[0]->name() == c_A);
            REQUIRE(sources[0]->filter().empty());
            REQUIRE(sources[0]->joins().size() == 1);
            auto q1 = cast<const Query>(sources[1].get());
            REQUIRE(q1);
            REQUIRE(q1->joins().size() == 1);
            REQUIRE(q1->query_graph().sources().size() == 2);
            REQUIRE(is<const BaseTable>(q1->query_graph().sources()[0]));
            REQUIRE(q1->query_graph().sources()[0]->alias() == nullptr);
            REQUIRE(q1->query_graph().sources()[0]->name() == c_C);
            REQUIRE(q1->query_graph().sources()[0]->filter().empty());
            REQUIRE(q1->query_graph().sources()[0]->joins().size() == 1);
            REQUIRE(q1->query_graph().group_by().size() == 1);
            REQUIRE(q1->query_graph().projections().size() == 2);
            auto q2 = cast<const Query>(q1->query_graph().sources()[1].get());
            REQUIRE(q2);
            REQUIRE(q2->joins().size() == 1);
            REQUIRE(q2->query_graph().sources().size() == 1);
            REQUIRE(q2->query_graph().sources()[0]->filter().empty());
            REQUIRE(q2->query_graph().group_by().size() == 1);
            REQUIRE(q2->query_graph().group_by()[0].get() == B_id);
            REQUIRE(q2->query_graph().projections().size() == 2);

            REQUIRE(joins.size() == 1);
            auto where_0 = cast<const BinaryExpr>(&joins[0]->condition()[0][0].expr());
            REQUIRE(where_0);
            REQUIRE(*where_0->lhs == A_val);
            auto qExpr = cast<QueryExpr>(where_0->rhs.get());
            REQUIRE(qExpr);
            auto where_1 = cast<const BinaryExpr>(&joins[0]->condition()[1][0].expr());
            REQUIRE(where_1);
            REQUIRE(*where_1->lhs == A_id);
            REQUIRE(where_1->op().type == TK_EQUAL);
            auto des = cast<Designator>(where_1->rhs.get());
            REQUIRE(des);
            REQUIRE(*std::get<const Expr*>(des->target()) == q1->query_graph().projections()[1].first.get());
        }

        SECTION("test multiple correlated subquery with non-equi-predicate")
        {
            const char *query = "SELECT id \
                                 FROM A \
                                 WHERE val = (SELECT MAX(C.val) \
                                              FROM C \
                                              WHERE C.id != (SELECT MIN(B.val) \
                                                             FROM B \
                                                             WHERE A.id != B.id));";
            auto stmt = as<SelectStmt>(m::statement_from_string(diag, query));
            auto graph = QueryGraph::Build(*stmt);

            REQUIRE(graph->group_by().empty());
            REQUIRE(graph->aggregates().empty());
            REQUIRE(graph->order_by().empty());
            REQUIRE(graph->limit().limit == 0);
            REQUIRE(graph->projections().size() == 1);

            REQUIRE(graph->sources().size() == 1);
            auto q1 = cast<const Query>(graph->sources()[0].get());
            REQUIRE(q1);
            REQUIRE(q1->joins().empty());
            auto where_0 = cast<const BinaryExpr>(&q1->filter()[0][0].expr());
            REQUIRE(where_0);
            REQUIRE(streq(to_string(*where_0->lhs).c_str(), "A.val"));
            REQUIRE(is<QueryExpr>(where_0->rhs));

            auto &q1_graph = q1->query_graph();
            REQUIRE(q1_graph.sources().size() == 1);
            REQUIRE(q1_graph.joins().empty());
            REQUIRE(q1_graph.group_by().size() == 2);
            REQUIRE(streq(to_string(q1_graph.group_by()[0].get()).c_str(), "A.id"));
            REQUIRE(streq(to_string(q1_graph.group_by()[1].get()).c_str(), "A.val"));
            REQUIRE(q1_graph.aggregates().size() == 1);
            REQUIRE(q1_graph.projections().size() == 3);
            auto q2 = cast<const Query>(q1_graph.sources()[0].get());
            REQUIRE(q2);
            REQUIRE(q2->joins().empty());
            auto where_2 = cast<const BinaryExpr>(&q2->filter()[0][0].expr());
            REQUIRE(where_2);
            REQUIRE(streq(to_string(*where_2->lhs).c_str(), "C.id"));
            REQUIRE(is<QueryExpr>(where_2->rhs));

            auto &q2_graph = q2->query_graph();
            REQUIRE(q2_graph.sources().size() == 3);
            REQUIRE(q2_graph.joins().size() == 1);
            REQUIRE(q2_graph.group_by().size() == 4);
            REQUIRE(streq(to_string(q2_graph.group_by()[0].get()).c_str(), "C.id"));
            REQUIRE(streq(to_string(q2_graph.group_by()[1].get()).c_str(), "C.val"));
            REQUIRE(streq(to_string(q2_graph.group_by()[2].get()).c_str(), "A.id"));
            REQUIRE(streq(to_string(q2_graph.group_by()[3].get()).c_str(), "A.val"));
            REQUIRE(q2_graph.aggregates().size() == 1);
            REQUIRE(q2_graph.projections().size() == 5);
            auto where_1 = cast<const BinaryExpr>(&q2_graph.joins()[0]->condition()[0][0].expr());
            REQUIRE(where_1);
            REQUIRE(streq(to_string(*where_1->lhs).c_str(), "A.id"));
            REQUIRE(where_1->op().type == TK_BANG_EQUAL);
            auto des = cast<Designator>(where_1->rhs.get());
            REQUIRE(des);
            REQUIRE(des->attr_name.text == c_id);
            REQUIRE(std::get<const Attribute*>(des->target())->table.name == c_B);
        }

    }
#endif
}
