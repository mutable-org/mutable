#include "catch.hpp"

#include "catalog/Type.hpp"
#include "IR/CNF.hpp"
#include "IR/Interpreter.hpp"
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "storage/RowStore.hpp"
#include "testutil.hpp"
#include "util/Diagnostic.hpp"
#include <cmath>
#include <string>
//#include "util/fn.hpp"
//#include <algorithm>

using namespace db;


/*======================================================================================================================
 * Helper funtctions for test setup.
 *====================================================================================================================*/


cnf::CNF get_where_cnf(const char * sql)
{
    /* Get catalog, lexer, parser, and sema object. */
    Catalog &C = Catalog::Get();
    Diagnostic diag(false, std::cout, std::cerr);
    std::istringstream in(sql);
    Lexer lexer(diag, C.get_pool(), "-", in);
    Parser parser(lexer);
    Sema sema(diag);

    /* Parse and analyze SQL statement. */
    auto stmt = as<SelectStmt>(parser.parse());
    sema(*stmt);

    /* Extract WHERE clause and obtain cnf. */
    auto where = as<WhereClause>(stmt->where);
    auto cond = where->where;
    cnf::CNFGenerator cnfGen;
    cnfGen(*cond);
    auto cnf = cnfGen.get();

    return cnf;
}

Expr * get_where_expr(const char * sql)
{
    /* Get catalog, lexer, parser, and sema object. */
    Catalog &C = Catalog::Get();
    Diagnostic diag(false, std::cout, std::cerr);
    std::istringstream in(sql);
    Lexer lexer(diag, C.get_pool(), "-", in);
    Parser parser(lexer);
    Sema sema(diag);

    /* Parse and analyze SQL statement. */
    auto stmt = as<SelectStmt>(parser.parse());
    sema(*stmt);

    /* Extract WHERE clause and obtain cnf. */
    auto where = as<WhereClause>(stmt->where);

    return where->where;
}

Expr * get_select_expr(const char * sql, size_t index)
{
    /* Get catalog, lexer, parser, and sema object. */
    Catalog &C = Catalog::Get();
    Diagnostic diag(false, std::cout, std::cerr);
    std::istringstream in(sql);
    Lexer lexer(diag, C.get_pool(), "-", in);
    Parser parser(lexer);
    Sema sema(diag);

    /* Parse and analyze SQL statement. */
    auto stmt = as<SelectStmt>(parser.parse());
    sema(*stmt);

    /* Extract WHERE clause and obtain cnf. */
    auto select = as<SelectClause>(stmt->select);

    return (select->select)[index].first;
}

Stmt * get_statement(const char *sql)
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

/*======================================================================================================================
 * Test ExressionEvaluator.
 *====================================================================================================================*/

TEST_CASE("ExpressionEvaluator", "[unit]")
{
    using std::to_string;
    using decimal = int64_t;

    /* Get Catalog and create new database to use for unit testing. */
    Catalog::Clear();
    Catalog &C = Catalog::Get();
    auto &db = C.add_database("mydb");
    C.set_database_in_use(db);

    /* Create pooled strings. */
    const char *col_int64_t = C.pool("col_int64_t");
    const char *col_float   = C.pool("col_float");
    const char *col_double  = C.pool("col_double");
    const char *col_decimal = C.pool("col_decimal");
    const char *col_bool    = C.pool("col_bool");
    const char *col_char    = C.pool("col_char");
    const char *tbl_tbl1    = C.pool("tbl1");
    //const char *tbl_tbl2    = C.pool("tbl2");

    /* Create tables. */
    Table &tbl1 = db.add_table(tbl_tbl1);
    //Table &tbl2 = db.add_table(tbl_tbl2);

    /* Add columns to tables. */
    tbl1.push_back(Type::Get_Integer(Type::TY_Vector, 4), col_int64_t);
    tbl1.push_back(Type::Get_Float(Type::TY_Vector), col_float);
    tbl1.push_back(Type::Get_Double(Type::TY_Vector), col_double);
    tbl1.push_back(Type::Get_Decimal(Type::TY_Vector, 8, 2), col_decimal);
    tbl1.push_back(Type::Get_Boolean(Type::TY_Vector), col_bool);
    tbl1.push_back(Type::Get_Char(Type::TY_Vector, 4), col_char);
    //tbl2.push_back(Type::Get_Float(Type::TY_Vector), col_float);
    //tbl2.push_back(Type::Get_Double(Type::TY_Vector), col_double);
    //tbl2.push_back(Type::Get_Decimal(Type::TY_Vector, 8, 2), col_decimal);
    //tbl2.push_back(Type::Get_Boolean(Type::TY_Vector), col_bool);
    //tbl2.push_back(Type::Get_Char(Type::TY_Vector, 7), col_char);

    /* Create tuple schema. */
    OperatorSchema schema;
    schema.add_element({tbl_tbl1, col_int64_t}, tbl1[col_int64_t].type);
    schema.add_element({tbl_tbl1, col_float}, tbl1[col_float].type);
    schema.add_element({tbl_tbl1, col_double}, tbl1[col_double].type);
    schema.add_element({tbl_tbl1, col_decimal}, tbl1[col_decimal].type);
    schema.add_element({tbl_tbl1, col_bool}, tbl1[col_bool].type);
    schema.add_element({tbl_tbl1, col_char}, tbl1[col_char].type);

    /* Create tuple and insert data. */
    tuple_type tup;
    int64_t col_int64_t_val = 42;
    float col_float_val = 13.37;
    double col_double_val = 123.45;
    decimal col_decimal_val = 237;
    bool col_bool_val = true;
    std::string col_char_val = "the string.";
    tup.emplace_back(col_int64_t_val);
    tup.emplace_back(col_float_val);
    tup.emplace_back(col_double_val);
    tup.emplace_back(col_decimal_val);
    tup.emplace_back(col_bool_val);
    tup.emplace_back(col_char_val);

    ExpressionEvaluator eval(schema, tup);

#define TEST(SQL, NAME, TYPE, VALUE) { \
    DYNAMIC_SECTION(NAME "/" << #TYPE) \
    { \
        auto stmt = as<const SelectStmt>(get_statement(SQL)); \
        auto select = as<const SelectClause>(stmt->select); \
        auto expr = select->select[0].first; \
        eval(*expr); \
        auto r = eval.result(); \
        delete stmt; \
        REQUIRE(std::holds_alternative<TYPE>(r)); \
        REQUIRE(std::get<TYPE>(r) == (VALUE)); \
    } \
}
#define TEST_EXPR(EXPR, NAME, TYPE, VALUE) \
        TEST("SELECT " EXPR " FROM tbl1;", NAME, TYPE, VALUE)

    /* Constants */
    TEST_EXPR("42", "constant", int64_t, 42);
    TEST_EXPR("13.37", "constant", double, 13.37);
    TEST_EXPR("TRUE", "constant", bool, true);
    TEST_EXPR("\"Hello, World!\"", "constant", std::string, "Hello, World!");

    /* Designators */
    TEST_EXPR("col_int64_t", "designator/attr", int64_t, col_int64_t_val);
    TEST_EXPR("tbl1.col_int64_t", "designator/tbl", int64_t, col_int64_t_val);

    TEST_EXPR("col_float", "designator/attr", float, col_float_val);
    TEST_EXPR("tbl1.col_float", "designator/tbl", float, col_float_val);

    TEST_EXPR("col_double", "designator/attr", double, col_double_val);
    TEST_EXPR("tbl1.col_double", "designator/tbl", double, col_double_val);

    TEST_EXPR("col_decimal", "designator/attr", int64_t, col_decimal_val);
    TEST_EXPR("tbl1.col_decimal", "designator/tbl", int64_t, col_decimal_val);

    TEST_EXPR("col_bool", "designator/attr", bool, col_bool_val);
    TEST_EXPR("tbl1.col_bool", "designator/tbl", bool, col_bool_val);

    TEST_EXPR("col_char", "designator/attr", std::string, col_char_val);
    TEST_EXPR("tbl1.col_char", "designator/tbl", std::string, col_char_val);

    /* Binary operators */
    TEST_EXPR("col_int64_t + 13", "binary/arithmetic/+", int64_t, col_int64_t_val + 13);
    TEST_EXPR("col_int64_t - 13", "binary/arithmetic/-", int64_t, col_int64_t_val - 13);
    TEST_EXPR("col_int64_t * 13", "binary/arithmetic/*", int64_t, col_int64_t_val * 13);
    TEST_EXPR("col_int64_t / 13", "binary/arithmetic//", int64_t, col_int64_t_val / 13);
    TEST_EXPR("col_int64_t % 13", "binary/arithmetic/%", int64_t, col_int64_t_val % 13);

    TEST_EXPR("col_float + 13", "binary/arithmetic/+", double, Approx(col_float_val + 13));
    TEST_EXPR("col_float - 13", "binary/arithmetic/-", double, Approx(col_float_val - 13));
    TEST_EXPR("col_float * 13", "binary/arithmetic/*", double, Approx(col_float_val * 13));
    TEST_EXPR("col_float / 13", "binary/arithmetic//", double, Approx(col_float_val / 13));

    TEST_EXPR("col_double + 13", "binary/arithmetic/+", double, Approx(col_double_val + 13));
    TEST_EXPR("col_double - 13", "binary/arithmetic/-", double, Approx(col_double_val - 13));
    TEST_EXPR("col_double * 13", "binary/arithmetic/*", double, Approx(col_double_val * 13));
    TEST_EXPR("col_double / 13", "binary/arithmetic//", double, Approx(col_double_val / 13));

    TEST_EXPR("col_decimal + col_decimal", "binary/arithmetic/+", decimal, col_decimal_val + col_decimal_val);
    TEST_EXPR("col_decimal - col_decimal", "binary/arithmetic/-", decimal, col_decimal_val - col_decimal_val);
    TEST_EXPR("col_decimal * col_decimal", "binary/arithmetic/*", decimal, col_decimal_val * col_decimal_val / 100);
    TEST_EXPR("col_decimal / col_decimal", "binary/arithmetic//", decimal, col_decimal_val / col_decimal_val / 100);

    TEST_EXPR("col_int64_t <  13", "binary/comparison/<",  bool, col_int64_t_val <  13);
    TEST_EXPR("col_int64_t <= 13", "binary/comparison/<=", bool, col_int64_t_val <= 13);
    TEST_EXPR("col_int64_t >  13", "binary/comparison/>",  bool, col_int64_t_val >  13);
    TEST_EXPR("col_int64_t >= 13", "binary/comparison/>=", bool, col_int64_t_val >= 13);
    TEST_EXPR("col_int64_t != 13", "binary/comparison/!=", bool, col_int64_t_val != 13);
    TEST_EXPR("col_int64_t =  13", "binary/comparison/=",  bool, col_int64_t_val == 13);

    TEST_EXPR("col_float <  13", "binary/comparison/<",  bool, col_float_val <  13);
    TEST_EXPR("col_float <= 13", "binary/comparison/<=", bool, col_float_val <= 13);
    TEST_EXPR("col_float >  13", "binary/comparison/>",  bool, col_float_val >  13);
    TEST_EXPR("col_float >= 13", "binary/comparison/>=", bool, col_float_val >= 13);
    TEST_EXPR("col_float != 13", "binary/comparison/!=", bool, col_float_val != 13);
    TEST_EXPR("col_float =  13", "binary/comparison/=",  bool, col_float_val == 13);

    TEST_EXPR("col_double <  13", "binary/comparison/<",  bool, col_double_val <  13);
    TEST_EXPR("col_double <= 13", "binary/comparison/<=", bool, col_double_val <= 13);
    TEST_EXPR("col_double >  13", "binary/comparison/>",  bool, col_double_val >  13);
    TEST_EXPR("col_double >= 13", "binary/comparison/>=", bool, col_double_val >= 13);
    TEST_EXPR("col_double != 13", "binary/comparison/!=", bool, col_double_val != 13);
    TEST_EXPR("col_double =  13", "binary/comparison/=",  bool, col_double_val == 13);

    TEST_EXPR("col_decimal <  13", "binary/comparison/<",  bool, col_decimal_val <  13);
    TEST_EXPR("col_decimal <= 13", "binary/comparison/<=", bool, col_decimal_val <= 13);
    TEST_EXPR("col_decimal >  13", "binary/comparison/>",  bool, col_decimal_val >  13);
    TEST_EXPR("col_decimal >= 13", "binary/comparison/>=", bool, col_decimal_val >= 13);
    TEST_EXPR("col_decimal != 13", "binary/comparison/!=", bool, col_decimal_val != 13);
    TEST_EXPR("col_decimal =  13", "binary/comparison/=",  bool, col_decimal_val == 13);

    TEST_EXPR("col_bool != TRUE", "binary/comparison/!=", bool, col_bool_val != true);
    TEST_EXPR("col_bool =  TRUE", "binary/comparison/=",  bool, col_bool_val == true);

    TEST_EXPR("col_char <  \"Hello, World!\"", "binary/comparison/<",  bool, col_char_val <  "Hello, World!");
    TEST_EXPR("col_char <= \"Hello, World!\"", "binary/comparison/<=", bool, col_char_val <= "Hello, World!");
    TEST_EXPR("col_char >  \"Hello, World!\"", "binary/comparison/>",  bool, col_char_val >  "Hello, World!");
    TEST_EXPR("col_char >= \"Hello, World!\"", "binary/comparison/>=", bool, col_char_val >= "Hello, World!");
    TEST_EXPR("col_char != \"Hello, World!\"", "binary/comparison/!=", bool, col_char_val != "Hello, World!");
    TEST_EXPR("col_char =  \"Hello, World!\"", "binary/comparison/=",  bool, col_char_val == "Hello, World!");

    TEST_EXPR("col_bool AND TRUE", "binary/logical/and", bool, col_bool_val and true);
    TEST_EXPR("col_bool OR FALSE", "binary/logical/or",  bool, col_bool_val or false);

    TEST_EXPR("col_char .. \"test\"", "binary/string/..", std::string, col_char_val + "test");

    /* Unary operators */
    TEST_EXPR("+col_int64_t", "unary/arithmetic/+", int64_t, +col_int64_t_val);
    TEST_EXPR("-col_int64_t", "unary/arithmetic/-", int64_t, -col_int64_t_val);

    TEST_EXPR("+col_float", "unary/arithmetic/+", float, +col_float_val); // TODO returns double?
    TEST_EXPR("-col_float", "unary/arithmetic/-", float, -col_float_val); // TODO returns double?

    TEST_EXPR("+col_double", "unary/arithmetic/+", double, +col_double_val);
    TEST_EXPR("-col_double", "unary/arithmetic/-", double, -col_double_val);

    TEST_EXPR("+col_decimal", "unary/arithmetic/+", decimal, +col_decimal_val);
    TEST_EXPR("-col_decimal", "unary/arithmetic/-", decimal, -col_decimal_val);

    TEST_EXPR("NOT col_bool", "unary/logical/not", bool, not col_bool_val);

    /* Function application */
    // TODO

#undef TEST_EXPR
#undef TEST
}
