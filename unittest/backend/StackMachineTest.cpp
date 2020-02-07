#include "catch.hpp"

#include "backend/StackMachine.hpp"
#include "catalog/Type.hpp"
#include "IR/CNF.hpp"
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "testutil.hpp"
#include "util/Diagnostic.hpp"
#include <cmath>
#include <string>


using namespace db;


/*======================================================================================================================
 * Helper funtctions for test setup.
 *====================================================================================================================*/

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

/*======================================================================================================================
 * Test StackMachine.
 *====================================================================================================================*/

TEST_CASE("StackMachine", "[core][backend][stackmachine]")
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

    /* Create tables. */
    Table &tbl1 = db.add_table(tbl_tbl1);

    /* Add columns to tables. */
    tbl1.push_back(col_int64_t, Type::Get_Integer(Type::TY_Vector, 4));
    tbl1.push_back(col_float, Type::Get_Float(Type::TY_Vector));
    tbl1.push_back(col_double, Type::Get_Double(Type::TY_Vector));
    tbl1.push_back(col_decimal, Type::Get_Decimal(Type::TY_Vector, 8, 2));
    tbl1.push_back(col_bool, Type::Get_Boolean(Type::TY_Vector));
    tbl1.push_back(col_char, Type::Get_Char(Type::TY_Vector, 4));

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

#define TEST(EXPR, NAME, TYPE, VALUE) { \
    DYNAMIC_SECTION(NAME) \
    { \
        auto stmt = as<const SelectStmt>(get_Stmt("SELECT " EXPR " FROM tbl1;")); \
        auto select = as<const SelectClause>(stmt->select); \
        auto expr = select->select[0].first; \
        StackMachine eval(schema, *expr); \
        tuple_type result; \
        result.reserve(std::max(schema.size(), eval.required_stack_size())); \
        eval(&result, tup); \
        REQUIRE(result.size() == 1); \
        auto r = result[0]; \
        delete stmt; \
        CHECK(std::holds_alternative<TYPE>(r)); \
        REQUIRE(std::get<TYPE>(r) == (VALUE)); \
    } \
}

    /* Constants */
    TEST("42", "constant/int64_t", int64_t, 42);
    TEST("13.37", "constant/double", double, 13.37);
    TEST("TRUE", "constant/bool", bool, true);
    TEST("\"Hello, World!\"", "constant/char", std::string_view, "Hello, World!");

    /* Designators */
    TEST("col_int64_t", "designator/attr/int64_t", int64_t, col_int64_t_val);
    TEST("tbl1.col_int64_t", "designator/tbl/int64_t", int64_t, col_int64_t_val);

    TEST("col_float", "designator/attr/float", float, col_float_val);
    TEST("tbl1.col_float", "designator/tbl/float", float, col_float_val);

    TEST("col_double", "designator/attr/double", double, col_double_val);
    TEST("tbl1.col_double", "designator/tbl/double", double, col_double_val);

    TEST("col_decimal", "designator/attr/decimal", decimal, col_decimal_val);
    TEST("tbl1.col_decimal", "designator/tbl/decimal", decimal, col_decimal_val);

    TEST("col_bool", "designator/attr/bool", bool, col_bool_val);
    TEST("tbl1.col_bool", "designator/tbl/bool", bool, col_bool_val);

    TEST("col_char", "designator/attr/char", std::string_view, col_char_val);
    TEST("tbl1.col_char", "designator/tbl/char", std::string_view, col_char_val);

    /* Binary operators */
    TEST("col_int64_t + 13", "binary/arithmetic/+/int64_t", int64_t, col_int64_t_val + 13);
    TEST("col_int64_t - 13", "binary/arithmetic/-/int64_t", int64_t, col_int64_t_val - 13);
    TEST("col_int64_t * 13", "binary/arithmetic/*/int64_t", int64_t, col_int64_t_val * 13);
    TEST("col_int64_t / 13", "binary/arithmetic///int64_t", int64_t, col_int64_t_val / 13);
    TEST("col_int64_t % 13", "binary/arithmetic/%/int64_t", int64_t, col_int64_t_val % 13);

    TEST("col_float + 13", "binary/arithmetic/+/float", float, Approx(col_float_val + 13));
    TEST("col_float - 13", "binary/arithmetic/-/float", float, Approx(col_float_val - 13));
    TEST("col_float * 13", "binary/arithmetic/*/float", float, Approx(col_float_val * 13));
    TEST("col_float / 13", "binary/arithmetic///float", float, Approx(col_float_val / 13));

    TEST("col_double + 13", "binary/arithmetic/+/double", double, Approx(col_double_val + 13));
    TEST("col_double - 13", "binary/arithmetic/-/double", double, Approx(col_double_val - 13));
    TEST("col_double * 13", "binary/arithmetic/*/double", double, Approx(col_double_val * 13));
    TEST("col_double / 13", "binary/arithmetic///double", double, Approx(col_double_val / 13));

    TEST("col_decimal + col_decimal", "binary/arithmetic/+/decimal", decimal, col_decimal_val + col_decimal_val);
    TEST("col_decimal - col_decimal", "binary/arithmetic/-/decimal", decimal, col_decimal_val - col_decimal_val);
    TEST("col_decimal * col_decimal", "binary/arithmetic/*/decimal", decimal, col_decimal_val * col_decimal_val / 100);
    TEST("col_decimal / col_decimal", "binary/arithmetic///decimal", decimal, (col_decimal_val / col_decimal_val) * 100);

    TEST("col_int64_t <  13", "binary/comparison/</int64_t",  bool, col_int64_t_val <  13);
    TEST("col_int64_t <= 13", "binary/comparison/<=/int64_t", bool, col_int64_t_val <= 13);
    TEST("col_int64_t >  13", "binary/comparison/>/int64_t",  bool, col_int64_t_val >  13);
    TEST("col_int64_t >= 13", "binary/comparison/>=/int64_t", bool, col_int64_t_val >= 13);
    TEST("col_int64_t != 13", "binary/comparison/!=/int64_t", bool, col_int64_t_val != 13);
    TEST("col_int64_t =  13", "binary/comparison/=/int64_t",  bool, col_int64_t_val == 13);

    TEST("col_float <  13", "binary/comparison/</float",  bool, col_float_val <  13);
    TEST("col_float <= 13", "binary/comparison/<=/float", bool, col_float_val <= 13);
    TEST("col_float >  13", "binary/comparison/>/float",  bool, col_float_val >  13);
    TEST("col_float >= 13", "binary/comparison/>=/float", bool, col_float_val >= 13);
    TEST("col_float != 13", "binary/comparison/!=/float", bool, col_float_val != 13);
    TEST("col_float =  13", "binary/comparison/=/float",  bool, col_float_val == 13);

    TEST("col_double <  13", "binary/comparison/</double",  bool, col_double_val <  13);
    TEST("col_double <= 13", "binary/comparison/<=/double", bool, col_double_val <= 13);
    TEST("col_double >  13", "binary/comparison/>/double",  bool, col_double_val >  13);
    TEST("col_double >= 13", "binary/comparison/>=/double", bool, col_double_val >= 13);
    TEST("col_double != 13", "binary/comparison/!=/double", bool, col_double_val != 13);
    TEST("col_double =  13", "binary/comparison/=/double",  bool, col_double_val == 13);

    TEST("col_decimal <  13", "binary/comparison/</decimal",  bool, col_decimal_val / 100.0 <  13);
    TEST("col_decimal <= 13", "binary/comparison/<=/decimal", bool, col_decimal_val / 100.0 <= 13);
    TEST("col_decimal >  13", "binary/comparison/>/decimal",  bool, col_decimal_val / 100.0 >  13);
    TEST("col_decimal >= 13", "binary/comparison/>=/decimal", bool, col_decimal_val / 100.0 >= 13);
    TEST("col_decimal != 13", "binary/comparison/!=/decimal", bool, col_decimal_val / 100.0 != 13);
    TEST("col_decimal =  13", "binary/comparison/=/decimal",  bool, col_decimal_val / 100.0 == 13);

    TEST("col_bool != TRUE", "binary/comparison/!=/bool", bool, col_bool_val != true);
    TEST("col_bool =  TRUE", "binary/comparison/=/bool",  bool, col_bool_val == true);

    TEST("col_char <  \"Hello, World!\"", "binary/comparison/</char",  bool, col_char_val <  "Hello, World!");
    TEST("col_char <= \"Hello, World!\"", "binary/comparison/<=/char", bool, col_char_val <= "Hello, World!");
    TEST("col_char >  \"Hello, World!\"", "binary/comparison/>/char",  bool, col_char_val >  "Hello, World!");
    TEST("col_char >= \"Hello, World!\"", "binary/comparison/>=/char", bool, col_char_val >= "Hello, World!");
    TEST("col_char != \"Hello, World!\"", "binary/comparison/!=/char", bool, col_char_val != "Hello, World!");
    TEST("col_char =  \"Hello, World!\"", "binary/comparison/=/char",  bool, col_char_val == "Hello, World!");

    TEST("col_bool AND TRUE", "binary/logical/and/bool", bool, col_bool_val and true);
    TEST("col_bool OR FALSE", "binary/logical/or/bool",  bool, col_bool_val or false);

    TEST("col_char .. \"test\"", "binary/string/../char", std::string_view, col_char_val + "test");

    /* Unary operators */
    TEST("+col_int64_t", "unary/arithmetic/+/int64_t", int64_t, +col_int64_t_val);
    TEST("-col_int64_t", "unary/arithmetic/-/int64_t", int64_t, -col_int64_t_val);

    TEST("+col_float", "unary/arithmetic/+/float", float, +col_float_val);
    TEST("-col_float", "unary/arithmetic/-/float", float, -col_float_val);

    TEST("+col_double", "unary/arithmetic/+/double", double, +col_double_val);
    TEST("-col_double", "unary/arithmetic/-/double", double, -col_double_val);

    TEST("+col_decimal", "unary/arithmetic/+/decimal", decimal, +col_decimal_val);
    TEST("-col_decimal", "unary/arithmetic/-/decimal", decimal, -col_decimal_val);

    TEST("NOT col_bool", "unary/logical/not/bool", bool, not col_bool_val);

    /* Function application */
    // TODO

    /*----- Mixed datatypes ------------------------------------------------------------------------------------------*/
    /* LHS int64 */
    TEST("col_int64_t + col_float",   "binary/arithmetic/+/int64_t,float",   float,   Approx(col_int64_t_val + col_float_val));
    TEST("col_int64_t + col_double",  "binary/arithmetic/+/int64_t,double",  double,  Approx(col_int64_t_val + col_double_val));
    TEST("col_int64_t + col_decimal", "binary/arithmetic/+/int64_t,decimal", decimal, col_int64_t_val * 100  + col_decimal_val);

    TEST("col_int64_t - col_float",   "binary/arithmetic/-/int64_t,float",   float,   Approx(col_int64_t_val - col_float_val));
    TEST("col_int64_t - col_double",  "binary/arithmetic/-/int64_t,double",  double,  Approx(col_int64_t_val - col_double_val));
    TEST("col_int64_t - col_decimal", "binary/arithmetic/-/int64_t,decimal", decimal, col_int64_t_val * 100  - col_decimal_val);

    TEST("col_int64_t * col_float",   "binary/arithmetic/*/int64_t,float",   float,   Approx(col_int64_t_val * col_float_val));
    TEST("col_int64_t * col_double",  "binary/arithmetic/*/int64_t,double",  double,  Approx(col_int64_t_val * col_double_val));
    TEST("col_int64_t * col_decimal", "binary/arithmetic/*/int64_t,decimal", decimal, col_int64_t_val * col_decimal_val);

    TEST("col_int64_t / col_float",   "binary/arithmetic///int64_t,float",   float,   Approx(col_int64_t_val / col_float_val));
    TEST("col_int64_t / col_double",  "binary/arithmetic///int64_t,double",  double,  Approx(col_int64_t_val / col_double_val));
    TEST("col_int64_t / col_decimal", "binary/arithmetic///int64_t,decimal", decimal, (col_int64_t_val * 100 * 100) / col_decimal_val);

    /* LHS float */
    TEST("col_float + col_int64_t", "binary/arithmetic/+/float,int64_t", float,  Approx(col_float_val + col_int64_t_val));
    TEST("col_float + col_double",  "binary/arithmetic/+/float,double",  double, Approx(col_float_val + col_double_val));
    TEST("col_float + col_decimal", "binary/arithmetic/+/float,decimal", float,  Approx(col_float_val + (col_decimal_val / 100.f)));

    TEST("col_float - col_int64_t", "binary/arithmetic/-/float,int64_t", float,  Approx(col_float_val - col_int64_t_val));
    TEST("col_float - col_double",  "binary/arithmetic/-/float,double",  double, Approx(col_float_val - col_double_val));
    TEST("col_float - col_decimal", "binary/arithmetic/-/float,decimal", float,  Approx(col_float_val - (col_decimal_val / 100.f)));

    TEST("col_float * col_int64_t", "binary/arithmetic/*/float,int64_t", float,  Approx(col_float_val * col_int64_t_val));
    TEST("col_float * col_double",  "binary/arithmetic/*/float,double",  double, Approx(col_float_val * col_double_val));
    TEST("col_float * col_decimal", "binary/arithmetic/*/float,decimal", float,  Approx(col_float_val * (col_decimal_val / 100.f)));

    TEST("col_float / col_int64_t", "binary/arithmetic///float,int64_t", float,  Approx(col_float_val / col_int64_t_val));
    TEST("col_float / col_double",  "binary/arithmetic///float,double",  double, Approx(col_float_val / col_double_val));
    TEST("col_float / col_decimal", "binary/arithmetic///float,decimal", float,  Approx(col_float_val / (col_decimal_val / 100.f)));

    /* LHS double */
    TEST("col_double + col_int64_t", "binary/arithmetic/+/double,int64_t", double, Approx(col_double_val + col_int64_t_val));
    TEST("col_double + col_float",   "binary/arithmetic/+/double,float",   double, Approx(col_double_val + col_float_val));
    TEST("col_double + col_decimal", "binary/arithmetic/+/double,decimal", double, Approx(col_double_val + (col_decimal_val / 100.)));

    TEST("col_double * col_int64_t", "binary/arithmetic/*/double,int64_t", double, Approx(col_double_val * col_int64_t_val));
    TEST("col_double * col_float",   "binary/arithmetic/*/double,float",   double, Approx(col_double_val * col_float_val));
    TEST("col_double * col_decimal", "binary/arithmetic/*/double,decimal", double, Approx(col_double_val * (col_decimal_val / 100.)));

    /* LHS decimal */
    TEST("col_decimal + col_int64_t", "binary/arithmetic/+/decimal,int64_t", decimal, col_decimal_val + col_int64_t_val * 100);
    TEST("col_decimal + col_float",   "binary/arithmetic/+/decimal,float",   float,   Approx(col_decimal_val / 100.0 + col_float_val));
    TEST("col_decimal + col_double",  "binary/arithmetic/+/decimal,double",  double,  Approx(col_decimal_val / 100.0 + col_double_val));

    TEST("col_decimal * col_int64_t", "binary/arithmetic/*/decimal,int64_t", decimal, col_decimal_val * col_int64_t_val);
    TEST("col_decimal * col_float",   "binary/arithmetic/*/decimal,float",   float,   Approx(col_decimal_val / 100.0 * col_float_val));
    TEST("col_decimal * col_double",  "binary/arithmetic/*/decimal,double",  double,  Approx(col_decimal_val / 100.0 * col_double_val));

    TEST("col_int64_t < col_float",   "binary/logical/</int64_t,float",   bool, col_int64_t_val < col_float_val);
    TEST("col_int64_t < col_double",  "binary/logical/</int64_t,double",  bool, col_int64_t_val < col_double_val);
    TEST("col_int64_t < col_decimal", "binary/logical/</int64_t,decimal", bool, col_int64_t_val < col_decimal_val / 100.0);

    TEST("col_float < col_int64_t", "binary/logical/</float,int64_t", bool, col_float_val < col_int64_t_val);
    TEST("col_float < col_double",  "binary/logical/</float,double",  bool, col_float_val < col_double_val);
    TEST("col_float < col_decimal", "binary/logical/</float,decimal", bool, col_float_val < col_decimal_val / 100.0);

    TEST("col_double < col_int64_t", "binary/logical/</double,int64_t", bool, col_double_val < col_int64_t_val);
    TEST("col_double < col_float",   "binary/logical/</double,float",   bool, col_double_val < col_float_val);
    TEST("col_double < col_decimal", "binary/logical/</double,decimal", bool, col_double_val < col_decimal_val / 100.0);

    TEST("col_decimal < col_int64_t", "binary/logical/</decimal,int64_t", bool, col_decimal_val / 100.0 < col_int64_t_val);
    TEST("col_decimal < col_float",   "binary/logical/</decimal,float",   bool, col_decimal_val / 100.0 < col_float_val);
    TEST("col_decimal < col_double",  "binary/logical/</decimal,double",  bool, col_decimal_val / 100.0 < col_double_val);

#undef TEST
}
