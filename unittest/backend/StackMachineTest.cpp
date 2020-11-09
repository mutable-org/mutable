#include "catch.hpp"

#include "backend/StackMachine.hpp"
#include "mutable/catalog/Type.hpp"
#include "mutable/IR/CNF.hpp"
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "testutil.hpp"
#include "util/Diagnostic.hpp"
#include <cmath>
#include <string>


using namespace m;


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

TEST_CASE("StackMachine/Expressions", "[core][backend]")
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
    tbl1.push_back(col_int64_t, Type::Get_Integer(Type::TY_Vector, 8));
    tbl1.push_back(col_float,   Type::Get_Float(Type::TY_Vector));
    tbl1.push_back(col_double,  Type::Get_Double(Type::TY_Vector));
    tbl1.push_back(col_decimal, Type::Get_Decimal(Type::TY_Vector, 8, 2));
    tbl1.push_back(col_bool,    Type::Get_Boolean(Type::TY_Vector));
    tbl1.push_back(col_char,    Type::Get_Char(Type::TY_Vector, 4));

    /* Create tuple schema. */
    Schema schema;
    schema.add({tbl_tbl1, col_int64_t}, tbl1[col_int64_t].type);
    schema.add({tbl_tbl1, col_float},   tbl1[col_float].type);
    schema.add({tbl_tbl1, col_double},  tbl1[col_double].type);
    schema.add({tbl_tbl1, col_decimal}, tbl1[col_decimal].type);
    schema.add({tbl_tbl1, col_bool},    tbl1[col_bool].type);
    schema.add({tbl_tbl1, col_char},    tbl1[col_char].type);

    /* Create tuple and insert data. */
    Tuple in(schema);
    int64_t col_int64_t_val = 42;
    float col_float_val = 13.37;
    double col_double_val = 123.45;
    decimal col_decimal_val = 237;
    bool col_bool_val = true;
    const char *col_char_val = "YES";
    in.set(0, col_int64_t_val);
    in.set(1, col_float_val);
    in.set(2, col_double_val);
    in.set(3, col_decimal_val);
    in.set(4, col_bool_val);
    in.not_null(5);
    strcpy(reinterpret_cast<char*>(in[5].as_p()), col_char_val);

    auto __test = [&](const char *exprstr, const char *name, auto cond) {
        DYNAMIC_SECTION(name)
        {
            std::ostringstream oss;
            oss << "SELECT " << exprstr << " FROM tbl1;\n";
            auto stmt = as<const SelectStmt>(get_Stmt(oss.str().c_str()));
            auto select = as<const SelectClause>(stmt->select);
            auto expr = select->select[0].first;
            StackMachine eval(schema);
            eval.emit(*expr, 1);
            eval.emit_St_Tup(0, 0, expr->type());
            Tuple out({ expr->type() });
            Tuple *args[] = {&out, &in};
            eval(args);
            if(out.is_null(0)) {
                expr->dump();
                eval.dump();
                std::cerr << "Got NULL\n";
                delete stmt;
            }
            REQUIRE(not out.is_null(0));
            bool cond_res = cond(out[0]);
            if (not cond_res) {
                expr->dump();
                eval.dump();
                std::cerr << "Got " << out[0] << '\n';
                delete stmt;
            }
            REQUIRE(cond_res);
            delete stmt;
        }
    };

#define TEST(EXPR, NAME, COND) __test(EXPR, NAME, [&](Value RES) { return COND; })

    /* Constants */
    TEST("42", "constant/int64_t", RES == 42);
    TEST("13.37", "constant/double", RES == 13.37);
    TEST("TRUE", "constant/bool", RES == true);
    TEST("\"Hello, World!\"", "constant/char", streq(RES.as<char*>(), "Hello, World!"));

    /* Designators */
    TEST("col_int64_t", "designator/attr/int64_t", RES == col_int64_t_val);
    TEST("tbl1.col_int64_t", "designator/tbl/int64_t", RES == col_int64_t_val);

    TEST("col_float", "designator/attr/float", RES == col_float_val);
    TEST("tbl1.col_float", "designator/tbl/float", RES == col_float_val);

    TEST("col_double", "designator/attr/double", RES == col_double_val);
    TEST("tbl1.col_double", "designator/tbl/double", RES == col_double_val);

    TEST("col_decimal", "designator/attr/decimal", RES == col_decimal_val);
    TEST("tbl1.col_decimal", "designator/tbl/decimal", RES == col_decimal_val);

    TEST("col_bool", "designator/attr/bool", RES == col_bool_val);
    TEST("tbl1.col_bool", "designator/tbl/bool", RES == col_bool_val);

    TEST("col_char", "designator/attr/char", streq(RES.as<char*>(), col_char_val));
    TEST("tbl1.col_char", "designator/tbl/char", streq(RES.as<char*>(), col_char_val));

    /* Binary operators */
    TEST("col_int64_t + 13", "binary/arithmetic/+/int64_t", RES == col_int64_t_val + 13);
    TEST("col_int64_t - 13", "binary/arithmetic/-/int64_t", RES == col_int64_t_val - 13);
    TEST("col_int64_t * 13", "binary/arithmetic/*/int64_t", RES == col_int64_t_val * 13);
    TEST("col_int64_t / 13", "binary/arithmetic///int64_t", RES == col_int64_t_val / 13);
    TEST("col_int64_t % 13", "binary/arithmetic/%/int64_t", RES == col_int64_t_val % 13);

    TEST("col_float + 13", "binary/arithmetic/+/float", RES.as_f() == Approx(col_float_val + 13));
    TEST("col_float - 13", "binary/arithmetic/-/float", RES.as_f() == Approx(col_float_val - 13));
    TEST("col_float * 13", "binary/arithmetic/*/float", RES.as_f() == Approx(col_float_val * 13));
    TEST("col_float / 13", "binary/arithmetic///float", RES.as_f() == Approx(col_float_val / 13));

    TEST("col_double + 13", "binary/arithmetic/+/double", RES.as_d() == Approx(col_double_val + 13));
    TEST("col_double - 13", "binary/arithmetic/-/double", RES.as_d() == Approx(col_double_val - 13));
    TEST("col_double * 13", "binary/arithmetic/*/double", RES.as_d() == Approx(col_double_val * 13));
    TEST("col_double / 13", "binary/arithmetic///double", RES.as_d() == Approx(col_double_val / 13));

    TEST("col_decimal + col_decimal", "binary/arithmetic/+/decimal", RES == col_decimal_val + col_decimal_val);
    TEST("col_decimal - col_decimal", "binary/arithmetic/-/decimal", RES == col_decimal_val - col_decimal_val);
    TEST("col_decimal * col_decimal", "binary/arithmetic/*/decimal", RES == col_decimal_val * col_decimal_val / 100);
    TEST("col_decimal / col_decimal", "binary/arithmetic///decimal", RES == (col_decimal_val / col_decimal_val) * 100);

    TEST("col_int64_t <  13", "binary/comparison/</int64_t",  RES == (col_int64_t_val <  13));
    TEST("col_int64_t <= 13", "binary/comparison/<=/int64_t", RES == (col_int64_t_val <= 13));
    TEST("col_int64_t >  13", "binary/comparison/>/int64_t",  RES == (col_int64_t_val >  13));
    TEST("col_int64_t >= 13", "binary/comparison/>=/int64_t", RES == (col_int64_t_val >= 13));
    TEST("col_int64_t != 13", "binary/comparison/!=/int64_t", RES == (col_int64_t_val != 13));
    TEST("col_int64_t =  13", "binary/comparison/=/int64_t",  RES == (col_int64_t_val == 13));

    TEST("col_float <  13", "binary/comparison/</float",  RES == (col_float_val <  13));
    TEST("col_float <= 13", "binary/comparison/<=/float", RES == (col_float_val <= 13));
    TEST("col_float >  13", "binary/comparison/>/float",  RES == (col_float_val >  13));
    TEST("col_float >= 13", "binary/comparison/>=/float", RES == (col_float_val >= 13));
    TEST("col_float != 13", "binary/comparison/!=/float", RES == (col_float_val != 13));
    TEST("col_float =  13", "binary/comparison/=/float",  RES == (col_float_val == 13));

    TEST("col_double <  13", "binary/comparison/</double",  RES == (col_double_val <  13));
    TEST("col_double <= 13", "binary/comparison/<=/double", RES == (col_double_val <= 13));
    TEST("col_double >  13", "binary/comparison/>/double",  RES == (col_double_val >  13));
    TEST("col_double >= 13", "binary/comparison/>=/double", RES == (col_double_val >= 13));
    TEST("col_double != 13", "binary/comparison/!=/double", RES == (col_double_val != 13));
    TEST("col_double =  13", "binary/comparison/=/double",  RES == (col_double_val == 13));

    TEST("col_decimal <  13", "binary/comparison/</decimal",  RES == (col_decimal_val / 100.0 <  13));
    TEST("col_decimal <= 13", "binary/comparison/<=/decimal", RES == (col_decimal_val / 100.0 <= 13));
    TEST("col_decimal >  13", "binary/comparison/>/decimal",  RES == (col_decimal_val / 100.0 >  13));
    TEST("col_decimal >= 13", "binary/comparison/>=/decimal", RES == (col_decimal_val / 100.0 >= 13));
    TEST("col_decimal != 13", "binary/comparison/!=/decimal", RES == (col_decimal_val / 100.0 != 13));
    TEST("col_decimal =  13", "binary/comparison/=/decimal",  RES == (col_decimal_val / 100.0 == 13));

    TEST("col_bool != TRUE", "binary/comparison/!=/bool", RES == (col_bool_val != true));
    TEST("col_bool =  TRUE", "binary/comparison/=/bool",  RES == (col_bool_val == true));

    TEST("col_char LIKE \"YES\"",         "binary/comparison/like_const/char",    RES == std::regex_match(std::string(col_char_val), std::regex("YES")));
    TEST("col_char LIKE \"_ES\"",         "binary/comparison/like_wildcard/char", RES == std::regex_match(std::string(col_char_val), std::regex(".ES")));
    TEST("col_char LIKE \"Y%\"",          "binary/comparison/like_any/char",      RES == std::regex_match(std::string(col_char_val), std::regex("Y(.*)")));
    TEST("col_char LIKE \"N\" .. \"%\"",  "binary/comparison/like_const/char",    RES == std::regex_match(std::string(col_char_val), std::regex("N(.*)")));

    TEST("col_char <  \"Hello, World!\"", "binary/comparison/</char",  RES == std::string(col_char_val) <  "Hello, World");
    TEST("col_char <= \"Hello, World!\"", "binary/comparison/<=/char", RES == std::string(col_char_val) <= "Hello, World");
    TEST("col_char >  \"Hello, World!\"", "binary/comparison/>/char",  RES == std::string(col_char_val) >  "Hello, World");
    TEST("col_char >= \"Hello, World!\"", "binary/comparison/>=/char", RES == std::string(col_char_val) >= "Hello, World");
    TEST("col_char != \"Hello, World!\"", "binary/comparison/!=/char", RES == (std::string(col_char_val) != "Hello, World"));
    TEST("col_char =  \"Hello, World!\"", "binary/comparison/=/char",  RES == (std::string(col_char_val) == "Hello, World"));

    TEST("col_bool AND TRUE", "binary/logical/and/bool", RES == (col_bool_val and true));
    TEST("col_bool OR FALSE", "binary/logical/or/bool",  RES == (col_bool_val or false));

    TEST("col_char .. \"test\"", "binary/string/../char", std::string(RES.as<char*>()) == "YEStest");

    /* Unary operators */
    TEST("+col_int64_t", "unary/arithmetic/+/int64_t", RES == +col_int64_t_val);
    TEST("-col_int64_t", "unary/arithmetic/-/int64_t", RES == -col_int64_t_val);

    TEST("+col_float", "unary/arithmetic/+/float", RES == +col_float_val);
    TEST("-col_float", "unary/arithmetic/-/float", RES == -col_float_val);

    TEST("+col_double", "unary/arithmetic/+/double", RES == +col_double_val);
    TEST("-col_double", "unary/arithmetic/-/double", RES == -col_double_val);

    TEST("+col_decimal", "unary/arithmetic/+/decimal", RES == +col_decimal_val);
    TEST("-col_decimal", "unary/arithmetic/-/decimal", RES == -col_decimal_val);

    TEST("NOT col_bool", "unary/logical/not/bool", RES == not col_bool_val);

    /* Function application */
    // TODO

    /*----- Mixed datatypes ------------------------------------------------------------------------------------------*/
    /* LHS int64 */
    TEST("col_int64_t + col_float",   "binary/arithmetic/+/int64_t,float",   RES.as_d() == Approx(col_int64_t_val + col_float_val));
    TEST("col_int64_t + col_double",  "binary/arithmetic/+/int64_t,double",  RES.as_d() == Approx(col_int64_t_val + col_double_val));
    TEST("col_int64_t + col_decimal", "binary/arithmetic/+/int64_t,decimal", RES == (col_int64_t_val * 100  + col_decimal_val));

    TEST("col_int64_t - col_float",   "binary/arithmetic/-/int64_t,float",   RES.as_d() == Approx(col_int64_t_val - col_float_val));
    TEST("col_int64_t - col_double",  "binary/arithmetic/-/int64_t,double",  RES.as_d() == Approx(col_int64_t_val - col_double_val));
    TEST("col_int64_t - col_decimal", "binary/arithmetic/-/int64_t,decimal", RES == (col_int64_t_val * 100  - col_decimal_val));

    TEST("col_int64_t * col_float",   "binary/arithmetic/*/int64_t,float",   RES.as_d() == Approx(col_int64_t_val * col_float_val));
    TEST("col_int64_t * col_double",  "binary/arithmetic/*/int64_t,double",  RES.as_d() == Approx(col_int64_t_val * col_double_val));
    TEST("col_int64_t * col_decimal", "binary/arithmetic/*/int64_t,decimal", RES == (col_int64_t_val * col_decimal_val));

    TEST("col_int64_t / col_float",   "binary/arithmetic///int64_t,float",   RES.as_d() == Approx(col_int64_t_val / col_float_val));
    TEST("col_int64_t / col_double",  "binary/arithmetic///int64_t,double",  RES.as_d() == Approx(col_int64_t_val / col_double_val));
    TEST("col_int64_t / col_decimal", "binary/arithmetic///int64_t,decimal", RES == ((col_int64_t_val * 100 * 100) / col_decimal_val));

    /* LHS float */
    TEST("col_float + col_int64_t", "binary/arithmetic/+/float,int64_t", RES.as_d() == Approx(col_float_val + col_int64_t_val));
    TEST("col_float + col_double",  "binary/arithmetic/+/float,double",  RES.as_d() == Approx(col_float_val + col_double_val));
    TEST("col_float + col_decimal", "binary/arithmetic/+/float,decimal", RES.as_i() == int64_t(col_float_val * 100) + col_decimal_val);

    TEST("col_float - col_int64_t", "binary/arithmetic/-/float,int64_t", RES.as_d() == Approx(col_float_val - col_int64_t_val));
    TEST("col_float - col_double",  "binary/arithmetic/-/float,double",  RES.as_d() == Approx(col_float_val - col_double_val));
    TEST("col_float - col_decimal", "binary/arithmetic/-/float,decimal", RES.as_i() == int64_t(col_float_val * 100 - col_decimal_val));

    TEST("col_float * col_int64_t", "binary/arithmetic/*/float,int64_t", RES.as_d() == Approx(col_float_val * col_int64_t_val));
    TEST("col_float * col_double",  "binary/arithmetic/*/float,double",  RES.as_d() == Approx(col_float_val * col_double_val));
    TEST("col_float * col_decimal", "binary/arithmetic/*/float,decimal", RES.as_i() == int64_t(col_float_val * col_decimal_val));

    TEST("col_float / col_int64_t", "binary/arithmetic///float,int64_t", RES.as_d() == Approx(col_float_val / col_int64_t_val));
    TEST("col_float / col_double",  "binary/arithmetic///float,double",  RES.as_d() == Approx(col_float_val / col_double_val));
    TEST("col_float / col_decimal", "binary/arithmetic///float,decimal", RES.as_i() == int64_t(col_float_val * 100 * 100 / col_decimal_val));

    /* LHS double */
    TEST("col_double + col_int64_t", "binary/arithmetic/+/double,int64_t", RES.as_d() == Approx(col_double_val + col_int64_t_val));
    TEST("col_double + col_float",   "binary/arithmetic/+/double,float",   RES.as_d() == Approx(col_double_val + col_float_val));
    TEST("col_double + col_decimal", "binary/arithmetic/+/double,decimal", RES.as_i() == int64_t(col_double_val * 100) + col_decimal_val);

    TEST("col_double * col_int64_t", "binary/arithmetic/*/double,int64_t", RES.as_d() == Approx(col_double_val * col_int64_t_val));
    TEST("col_double * col_float",   "binary/arithmetic/*/double,float",   RES.as_d() == Approx(col_double_val * col_float_val));
    TEST("col_double * col_decimal", "binary/arithmetic/*/double,decimal", RES.as_i() == int64_t(col_double_val * col_decimal_val));

    /* LHS decimal */
    TEST("col_decimal + col_int64_t", "binary/arithmetic/+/decimal,int64_t", RES == (col_decimal_val + col_int64_t_val * 100));
    TEST("col_decimal + col_float",   "binary/arithmetic/+/decimal,float",   RES.as_i() == col_decimal_val + int64_t(col_float_val * 100));
    TEST("col_decimal + col_double",  "binary/arithmetic/+/decimal,double",  RES.as_i() == col_decimal_val + int64_t(col_double_val * 100));

    TEST("col_decimal * col_int64_t", "binary/arithmetic/*/decimal,int64_t", RES == (col_decimal_val * col_int64_t_val));
    TEST("col_decimal * col_float",   "binary/arithmetic/*/decimal,float",   RES.as_i() == int64_t(col_decimal_val * col_float_val));
    TEST("col_decimal * col_double",  "binary/arithmetic/*/decimal,double",  RES.as_i() == int64_t(col_decimal_val * col_double_val));

    TEST("col_int64_t < col_float",   "binary/logical/</int64_t,float",   RES == (col_int64_t_val < col_float_val));
    TEST("col_int64_t < col_double",  "binary/logical/</int64_t,double",  RES == (col_int64_t_val < col_double_val));
    TEST("col_int64_t < col_decimal", "binary/logical/</int64_t,decimal", RES == (col_int64_t_val < col_decimal_val / 100.0));

    TEST("col_float < col_int64_t", "binary/logical/</float,int64_t", RES == (col_float_val < col_int64_t_val));
    TEST("col_float < col_double",  "binary/logical/</float,double",  RES == (col_float_val < col_double_val));
    TEST("col_float < col_decimal", "binary/logical/</float,decimal", RES == (col_float_val < col_decimal_val / 100.0));

    TEST("col_double < col_int64_t", "binary/logical/</double,int64_t", RES == (col_double_val < col_int64_t_val));
    TEST("col_double < col_float",   "binary/logical/</double,float",   RES == (col_double_val < col_float_val));
    TEST("col_double < col_decimal", "binary/logical/</double,decimal", RES == (col_double_val < col_decimal_val / 100.0));

    TEST("col_decimal < col_int64_t", "binary/logical/</decimal,int64_t", RES == (col_decimal_val / 100.0 < col_int64_t_val));
    TEST("col_decimal < col_float",   "binary/logical/</decimal,float",   RES == (col_decimal_val / 100.0 < col_float_val));
    TEST("col_decimal < col_double",  "binary/logical/</decimal,double",  RES == (col_decimal_val / 100.0 < col_double_val));

#undef TEST
}

TEST_CASE("StackMachine/Sel", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };

    SECTION("select first")
    {
        SM.add_and_emit_load(true);
        SM.add_and_emit_load(42);
        SM.add_and_emit_load(13);
        SM.emit_Sel();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 42);
    }

    SECTION("select second")
    {
        SM.add_and_emit_load(false);
        SM.add_and_emit_load(42);
        SM.add_and_emit_load(13);
        SM.emit_Sel();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 13);
    }

    SECTION("select with NULL condition")
    {
        SM.emit_Push_Null();
        SM.add_and_emit_load(42);
        SM.add_and_emit_load(13);
        SM.emit_Sel();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(res.is_null(0));
    }

    SECTION("select first that is NULL")
    {
        SM.add_and_emit_load(true);
        SM.emit_Push_Null();
        SM.add_and_emit_load(13);
        SM.emit_Sel();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(res.is_null(0));
    }

    SECTION("select second that is NULL")
    {
        SM.add_and_emit_load(false);
        SM.add_and_emit_load(42);
        SM.emit_Push_Null();
        SM.emit_Sel();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(res.is_null(0));
    }
}
