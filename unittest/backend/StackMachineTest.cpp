#include "catch2/catch.hpp"

#include "backend/Interpreter.hpp"
#include "backend/StackMachine.hpp"
#include <mutable/catalog/Type.hpp>
#include <mutable/IR/CNF.hpp>
#include <mutable/mutable.hpp>
#include <mutable/util/Diagnostic.hpp>
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "testutil.hpp"
#include <cmath>
#include <string>
#include <sstream>
#include <regex>


using namespace m;
using namespace m::ast;


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

            Diagnostic diag(true, std::cout, std::cerr);
            auto stmt = statement_from_string(diag, oss.str());
            M_insist(diag.num_errors() == 0);
            auto select = as<SelectStmt>(*stmt).select.get();
            auto expr = as<SelectClause>(select)->select[0].first.get();
            StackMachine eval(schema);
            eval.emit(*expr, 1);
            eval.emit_St_Tup(0, 0, expr->type());
            Tuple out({ expr->type() });
            Tuple *args[] = {&out, &in};
            eval(args);
            if (out.is_null(0)) {
                expr->dump();
                eval.dump();
                std::cerr << "Got NULL\n";
            }
            REQUIRE(not out.is_null(0));
            bool cond_res = cond(out[0]);
            if (not cond_res) {
                expr->dump();
                eval.dump();
                std::cerr << "Got " << out[0] << '\n';
            }
            REQUIRE(cond_res);
        }
    };

#define TEST(EXPR, NAME, COND) __test(EXPR, NAME, [&](Value RES) { return COND; })

    /* Constants */
    TEST("42", "constant/int64_t", RES == 42);
    TEST("13.37", "constant/double", RES == 13.37);
    TEST("TRUE", "constant/bool", RES == true);
    TEST("\"Hello, World!\"", "constant/char", streq(RES.as<char*>(), "Hello, World!"));
    TEST("d'1970-01-01'", "constant/date", RES == (1970 << 9) | (1 << 5) | 1);
    TEST("d'1970-01-01 01:00:00'", "constant/datetime", RES == 3600);

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

TEST_CASE("StackMachine/emit/CNF", "[core][backend]")
{
    auto check_emit_cnf = [](std::string case_name, std::string expr_str, bool expected_result)-> void {
        DYNAMIC_SECTION(case_name)
        {
            StackMachine SM;
            Tuple res({ Type::Get_Boolean(Type::TY_Scalar) });
            Tuple *args[] = { &res };

            std::ostringstream oss;
            oss << "SELECT " << expr_str << ";";

            Diagnostic diag(true, std::cout, std::cerr);
            auto stmt = statement_from_string(diag, oss.str());
            M_insist(diag.num_errors() == 0);
            auto select = as<SelectStmt>(*stmt).select.get();
            auto expr = as<SelectClause>(select)->select[0].first.get();
            auto cnf = cnf::to_CNF(*expr);

            SM.emit(cnf);
            SM.emit_St_Tup_b(0, 0);
            SM(args);

            REQUIRE(not res.is_null(0));
            CHECK(res[0] == expected_result);
        }
    };

    check_emit_cnf("Conjunction",                   "TRUE AND FALSE",                       false);
    check_emit_cnf("Disjunction",                   "TRUE OR FALSE",                        true);
    check_emit_cnf("Negation",                      "NOT TRUE",                             false);
    check_emit_cnf("Conjunction of disjunctions",   "(TRUE OR FALSE) AND (FALSE OR TRUE)",  true);
    check_emit_cnf("Negation of disjunction",       "NOT (TRUE OR FALSE)",                  false);
}

TEST_CASE("StackMachine/emit_Ld", "[core][backend]")
{
    SECTION("emit_Ld_i8")
    {
        Tuple tup({ Type::Get_Integer(Type::TY_Scalar, 1) });
        Tuple *args[] = { &tup };

        int8_t i = 42;
        StackMachine SM;
        SM.add_and_emit_load(&i);
        SM.emit_Ld( Type::Get_Integer(Type::TY_Scalar, 1) );
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not tup.is_null(0));
        CHECK(tup[0].as_i() == 42);
    }

    SECTION("emit_Ld_i16")
    {
        Tuple tup({ Type::Get_Integer(Type::TY_Scalar, 2) });
        Tuple *args[] = { &tup };

        int16_t i = 42;
        StackMachine SM;
        SM.add_and_emit_load(&i);
        SM.emit_Ld( Type::Get_Integer(Type::TY_Scalar, 2) );
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not tup.is_null(0));
        CHECK(tup[0].as_i() == 42);
    }

    SECTION("emit_Ld_i32")
    {
        Tuple tup({ Type::Get_Integer(Type::TY_Scalar, 4) });
        Tuple *args[] = { &tup };

        int32_t i = 1337;
        StackMachine SM;
        SM.add_and_emit_load(&i);
        SM.emit_Ld( Type::Get_Integer(Type::TY_Scalar, 4) );
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not tup.is_null(0));
        CHECK(tup[0].as_i() == 1337);
    }

    SECTION("emit_Ld_i64")
    {
        Tuple tup({ Type::Get_Integer(Type::TY_Scalar, 8) });
        Tuple *args[] = { &tup };

        int64_t i = 1337L;
        StackMachine SM;
        SM.add_and_emit_load(&i);
        SM.emit_Ld( Type::Get_Integer(Type::TY_Scalar, 8) );
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not tup.is_null(0));
        CHECK(tup[0].as_i() == 1337L);
    }

    SECTION("emit_Ld_f")
    {
        Tuple tup({ Type::Get_Float(Type::TY_Scalar) });
        Tuple *args[] = { &tup };

        float f = 42.0;
        StackMachine SM;
        SM.add_and_emit_load(&f);
        SM.emit_Ld( Type::Get_Float(Type::TY_Scalar) );
        SM.emit_St_Tup_f(0, 0);
        SM(args);
        REQUIRE(not tup.is_null(0));
        CHECK(tup[0].as_f() == 42.0);
    }

    SECTION("emit_Ld_d")
    {
        Tuple tup({ Type::Get_Double(Type::TY_Scalar) });
        Tuple *args[] = { &tup };

        double d = 42.0;
        StackMachine SM;
        SM.add_and_emit_load(&d);
        SM.emit_Ld( Type::Get_Double(Type::TY_Scalar) );
        SM.emit_St_Tup_d(0, 0);
        SM(args);
        REQUIRE(not tup.is_null(0));
        CHECK(tup[0].as_d() == 42.0);
    }

    SECTION("emit_Ld_s")
    {
        StackMachine SM;
        std::ostringstream oss;
        auto idx = SM.add(&oss);

        std::string str = "sql";
        SM.add_and_emit_load(str.c_str());
        SM.emit_Ld( Type::Get_Char(Type::TY_Scalar, 3) );
        SM.emit_Print_s(idx);
        SM(nullptr);
        CHECK(oss.str() == "\"sql\"");
    }
}

TEST_CASE("StackMachine/emit_St", "[core][backend]")
{
    SECTION("emit_St_i8")
    {
        int8_t i = 0;
        int8_t i2 = 42;
        StackMachine SM;
        SM.add_and_emit_load(&i);
        SM.add_and_emit_load(i2);
        SM.emit_St( Type::Get_Integer(Type::TY_Scalar, 1) );
        SM(nullptr);
        CHECK(i == 42);
    }

    SECTION("emit_St_i16")
    {
        int16_t i = 0;
        int16_t i2 = 42;
        StackMachine SM;
        SM.add_and_emit_load(&i);
        SM.add_and_emit_load(i2);
        SM.emit_St( Type::Get_Integer(Type::TY_Scalar, 2) );
        SM(nullptr);
        CHECK(i == 42);
    }

    SECTION("emit_St_i32")
    {
        int32_t i = 0;
        StackMachine SM;
        SM.add_and_emit_load(&i);
        SM.add_and_emit_load(1337);
        SM.emit_St( Type::Get_Integer(Type::TY_Scalar, 4) );
        SM(nullptr);
        CHECK(i == 1337);
    }

    SECTION("emit_St_i64")
    {
        int64_t i = 0;
        StackMachine SM;
        SM.add_and_emit_load(&i);
        SM.add_and_emit_load(1337L);
        SM.emit_St( Type::Get_Integer(Type::TY_Scalar, 8) );
        SM(nullptr);
        CHECK(i == 1337L);
    }

    SECTION("emit_St_f")
    {
        float f1 = 0.0;
        float f2 = 42.0;
        StackMachine SM;
        SM.add_and_emit_load(&f1);
        SM.add_and_emit_load(f2);
        SM.emit_St( Type::Get_Float(Type::TY_Scalar) );
        SM(nullptr);
        CHECK(f1 == 42.0);
    }

    SECTION("emit_St_d")
    {
        double d1 = 0.0;
        double d2 = 42.0;
        StackMachine SM;
        SM.add_and_emit_load(&d1);
        SM.add_and_emit_load(d2);
        SM.emit_St( Type::Get_Double(Type::TY_Scalar) );
        SM(nullptr);
        CHECK(d1 == 42.0);
    }

    SECTION("emit_St_s")
    {
        StackMachine SM;
        std::string str = "abcd";
        SM.add_and_emit_load(str.c_str());
        SM.add_and_emit_load("test");
        SM.emit_St( Type::Get_Char(Type::TY_Scalar, 4) );
        SM(nullptr);
        CHECK(str == "test");
    }
}

TEST_CASE("StackMachine/emit_St_Tup/emit_St_Tup_Null", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };
    SM.emit_St_Tup(0, 0, Type::Get_None());
    SM(args);
    REQUIRE(res.is_null(0));
}

TEST_CASE("StackMachine/emit_Print", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Boolean(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    SECTION("Emit a print instruction with type none")
    {
        std::ostringstream oss;
        std::size_t idx = SM.add(&oss);
        SM.emit_Print(idx, Type::Get_None());
        SM(args);
        REQUIRE(!oss.str().compare("NULL"));
    }
}

TEST_CASE("StackMachine/emit_Cast", "[core][backend]")
{
    // res : (int, float, double, bool)
    Tuple res({
                Type::Get_Integer(Type::TY_Scalar, 4),
                Type::Get_Float(Type::TY_Scalar),
                Type::Get_Double(Type::TY_Scalar),
                Type::Get_Boolean(Type::TY_Scalar),
            });

    Tuple *args[] = { &res };
    StackMachine SM;

    SECTION("int -> int (nothing to be done)")
    {
        SM.add_and_emit_load(42);
        SM.emit_Cast(Type::Get_Integer(Type::TY_Scalar, 4), Type::Get_Integer(Type::TY_Scalar, 4));
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == 42);
    }

    SECTION("int -> decimal")
    {
        SM.add_and_emit_load(42);
        SM.emit_Cast(Type::Get_Decimal(Type::TY_Scalar, 32, 4), Type::Get_Integer(Type::TY_Scalar, 4));
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == 420000);
    }

    SECTION("int -> float")
    {
        SM.add_and_emit_load(42);
        SM.emit_Cast(Type::Get_Float(Type::TY_Scalar), Type::Get_Integer(Type::TY_Scalar, 4));
        SM.emit_St_Tup_f(0, 1);
        SM(args);
        REQUIRE(not res.is_null(1));
        REQUIRE(res[1] == 42.0F);
    }

    SECTION("int -> double")
    {
        SM.add_and_emit_load(42);
        SM.emit_Cast(Type::Get_Double(Type::TY_Scalar), Type::Get_Integer(Type::TY_Scalar, 4));
        SM.emit_St_Tup_d(0, 2);
        SM(args);
        REQUIRE(not res.is_null(2));
        REQUIRE(res[2] == 42.0);
    }

    SECTION("float -> int")
    {
        SM.add_and_emit_load(42.0F);
        SM.emit_Cast(Type::Get_Integer(Type::TY_Scalar, 4), Type::Get_Float(Type::TY_Scalar));
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == 42);
    }

    SECTION("float -> decimal")
    {
        SM.add_and_emit_load(42.0F);
        SM.emit_Cast(Type::Get_Decimal(Type::TY_Scalar, 32, 4), Type::Get_Float(Type::TY_Scalar));
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == 420000);
    }

    SECTION("float -> double")
    {
        SM.add_and_emit_load(42.0F);
        SM.emit_Cast(Type::Get_Double(Type::TY_Scalar), Type::Get_Float(Type::TY_Scalar));
        SM.emit_St_Tup_i(0, 2);
        SM(args);
        REQUIRE(not res.is_null(2));
        REQUIRE(res[2] == 42.0);
    }

    SECTION("double -> int")
    {
        SM.add_and_emit_load(42.0);
        SM.emit_Cast(Type::Get_Integer(Type::TY_Scalar, 4), Type::Get_Double(Type::TY_Scalar));
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == 42);
    }

    SECTION("double -> decimal")
    {
        SM.add_and_emit_load(42.0);
        SM.emit_Cast(Type::Get_Decimal(Type::TY_Scalar, 32, 4), Type::Get_Double(Type::TY_Scalar));
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == 420000);
    }

    SECTION("double -> float")
    {
        SM.add_and_emit_load(42.0);
        SM.emit_Cast(Type::Get_Float(Type::TY_Scalar), Type::Get_Double(Type::TY_Scalar));
        SM.emit_St_Tup_i(0, 1);
        SM(args);
        REQUIRE(not res.is_null(1));
        REQUIRE(res[1] == 42.0F);
    }

    SECTION("decimal -> int")
    {
        SM.add_and_emit_load(31410);
        SM.emit_Cast(Type::Get_Integer(Type::TY_Scalar, 4), Type::Get_Decimal(Type::TY_Scalar, 32, 4));
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == 3);
    }

    SECTION("decimal -> float")
    {
        SM.add_and_emit_load(31410);
        SM.emit_Cast(Type::Get_Float(Type::TY_Scalar), Type::Get_Decimal(Type::TY_Scalar, 32, 4));
        SM.emit_St_Tup_f(0, 1);
        SM(args);
        REQUIRE(not res.is_null(1));
        REQUIRE(res[1] == (float) 31410/10000);
    }

    SECTION("decimal -> double")
    {
        SM.add_and_emit_load(31410);
        SM.emit_Cast(Type::Get_Double(Type::TY_Scalar), Type::Get_Decimal(Type::TY_Scalar, 32, 4));
        SM.emit_St_Tup_f(0, 2);
        SM(args);
        REQUIRE(not res.is_null(2));
        REQUIRE(res[2] == (double) 31410/10000);
    }

    SECTION("decimal(10,2) -> decimal(10,1)")
    {
        SM.add_and_emit_load(314);
        SM.emit_Cast(Type::Get_Decimal(Type::TY_Scalar, 10, 1), Type::Get_Decimal(Type::TY_Scalar, 10, 2));
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == 31);
    }

    SECTION("decimal(10,2) -> decimal(10,2)")
    {
        SM.add_and_emit_load(314);
        SM.emit_Cast(Type::Get_Decimal(Type::TY_Scalar, 10, 2), Type::Get_Decimal(Type::TY_Scalar, 10, 2));
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == 314);
    }

    SECTION("decimal(10,2) -> decimal(10,3)")
    {
        SM.add_and_emit_load(314);
        SM.emit_Cast(Type::Get_Decimal(Type::TY_Scalar, 10, 3), Type::Get_Decimal(Type::TY_Scalar, 10, 2));
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == 3140);
    }

    SECTION("CharacterSequence -> CharacterSequence (nothing to be done)")
    {
        SM.add_and_emit_load(42);
        SM.emit_Cast(Type::Get_Char(Type::TY_Scalar, 4), Type::Get_Char(Type::TY_Scalar, 4));
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == 42);
    }

    SECTION("bool -> int")
    {
        SM.add_and_emit_load(true);
        SM.emit_Cast(Type::Get_Integer(Type::TY_Scalar, 4), Type::Get_Boolean(Type::TY_Scalar));
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] != 0);
    }
}

/*======================================================================================================================
 * Control flow operations
 *====================================================================================================================*/

TEST_CASE("StackMachine/ControlFlow/Stop_Z", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };

    SECTION("SM should stop")
    {
        SM.add_and_emit_load(0);
        SM.emit_Stop_Z();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(res.is_null(0));
    }

    SECTION("SM should not stop")
    {
        SM.add_and_emit_load(42);
        SM.emit_Stop_Z();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        CHECK(res[0] == 42);
    }
}

TEST_CASE("StackMachine/ControlFlow/Stop_NZ", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };

    SECTION("SM should stop")
    {
        SM.add_and_emit_load(42);
        SM.emit_Stop_NZ();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(res.is_null(0));
    }

    SECTION("SM should not stop")
    {
        SM.add_and_emit_load(0);
        SM.emit_Stop_NZ();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        CHECK(res[0] == 0);
    }
}

TEST_CASE("StackMachine/ControlFlow/Stop_False", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Boolean(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    SECTION("SM should stop")
    {
        SM.add_and_emit_load(false);
        SM.emit_Stop_False();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(res.is_null(0));
    }

    SECTION("SM should not stop")
    {
        SM.add_and_emit_load(true);
        SM.emit_Stop_False();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        CHECK(res[0] == true);
    }
}

TEST_CASE("StackMachine/ControlFlow/Stop_True", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Boolean(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    SECTION("SM should stop")
    {
        SM.add_and_emit_load(true);
        SM.emit_Stop_True();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(res.is_null(0));
    }

    SECTION("SM should not stop")
    {
        SM.add_and_emit_load(false);
        SM.emit_Stop_True();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        CHECK(res[0] == false);
    }
}

/*======================================================================================================================
 * Stack manipulation operations
 *====================================================================================================================*/

TEST_CASE("StackMachine/StackManipulation/Pop", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Boolean(Type::TY_Scalar), Type::Get_Boolean(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    SM.emit_Push_Null();
    SM.add_and_emit_load(42);
    SM.emit_Is_Null();
    SM.emit_St_Tup_b(0, 0);
    SM.emit_Pop();
    SM.emit_Is_Null();
    SM.emit_St_Tup_b(0, 1);
    SM(args);

    REQUIRE(not res.is_null(0));
    REQUIRE(res[0] == false);
    REQUIRE(not res.is_null(1));
    REQUIRE(res[1] == true);
}

TEST_CASE("StackMachine/StackManipulation/Dup", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4), Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };

    SM.add_and_emit_load(42);
    SM.emit_Dup();
    SM.emit_St_Tup_i(0, 0);
    SM.emit_Pop();
    SM.emit_St_Tup_i(0, 1);
    SM(args);

    REQUIRE(not res.is_null(0));
    REQUIRE(res[0] == 42);
    REQUIRE(not res.is_null(1));
    REQUIRE(res[1] == 42);
}

/*======================================================================================================================
 * Context Access Operations
 *====================================================================================================================*/

TEST_CASE("StackMachine/ContextAccess/Upd_Ctx", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };

    std::ostringstream oss1;
    std::ostringstream oss2;

    std::size_t idx = SM.add(&oss1);
    SM.add_and_emit_load("Stream 1");
    SM.emit_Print_s(idx);

    SM.add_and_emit_load(&oss2);
    SM.emit_Upd_Ctx(idx);
    SM.add_and_emit_load("Stream 2");
    SM.emit_Print_s(idx);

    SM(args);

    REQUIRE(!oss1.str().compare("\"Stream 1\""));
    REQUIRE(!oss2.str().compare("\"Stream 2\""));
}

/*======================================================================================================================
 * Tuple Access Operations
 *====================================================================================================================*/

TEST_CASE("StackMachine/TupleAccess/St_Tup_Null", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };

    SM.emit_Push_Null();
    SM.emit_St_Tup_Null(0, 0);
    SM(args);
    REQUIRE(res.is_null(0));
}

TEST_CASE("StackMachine/TupleAccess/St_Tup_s", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Char(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };

    SM.emit_Push_Null();
    SM.emit_St_Tup_s(0, 0, 4);
    SM(args);
    REQUIRE(res.is_null(0));
}

/*======================================================================================================================
 * I/O Operations
 *====================================================================================================================*/

TEST_CASE("StackMachine/IO/Putc", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };
    std::ostringstream oss;

    SECTION("Print the character 'c'")
    {
        char chr = 'c';
        std::size_t idx = SM.add(&oss);
        SM.emit_Putc(idx, chr);
        SM(args);
        REQUIRE(oss.str() == "c");
    }

    SECTION("Print the character ' '")
    {
        char chr = ' ';
        std::size_t idx = SM.add(&oss);
        SM.emit_Putc(idx, chr);
        SM(args);
        REQUIRE(oss.str() == " ");
    }
}

TEST_CASE("StackMachine/IO/Print_i", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };
    std::ostringstream oss;

    SECTION("Print the integer on top of stack (NULL)")
    {
        std::size_t idx = SM.add(&oss);
        SM.emit_Push_Null();
        SM.emit_Print_i(idx);
        SM(args);
        REQUIRE(oss.str() == "NULL");
    }

    SECTION("Print the integer on top of stack (42)")
    {
        std::size_t idx = SM.add(&oss);
        SM.add_and_emit_load(42);
        SM.emit_Print(idx, Type::Get_Integer(Type::TY_Scalar, 4));
        SM(args);
        REQUIRE(oss.str() == "42");
    }

    SECTION("Print the integer on top of stack (-42)")
    {
        std::size_t idx = SM.add(&oss);
        SM.add_and_emit_load(-42);
        SM.emit_Print_i(idx);
        SM(args);
        REQUIRE(oss.str() == "-42");
    }
}

TEST_CASE("StackMachine/IO/Print_f", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };
    std::ostringstream oss;
    using std::to_string;

    SECTION("Print the float on top of stack (NULL)")
    {
        std::size_t idx = SM.add(&oss);
        SM.emit_Push_Null();
        SM.emit_Print_f(idx);
        SM(args);
        REQUIRE("NULL" == oss.str());
    }

    SECTION("Print the float on top of stack (0.123456)")
    {
        std::size_t idx = SM.add(&oss);
        float f = 0.123456;
        SM.add_and_emit_load(f);
        SM.emit_Print(idx, Type::Get_Float(Type::TY_Scalar));
        SM(args);
        REQUIRE(to_string(f) == oss.str());
    }

    SECTION("Print the float on top of stack (-42.42)")
    {
        std::size_t idx = SM.add(&oss);
        float f = -42.42;
        SM.add_and_emit_load(f);
        SM.emit_Print_f(idx);
        SM(args);
        REQUIRE(to_string(f) == oss.str());
    }
}

TEST_CASE("StackMachine/IO/Print_d", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };
    std::ostringstream oss;
    using std::to_string;

    SECTION("Print the double on top of stack (NULL)")
    {
        std::size_t idx = SM.add(&oss);
        SM.emit_Push_Null();
        SM.emit_Print_d(idx);
        SM(args);
        REQUIRE("NULL" == oss.str());
    }

    SECTION("Print the double on top of stack (0.123456)")
    {
        std::size_t idx = SM.add(&oss);
        double d = 0.123456;
        SM.add_and_emit_load(d);
        SM.emit_Print(idx, Type::Get_Double(Type::TY_Scalar));
        SM(args);
        REQUIRE(to_string(d) == oss.str());
    }
}

TEST_CASE("StackMachine/IO/Print_s", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };
    std::ostringstream oss;

    SECTION("Print the string on top of stack (NULL)")
    {
        std::size_t idx = SM.add(&oss);
        SM.emit_Push_Null();
        SM.emit_Print_s(idx);
        SM(args);
        REQUIRE(oss.str() == "NULL");
    }

    SECTION("Print the string on top of stack (sql)")
    {
        std::size_t idx = SM.add(&oss);
        SM.add_and_emit_load("sql");
        SM.emit_Print_s(idx);
        SM(args);
        REQUIRE(oss.str() == "\"sql\"");
    }
}

TEST_CASE("StackMachine/IO/Print_b", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };
    std::ostringstream oss;

    SECTION("Print the boolean on top of stack (NULL)")
    {
        std::size_t idx = SM.add(&oss);
        SM.emit_Push_Null();
        SM.emit_Print_b(idx);
        SM(args);
        REQUIRE(oss.str() == "NULL");
    }

    SECTION("Print the boolean on top of stack (TRUE)")
    {
        std::size_t idx = SM.add(&oss);
        SM.add_and_emit_load(true);
        SM.emit_Print_b(idx);
        SM(args);
        REQUIRE(oss.str() == "TRUE");
    }

    SECTION("Print the boolean on top of stack (FALSE)")
    {
        std::size_t idx = SM.add(&oss);
        SM.add_and_emit_load(false);
        SM.emit_Print_b(idx);
        SM(args);
        REQUIRE(oss.str() == "FALSE");
    }
}

TEST_CASE("StackMachine/IO/Print_date", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Date(Type::TY_Scalar) });
    Tuple *args[] = { &res };
    std::ostringstream oss;

    SECTION("Print the date on top of stack (NULL)")
    {
        std::size_t idx = SM.add(&oss);
        SM.emit_Push_Null();
        SM.emit_Print_date(idx);
        SM(args);
        REQUIRE(oss.str() == "NULL");
    }

    SECTION("Print the date on top of stack (2042-11-17)")
    {
        std::size_t idx = SM.add(&oss);
        SM.add_and_emit_load(Interpreter::eval(Constant(Token(Position("pos"), "d'2042-11-17'", TK_DATE))));
        SM.emit_Print_date(idx);
        SM(args);
        REQUIRE(oss.str() == "2042-11-17");
    }

    SECTION("Print the date on top of stack (-2042-11-17)")
    {
        std::size_t idx = SM.add(&oss);
        SM.add_and_emit_load(Interpreter::eval(Constant(Token(Position("pos"), "d'-2042-11-17'", TK_DATE))));
        SM.emit_Print_date(idx);
        SM(args);
        REQUIRE(oss.str() == "-2042-11-17");
    }
}

TEST_CASE("StackMachine/IO/Print_datetime", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Date(Type::TY_Scalar) });
    Tuple *args[] = { &res };
    std::ostringstream oss;

    SECTION("Print the datetime on top of stack (NULL)")
    {
        std::size_t idx = SM.add(&oss);
        SM.emit_Push_Null();
        SM.emit_Print_datetime(idx);
        SM(args);
        REQUIRE(oss.str() == "NULL");
    }

    SECTION("Print the datetime on top of stack (2042-11-17 12:34:56)")
    {
        std::size_t idx = SM.add(&oss);
        SM.add_and_emit_load(Interpreter::eval(Constant(Token(Position("pos"), "d'2042-11-17 12:34:56'", TK_DATE_TIME))));
        SM.emit_Print_datetime(idx);
        SM(args);
        REQUIRE(oss.str() == "2042-11-17 12:34:56");
    }

    SECTION("Print the datetime on top of stack (-2042-11-17 12:34:56)")
    {
        std::size_t idx = SM.add(&oss);
        SM.add_and_emit_load(Interpreter::eval(Constant(Token(Position("pos"), "d'-2042-11-17 12:34:56'", TK_DATE_TIME))));
        SM.emit_Print_datetime(idx);
        SM(args);
        REQUIRE(oss.str() == "-2042-11-17 12:34:56");
    }
}

/*======================================================================================================================
 * Storage Access Operations
 *====================================================================================================================*/

/*----- Load from memory ---------------------------------------------------------------------------------------------*/

TEST_CASE("StackMachine/StorageAccess/Ld_i8", "[core][backend]")
{
    Tuple tup({ Type::Get_Integer(Type::TY_Scalar, 1) });
    Tuple *args[] = { &tup };

    int8_t i = 42;
    StackMachine SM;
    SM.add_and_emit_load(&i);
    SM.emit_Ld_i8();
    SM.emit_St_Tup_i(0, 0);
    SM(args);
    REQUIRE(not tup.is_null(0));
    CHECK(tup[0].as_i() == 42);
}

TEST_CASE("StackMachine/StorageAccess/Ld_i16", "[core][backend]")
{
    Tuple tup({ Type::Get_Integer(Type::TY_Scalar, 2) });
    Tuple *args[] = { &tup };

    int16_t i = 42;
    StackMachine SM;
    SM.add_and_emit_load(&i);
    SM.emit_Ld_i16();
    SM.emit_St_Tup_i(0, 0);
    SM(args);
    REQUIRE(not tup.is_null(0));
    CHECK(tup[0].as_i() == 42);
}

TEST_CASE("StackMachine/StorageAccess/Ld_i32", "[core][backend]")
{
    Tuple tup({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &tup };

    int32_t i = 1337;
    StackMachine SM;
    SM.add_and_emit_load(&i);
    SM.emit_Ld_i32();
    SM.emit_St_Tup_i(0, 0);
    SM(args);
    REQUIRE(not tup.is_null(0));
    CHECK(tup[0].as_i() == 1337);
}

TEST_CASE("StackMachine/StorageAccess/Ld_i64", "[core][backend]")
{
    Tuple tup({ Type::Get_Integer(Type::TY_Scalar, 8) });
    Tuple *args[] = { &tup };

    int64_t i = 1337L;
    StackMachine SM;
    SM.add_and_emit_load(&i);
    SM.emit_Ld_i64();
    SM.emit_St_Tup_i(0, 0);
    SM(args);
    REQUIRE(not tup.is_null(0));
    CHECK(tup[0].as_i() == 1337L);
}

TEST_CASE("StackMachine/StorageAccess/Ld_f", "[core][backend]")
{
    Tuple tup({ Type::Get_Float(Type::TY_Scalar) });
    Tuple *args[] = { &tup };

    float f = 42.0;
    StackMachine SM;
    SM.add_and_emit_load(&f);
    SM.emit_Ld_f();
    SM.emit_St_Tup_f(0, 0);
    SM(args);
    REQUIRE(not tup.is_null(0));
    CHECK(tup[0].as_f() == 42.0);
}

TEST_CASE("StackMachine/StorageAccess/Ld_d", "[core][backend]")
{
    Tuple tup({ Type::Get_Double(Type::TY_Scalar) });
    Tuple *args[] = { &tup };

    double d = 42.0;
    StackMachine SM;
    SM.add_and_emit_load(&d);
    SM.emit_Ld_d();
    SM.emit_St_Tup_d(0, 0);
    SM(args);
    REQUIRE(not tup.is_null(0));
    CHECK(tup[0].as_d() == 42.0);
}

TEST_CASE("StackMachine/StorageAccess/Ld_s", "[core][backend]")
{
    StackMachine SM;
    std::ostringstream oss;
    auto idx = SM.add(&oss);

    std::string str = "sql";
    SM.add_and_emit_load(str.c_str());
    SM.emit_Ld_s(3);
    SM.emit_Print_s(idx);
    SM(nullptr);
    CHECK(oss.str() == "\"sql\"");
}

TEST_CASE("StackMachine/StorageAccess/Ld_b", "[core][backend]")
{
    Tuple tup({ Type::Get_Boolean(Type::TY_Scalar) });
    Tuple *args[] = { &tup };

    bool b = true;
    StackMachine SM;
    SM.add_and_emit_load(&b);
    SM.emit_Ld_b(1);
    SM.emit_St_Tup_d(0, 0);
    SM(args);
    REQUIRE(not tup.is_null(0));
    CHECK(tup[0].as_b() == true);
}

/*----- Store to memory ----------------------------------------------------------------------------------------------*/

TEST_CASE("StackMachine/StorageAccess/St_i8", "[core][backend]")
{
    int8_t i = 0;
    int8_t i2 = 42;
    StackMachine SM;
    SM.add_and_emit_load(&i);
    SM.add_and_emit_load(i2);
    SM.emit_St( Type::Get_Integer(Type::TY_Scalar, 1) );
    SM(nullptr);
    CHECK(i == 42);
}

TEST_CASE("StackMachine/StorageAccess/St_i16", "[core][backend]")
{
    int16_t i = 0;
    int16_t i2 = 42;
    StackMachine SM;
    SM.add_and_emit_load(&i);
    SM.add_and_emit_load(i2);
    SM.emit_St( Type::Get_Integer(Type::TY_Scalar, 2) );
    SM(nullptr);
    CHECK(i == 42);
}

TEST_CASE("StackMachine/StorageAccess/St_i32", "[core][backend]")
{
    int32_t i = 0;
    StackMachine SM;
    SM.add_and_emit_load(&i);
    SM.add_and_emit_load(1337);
    SM.emit_St( Type::Get_Integer(Type::TY_Scalar, 4) );
    SM(nullptr);
    CHECK(i == 1337);
}

TEST_CASE("StackMachine/StorageAccess/St_i64", "[core][backend]")
{
    int64_t i = 0;
    StackMachine SM;
    SM.add_and_emit_load(&i);
    SM.add_and_emit_load(1337L);
    SM.emit_St( Type::Get_Integer(Type::TY_Scalar, 8) );
    SM(nullptr);
    CHECK(i == 1337L);
}

TEST_CASE("StackMachine/StorageAccess/St_f", "[core][backend]")
{
    float f1 = 0.0;
    float f2 = 42.0;
    StackMachine SM;
    SM.add_and_emit_load(&f1);
    SM.add_and_emit_load(f2);
    SM.emit_St( Type::Get_Float(Type::TY_Scalar) );
    SM(nullptr);
    CHECK(f1 == 42.0);
}

TEST_CASE("StackMachine/StorageAccess/St_d", "[core][backend]")
{
    double d1 = 0.0;
    double d2 = 42.0;
    StackMachine SM;
    SM.add_and_emit_load(&d1);
    SM.add_and_emit_load(d2);
    SM.emit_St( Type::Get_Double(Type::TY_Scalar) );
    SM(nullptr);
    CHECK(d1 == 42.0);
}

TEST_CASE("StackMachine/StorageAccess/St_s", "[core][backend]")
{
    StackMachine SM;
    std::string str = "abcd";
    SM.add_and_emit_load(str.c_str());
    SM.add_and_emit_load("test");
    SM.emit_St_s(4);
    SM(nullptr);
    CHECK(str == "test");
}

TEST_CASE("StackMachine/StorageAccess/St_b", "[core][backend]")
{
    bool b = false;
    StackMachine SM;
    SM.add_and_emit_load(&b);
    SM.add_and_emit_load(true);
    SM.emit_St_b(0);
    SM(nullptr);
    CHECK(b == true);
}

/*======================================================================================================================
 * Arithmetical operations
 *====================================================================================================================*/

TEST_CASE("StackMachine/Arithmetical/Inc", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };

    SECTION("Increment positive integer")
    {
        SM.add_and_emit_load(42);
        SM.emit_Inc();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 43);
    }

    SECTION("Increment negative integer")
    {
        SM.add_and_emit_load(-42);
        SM.emit_Inc();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == -41);
    }
}

TEST_CASE("StackMachine/Arithmetical/Dec", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };

    SECTION("Decrement positive integer")
    {
        SM.add_and_emit_load(42);
        SM.emit_Dec();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 41);
    }

    SECTION("Decrement negative integer")
    {
        SM.add_and_emit_load(-42);
        SM.emit_Dec();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == -43);
    }
}

TEST_CASE("StackMachine/Arithmetical/Minus_x", "[core][backend]")
{
    StackMachine SM;

    SECTION("Minus_i")
    {
        Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
        Tuple *args[] = { &res };

        SECTION("Negate positive integer")
        {
            SM.add_and_emit_load(42);
            SM.emit_Minus_i();
            SM.emit_St_Tup_i(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0].as_i() == -42);
        }

        SECTION("Negate negative integer")
        {
            SM.add_and_emit_load(-42);
            SM.emit_Minus_i();
            SM.emit_St_Tup_i(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0].as_i() == 42);
        }
    }

    SECTION("Minus_f")
    {
        Tuple res({ Type::Get_Float(Type::TY_Scalar) });
        Tuple *args[] = { &res };

        SECTION("Negate positive float")
        {
            float f = 4.2;
            SM.add_and_emit_load(f);
            SM.emit_Minus_f();
            SM.emit_St_Tup_f(0.0, 0.0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0].as_f() == -f);
        }

        SECTION("Decrement negative float")
        {
            float f = -4.2;
            SM.add_and_emit_load(f);
            SM.emit_Minus_f();
            SM.emit_St_Tup_f(0.0, 0.0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0].as_f() == -f);
        }
    }

    SECTION("Minus_d")
    {
        Tuple res({ Type::Get_Double(Type::TY_Scalar) });
        Tuple *args[] = { &res };

        SECTION("Negate positive double")
        {
            double d = 4.2;
            SM.add_and_emit_load(d);
            SM.emit_Minus_d();
            SM.emit_St_Tup_d(0.0, 0.0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0].as_d() == -d);
        }

        SECTION("Decrement negative double")
        {
            double d = -4.2;
            SM.add_and_emit_load(d);
            SM.emit_Minus_d();
            SM.emit_St_Tup_d(0.0, 0.0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0].as_d() == -d);
        }
    }
}

TEST_CASE("StackMachine/Arithmetical/Add_x", "[core][backend]")
{
    StackMachine SM;

    SECTION("Add_i")
    {
        Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
        Tuple *args[] = { &res };

        SM.add_and_emit_load(42);
        SM.add_and_emit_load(66);
        SM.emit_Add_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 108);
    }

    SECTION("Add_f")
    {
        Tuple res({ Type::Get_Float(Type::TY_Scalar) });
        Tuple *args[] = { &res };

        float f1 = 4.2f;
        float f2 = 3.141f;
        SM.add_and_emit_load(f1);
        SM.add_and_emit_load(f2);
        SM.emit_Add_f();
        SM.emit_St_Tup_f(0.0, 0.0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_f() == f1 + f2);
    }

    SECTION("Add_d")
    {
        Tuple res({ Type::Get_Double(Type::TY_Scalar) });
        Tuple *args[] = { &res };

        SECTION("Add two doubles")
        {
            const double d1 = 3.141;
            const double d2 = 5.639;
            SM.add_and_emit_load(d1);
            SM.add_and_emit_load(d2);
            SM.emit_Add_d();
            SM.emit_St_Tup_d(0.0, 0.0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0].as_d() == d1 + d2);
        }
    }
}

TEST_CASE("StackMachine/Arithmetical/Sub_x", "[core][backend]")
{
    StackMachine SM;

    SECTION("Sub_i")
    {
        Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
        Tuple *args[] = { &res };

        SM.add_and_emit_load(8989);
        SM.add_and_emit_load(555);
        SM.emit_Sub_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 8434);
    }

    SECTION("Sub_f")
    {
        Tuple res({ Type::Get_Float(Type::TY_Scalar) });
        Tuple *args[] = { &res };

        float f1 = 4.2f;
        float f2 = 3.141f;
        SM.add_and_emit_load(f1);
        SM.add_and_emit_load(f2);
        SM.emit_Sub_f();
        SM.emit_St_Tup_f(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_f() == f1 - f2);
    }

    SECTION("Sub_d")
    {
        Tuple res({ Type::Get_Double(Type::TY_Scalar) });
        Tuple *args[] = { &res };

        double d1 = 5.639;
        double d2 = 3.141;
        SM.add_and_emit_load(d1);
        SM.add_and_emit_load(d2);
        SM.emit_Sub_d();
        SM.emit_St_Tup_d(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_d() == d1 - d2);
    }
}

TEST_CASE("StackMachine/Arithmetical/Mul_x", "[core][backend]")
{
    StackMachine SM;

    SECTION("Mul_i")
    {
        Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
        Tuple *args[] = { &res };

        SM.add_and_emit_load(6);
        SM.add_and_emit_load(7);
        SM.emit_Mul_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 42);
    }

    SECTION("Mul_f")
    {
        Tuple res({ Type::Get_Float(Type::TY_Scalar) });
        Tuple *args[] = { &res };

        float f1 = 4.2f;
        float f2 = 3.141f;
        SM.add_and_emit_load(f1);
        SM.add_and_emit_load(f2);
        SM.emit_Mul_f();
        SM.emit_St_Tup_f(0.0, 0.0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_f() == f1 * f2);
    }

    SECTION("Mul_d")
    {
        Tuple res({ Type::Get_Double(Type::TY_Scalar) });
        Tuple *args[] = { &res };

        double d1 = 2.718;
        double d2 = 3.141;
        SM.add_and_emit_load(d1);
        SM.add_and_emit_load(d2);
        SM.emit_Mul_d();
        SM.emit_St_Tup_d(0.0, 0.0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_d() == d1 * d2);
    }
}

TEST_CASE("StackMachine/Arithmetical/Div_x", "[core][backend]")
{
    StackMachine SM;

    SECTION("Div_i")
    {
        Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
        Tuple *args[] = { &res };

        SM.add_and_emit_load(42);
        SM.add_and_emit_load(7);
        SM.emit_Div_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 6);
    }

    SECTION("Div_f")
    {
        Tuple res({ Type::Get_Float(Type::TY_Scalar) });
        Tuple *args[] = { &res };

        float f1 = 42.24f;
        float f2 = 6.f;
        SM.add_and_emit_load(f1);
        SM.add_and_emit_load(f2);
        SM.emit_Div_f();
        SM.emit_St_Tup_f(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_f() == f1 / f2);
    }

    SECTION("Div_d")
    {
        Tuple res({ Type::Get_Double(Type::TY_Scalar) });
        Tuple *args[] = { &res };

        double d1 = 3.141;
        double d2 = 0.5;
        SM.add_and_emit_load(d1);
        SM.add_and_emit_load(d2);
        SM.emit_Div_d();
        SM.emit_St_Tup_d(0.0, 0.0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_d() == d1 / d2);
    }
}

TEST_CASE("StackMachine/Arithmetical/Mod_i", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };

    SECTION("Two integers modulo without rest")
    {
        SM.add_and_emit_load(42);
        SM.add_and_emit_load(7);
        SM.emit_Mod_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 0);
    }

    SECTION("Two integers modulo with rest")
    {
        SM.add_and_emit_load(42);
        SM.add_and_emit_load(10);
        SM.emit_Mod_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 2);
    }
}

TEST_CASE("StackMachine/Arithmetical/Cat_s", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };
    std::ostringstream oss;

    SECTION("Concatenate two strings")
    {
        int idx = SM.add(&oss);
        SM.add_and_emit_load("abc");
        SM.add_and_emit_load("sql");
        SM.emit_Cat_s();
        SM.emit_Print_s(idx);
        SM(args);
        REQUIRE(!oss.str().compare("\"abcsql\""));
    }

    SECTION("Concatenate empty string to a non empty string")
    {
        int idx = SM.add(&oss);
        SM.add_and_emit_load("This is a test string!");
        SM.add_and_emit_load("");
        SM.emit_Cat_s();
        SM.emit_Print_s(idx);
        SM(args);
        REQUIRE(!oss.str().compare("\"This is a test string!\""));
    }

    SECTION("rhs is NULL")
    {
        int idx = SM.add(&oss);
        SM.add_and_emit_load("This is a test string!");
        SM.emit_Push_Null();
        SM.emit_Cat_s();
        SM.emit_Print_s(idx);
        SM(args);
        REQUIRE(!oss.str().compare("\"This is a test string!\""));
    }

        SECTION("lhs is NULL")
    {
        int idx = SM.add(&oss);
        SM.emit_Push_Null();
        SM.add_and_emit_load("This is a test string!");
        SM.emit_Cat_s();
        SM.emit_Print_s(idx);
        SM(args);
        REQUIRE(!oss.str().compare("\"This is a test string!\""));
    }
}

/*======================================================================================================================
 * Bitwise operations
 *====================================================================================================================*/

TEST_CASE("StackMachine/Bitwise/Neg_i", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };

    SECTION("Bitwise negate 42")
    {
        SM.add_and_emit_load(42);
        SM.emit_Neg_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == -43);
    }

    SECTION("Bitwise negate 0")
    {
        SM.add_and_emit_load(0);
        SM.emit_Neg_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == -1);
    }

    SECTION("Bitwise negate -1")
    {
        SM.add_and_emit_load(-1);
        SM.emit_Neg_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 0);
    }
}

TEST_CASE("StackMachine/Bitwise/And_i", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };

    SECTION("Bitwise and two numbers where one is zero")
    {
        SM.add_and_emit_load(42);
        SM.add_and_emit_load(0);
        SM.emit_And_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 0);
    }

    SECTION("Bitwise and a number and its bit-negation")
    {
        SM.add_and_emit_load(42);
        SM.add_and_emit_load(~42);
        SM.emit_And_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 0);
    }

    SECTION("Bitwise and a number with itself")
    {
        SM.add_and_emit_load(42);
        SM.add_and_emit_load(42);
        SM.emit_And_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 42);
    }

    SECTION("Bitwise and two numbers where one is only ones")
    {
        SM.add_and_emit_load(42);
        SM.add_and_emit_load(~0);
        SM.emit_And_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 42);
    }
}

TEST_CASE("StackMachine/Bitwise/Or_i", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };

    SECTION("Bitwise or two numbers where one is zero")
    {
        SM.add_and_emit_load(42);
        SM.add_and_emit_load(0);
        SM.emit_Or_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 42);
    }

    SECTION("Bitwise or a number and its bit-negation")
    {
        SM.add_and_emit_load(42);
        SM.add_and_emit_load(~42);
        SM.emit_Or_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == ~0);
    }

    SECTION("Bitwise or a number with itself")
    {
        SM.add_and_emit_load(42);
        SM.add_and_emit_load(42);
        SM.emit_Or_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 42);
    }

    SECTION("Bitwise or two numbers where one is only ones")
    {
        SM.add_and_emit_load(42);
        SM.add_and_emit_load(~0);
        SM.emit_Or_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == ~0);
    }
}

TEST_CASE("StackMachine/Bitwise/Xor_i", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };

    SECTION("Bitwise xor two numbers where one is zero")
    {
        SM.add_and_emit_load(42);
        SM.add_and_emit_load(0);
        SM.emit_Xor_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 42);
    }

    SECTION("Bitwise xor a number and its bit-negation")
    {
        SM.add_and_emit_load(42);
        SM.add_and_emit_load(~42);
        SM.emit_Xor_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == ~0);
    }

    SECTION("Bitwise xor a number with itself")
    {
        SM.add_and_emit_load(42);
        SM.add_and_emit_load(42);
        SM.emit_Xor_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 0);
    }

    SECTION("Bitwise xor two numbers where one is only ones")
    {
        SM.add_and_emit_load(42);
        SM.add_and_emit_load(~0);
        SM.emit_Xor_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == ~42);
    }
}

TEST_CASE("StackMachine/Bitwise/ShL_i", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };

    SM.add_and_emit_load(1);
    SM.add_and_emit_load(10);
    SM.emit_ShL_i();
    SM.emit_St_Tup_i(0, 0);
    SM(args);
    REQUIRE(not res.is_null(0));
    REQUIRE(res[0].as_i() == 1024);
}

TEST_CASE("StackMachine/Bitwise/ShLi_i", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };

    SM.add_and_emit_load(1);
    SM.emit_ShLi_i(10);
    SM.emit_St_Tup_i(0, 0);
    SM(args);
    REQUIRE(not res.is_null(0));
    REQUIRE(res[0].as_i() == 1024);
}

TEST_CASE("StackMachine/Bitwise/SARi_i", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };

    SECTION("Shift right arithmetically 1024 by 10")
    {
        SM.add_and_emit_load(1024);
        SM.emit_SARi_i(10);
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 1);
    }

    SECTION("Shift right arithmetically -65536 by 16")
    {
        SM.add_and_emit_load(-65536);
        SM.emit_SARi_i(16);
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == -1);
    }
}


/*======================================================================================================================
 * Logical operations
 *====================================================================================================================*/

TEST_CASE("StackMachine/Locigal/Not_b", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Boolean(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    SECTION("Not true")
    {
        SM.add_and_emit_load(true);
        SM.emit_Not_b();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == false);
    }

    SECTION("Not false")
    {
        SM.add_and_emit_load(false);
        SM.emit_Not_b();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == true);
    }
}

TEST_CASE("StackMachine/Locigal/And_b", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Boolean(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    SECTION("false and false")
    {
        SM.add_and_emit_load(false);
        SM.add_and_emit_load(false);
        SM.emit_And_b();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == false);
    }

    SECTION("false and true")
    {
        SM.add_and_emit_load(false);
        SM.add_and_emit_load(true);
        SM.emit_And_b();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == false);
    }

    SECTION("true and true")
    {
        SM.add_and_emit_load(true);
        SM.add_and_emit_load(true);
        SM.emit_And_b();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == true);
    }
}

TEST_CASE("StackMachine/Locigal/Or_b", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Boolean(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    SECTION("false or false")
    {
        SM.add_and_emit_load(false);
        SM.add_and_emit_load(false);
        SM.emit_Or_b();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == false);
    }

    SECTION("false or true")
    {
        SM.add_and_emit_load(false);
        SM.add_and_emit_load(true);
        SM.emit_Or_b();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == true);
    }

    SECTION("true or true")
    {
        SM.add_and_emit_load(true);
        SM.add_and_emit_load(true);
        SM.emit_Or_b();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == true);
    }
}

/*======================================================================================================================
 * Comparison operations
 *====================================================================================================================*/

TEST_CASE("StackMachine/Comparison/EqZ_i", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Boolean(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    SECTION("Equals zero")
    {
        SM.add_and_emit_load(0);
        SM.emit_EqZ_i();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == true);
    }

    SECTION("Does not equal zero")
    {
        SM.add_and_emit_load(42);
        SM.emit_EqZ_i();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == false);
    }
}

TEST_CASE("StackMachine/Comparison/NEZ_i", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Boolean(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    SECTION("Equals zero")
    {
        SM.add_and_emit_load(0);
        SM.emit_NEZ_i();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == false);
    }

    SECTION("Does not equal zero")
    {
        SM.add_and_emit_load(42);
        SM.emit_NEZ_i();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == true);
    }
}

TEST_CASE("StackMachine/Comparison/Eq_x", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Boolean(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    SECTION("Eq_i")
    {
        SECTION("Arguments are equal")
        {
            SM.add_and_emit_load(42);
            SM.add_and_emit_load(42);
            SM.emit_Eq_i();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("Arguments are not equal")
        {
            SM.add_and_emit_load(42);
            SM.add_and_emit_load(35);
            SM.emit_Eq_i();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }

    SECTION("Eq_f")
    {
        SECTION("Arguments are equal")
        {
            float f1 = 42.42;
            SM.add_and_emit_load(f1);
            SM.add_and_emit_load(f1);
            SM.emit_Eq_f();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("Arguments are not equal")
        {
            float f1 = 42.42;
            float f2 = 35.35;
            SM.add_and_emit_load(f1);
            SM.add_and_emit_load(f2);
            SM.emit_Eq_f();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }

    SECTION("Eq_d")
    {
        SECTION("Arguments are equal")
        {
            double d1 = 42.42;
            SM.add_and_emit_load(d1);
            SM.add_and_emit_load(d1);
            SM.emit_Eq_d();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("Arguments are not equal")
        {
            double d1 = 42.42;
            double d2 = 35.35;
            SM.add_and_emit_load(d1);
            SM.add_and_emit_load(d2);
            SM.emit_Eq_d();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }

    SECTION("Eq_b")
    {
        SECTION("Arguments are equal")
        {
            SM.add_and_emit_load(true);
            SM.add_and_emit_load(true);
            SM.emit_Eq_b();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("Arguments are not equal")
        {
            SM.add_and_emit_load(true);
            SM.add_and_emit_load(false);
            SM.emit_Eq_b();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }

    SECTION("Eq_s")
    {
        SECTION("Arguments are equal")
        {
            SM.add_and_emit_load("sql");
            SM.add_and_emit_load("sql");
            SM.emit_Eq_s();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("Arguments are not equal")
        {
            SM.add_and_emit_load("sql");
            SM.add_and_emit_load("abcd");
            SM.emit_Eq_s();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }
}

TEST_CASE("StackMachine/Comparison/NE_x", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Boolean(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    SECTION("NE_i")
    {
        SECTION("Arguments are equal")
        {
            SM.add_and_emit_load(42);
            SM.add_and_emit_load(42);
            SM.emit_NE_i();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }

        SECTION("Arguments are not equal")
        {
            SM.add_and_emit_load(42);
            SM.add_and_emit_load(35);
            SM.emit_NE_i();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }
    }

    SECTION("NE_f")
    {
        SECTION("Arguments are equal")
        {
            float f1 = 42.42;
            SM.add_and_emit_load(f1);
            SM.add_and_emit_load(f1);
            SM.emit_NE_f();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }

        SECTION("Arguments are not equal")
        {
            float f1 = 42.42;
            float f2 = 35.35;
            SM.add_and_emit_load(f1);
            SM.add_and_emit_load(f2);
            SM.emit_NE_f();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }
    }

    SECTION("NE_d")
    {
        SECTION("Arguments are equal")
        {
            double d1 = 42.42;
            SM.add_and_emit_load(d1);
            SM.add_and_emit_load(d1);
            SM.emit_NE_d();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }

        SECTION("Arguments are not equal")
        {
            double d1 = 42.42;
            double d2 = 35.35;
            SM.add_and_emit_load(d1);
            SM.add_and_emit_load(d2);
            SM.emit_NE_d();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }
    }

    SECTION("NE_b")
    {
        SECTION("Arguments are equal")
        {
            SM.add_and_emit_load(true);
            SM.add_and_emit_load(true);
            SM.emit_NE_b();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }

        SECTION("Arguments are not equal")
        {
            SM.add_and_emit_load(true);
            SM.add_and_emit_load(false);
            SM.emit_NE_b();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }
    }

    SECTION("NE_s")
    {
        SECTION("Arguments are equal")
        {
            SM.add_and_emit_load("sql");
            SM.add_and_emit_load("sql");
            SM.emit_NE_s();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }

        SECTION("Arguments are not equal")
        {
            SM.add_and_emit_load("sql");
            SM.add_and_emit_load("abcd");
            SM.emit_NE_s();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }
    }
}

TEST_CASE("StackMachine/Comparison/LT_x", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Boolean(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    SECTION("LT_i")
    {
        SECTION("LT_i is true")
        {
            SM.add_and_emit_load(42);
            SM.add_and_emit_load(149);
            SM.emit_LT_i();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("LT_i is false")
        {
            SM.add_and_emit_load(42);
            SM.add_and_emit_load(35);
            SM.emit_LT_i();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }

    SECTION("LT_f")
    {
        SECTION("LT_f is true")
        {
            float f1 = 12.524;
            float f2 = 42.42;
            SM.add_and_emit_load(f1);
            SM.add_and_emit_load(f2);
            SM.emit_LT_f();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("LT_f is false")
        {
            float f1 = 42.42;
            float f2 = 35.35;
            SM.add_and_emit_load(f1);
            SM.add_and_emit_load(f2);
            SM.emit_LT_f();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }

    SECTION("LT_d")
    {
        SECTION("LT_d is true")
        {
            double d1 = 12.524;
            double d2 = 42.42;
            SM.add_and_emit_load(d1);
            SM.add_and_emit_load(d2);
            SM.emit_LT_d();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("LT_d is false")
        {
            double d1 = 42.42;
            double d2 = 35.35;
            SM.add_and_emit_load(d1);
            SM.add_and_emit_load(d2);
            SM.emit_LT_d();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }

    SECTION("LT_s")
    {
        SECTION("LT_s is true")
        {
            SM.add_and_emit_load("sql");
            SM.add_and_emit_load("xyzz");
            SM.emit_LT_s();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("LT_s is false")
        {
            SM.add_and_emit_load("sql");
            SM.add_and_emit_load("abcd");
            SM.emit_LT_s();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }
}

TEST_CASE("StackMachine/Comparison/GT_x", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Boolean(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    SECTION("GT_i")
    {
        SECTION("GT_i is true")
        {
            SM.add_and_emit_load(149);
            SM.add_and_emit_load(42);
            SM.emit_GT_i();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("GT_i is false")
        {
            SM.add_and_emit_load(35);
            SM.add_and_emit_load(42);
            SM.emit_GT_i();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }

    SECTION("GT_f")
    {
        SECTION("GT_f is true")
        {
            float f1 = 42.42;
            float f2 = 12.524;
            SM.add_and_emit_load(f1);
            SM.add_and_emit_load(f2);
            SM.emit_GT_f();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("GT_f is false")
        {
            float f1 = 35.35;
            float f2 = 42.42;
            SM.add_and_emit_load(f1);
            SM.add_and_emit_load(f2);
            SM.emit_GT_f();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }

    SECTION("GT_d")
    {
        SECTION("GT_d is true")
        {
            double d1 = 42.42;
            double d2 = 12.524;
            SM.add_and_emit_load(d1);
            SM.add_and_emit_load(d2);
            SM.emit_GT_d();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("GT_d is false")
        {
            double d1 = 35.35;
            double d2 = 42.42;
            SM.add_and_emit_load(d1);
            SM.add_and_emit_load(d2);
            SM.emit_GT_d();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }

    SECTION("GT_s")
    {
        SECTION("GT_s is true")
        {
            SM.add_and_emit_load("xyzz");
            SM.add_and_emit_load("sql");
            SM.emit_GT_s();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("GT_s is false")
        {
            SM.add_and_emit_load("abcd");
            SM.add_and_emit_load("sql");
            SM.emit_GT_s();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }
}

TEST_CASE("StackMachine/Comparison/LE_x", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Boolean(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    SECTION("LE_i")
    {
        SECTION("LE is true")
        {
            SM.add_and_emit_load(42);
            SM.add_and_emit_load(149);
            SM.emit_LE_i();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("LE is false")
        {
            SM.add_and_emit_load(42);
            SM.add_and_emit_load(35);
            SM.emit_LE_i();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }

    SECTION("LE_f")
    {
        SECTION("LE is true")
        {
            float f1 = 42.42;
            SM.add_and_emit_load(f1);
            SM.add_and_emit_load(f1);
            SM.emit_LE_f();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("LE is false")
        {
            float f1 = 42.42;
            float f2 = 35.35;
            SM.add_and_emit_load(f1);
            SM.add_and_emit_load(f2);
            SM.emit_LE_f();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }

    SECTION("LE_d")
    {
        SECTION("LE is true")
        {
            double d1 = 42.42;
            SM.add_and_emit_load(d1);
            SM.add_and_emit_load(d1);
            SM.emit_LE_d();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("LE is false")
        {
            double d1 = 42.42;
            double d2 = 35.35;
            SM.add_and_emit_load(d1);
            SM.add_and_emit_load(d2);
            SM.emit_LE_d();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }

    SECTION("LE_s")
    {
        SECTION("LE is true")
        {
            SM.add_and_emit_load("sql");
            SM.add_and_emit_load("sql");
            SM.emit_LE_s();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("LE is false")
        {
            SM.add_and_emit_load("sql");
            SM.add_and_emit_load("abcd");
            SM.emit_LE_s();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }
}

TEST_CASE("StackMachine/Comparison/GE_x", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Boolean(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    SECTION("GE_i")
    {
        SECTION("GE_i is true")
        {
            SM.add_and_emit_load(149);
            SM.add_and_emit_load(42);
            SM.emit_GE_i();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("GE_i is false")
        {
            SM.add_and_emit_load(35);
            SM.add_and_emit_load(42);
            SM.emit_GE_i();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }

    SECTION("GE_f")
    {
        SECTION("GE_f is true")
        {
            float f1 = 42.42;
            SM.add_and_emit_load(f1);
            SM.add_and_emit_load(f1);
            SM.emit_GE_f();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("GE_f is false")
        {
            float f1 = 35.35;
            float f2 = 42.42;
            SM.add_and_emit_load(f1);
            SM.add_and_emit_load(f2);
            SM.emit_GE_f();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }

    SECTION("GE_d")
    {
        SECTION("GE_d is true")
        {
            double d1 = 42.42;
            SM.add_and_emit_load(d1);
            SM.add_and_emit_load(d1);
            SM.emit_GE_d();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("GE_d is false")
        {
            double d1 = 35.35;
            double d2 = 42.42;
            SM.add_and_emit_load(d1);
            SM.add_and_emit_load(d2);
            SM.emit_GE_d();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }

    SECTION("GE_s")
    {
        SECTION("GE_s is true")
        {
            SM.add_and_emit_load("sql");
            SM.add_and_emit_load("sql");
            SM.emit_GE_s();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == true);
        }

        SECTION("GE_s is false")
        {
            SM.add_and_emit_load("abcd");
            SM.add_and_emit_load("sql");
            SM.emit_GE_s();
            SM.emit_St_Tup_b(0, 0);
            SM(args);
            REQUIRE(not res.is_null(0));
            REQUIRE(res[0] == false);
        }
    }
}

TEST_CASE("StackMachine/Comparison/Cmp_i", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 8) });
    Tuple *args[] = { &res };

    SECTION("Compare two unequal ints")
    {
        SM.add_and_emit_load(42);
        SM.add_and_emit_load(-42);
        SM.emit_Cmp_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() > 0);
    }

    SECTION("Compare two equal ints")
    {
        SM.add_and_emit_load(42);
        SM.add_and_emit_load(42);
        SM.emit_Cmp_i();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 0);
    }
}

TEST_CASE("StackMachine/Comparison/Cmp_f", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 8) });
    Tuple *args[] = { &res };

    SECTION("Compare two unequal floats")
    {
        float f1 = 42.0;
        float f2 = -42.0;
        SM.add_and_emit_load(f1);
        SM.add_and_emit_load(f2);
        SM.emit_Cmp_f();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() > 0);
    }

    SECTION("Compare two equal floats")
    {
        float f1 = 42.0;
        float f2 = 42.0;
        SM.add_and_emit_load(f1);
        SM.add_and_emit_load(f2);
        SM.emit_Cmp_f();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 0);
    }
}

TEST_CASE("StackMachine/Comparison/Cmp_d", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 8) });
    Tuple *args[] = { &res };

    SECTION("Compare two unequal doubles")
    {
        double d1 = 42.0;
        double d2 = -42.0;
        SM.add_and_emit_load(d1);
        SM.add_and_emit_load(d2);
        SM.emit_Cmp_d();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() > 0);
    }

    SECTION("Compare two equal doubles")
    {
        double d1 = 42.0;
        double d2 = 42.0;
        SM.add_and_emit_load(d1);
        SM.add_and_emit_load(d2);
        SM.emit_Cmp_d();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 0);
    }
}

TEST_CASE("StackMachine/Comparison/Cmp_s", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 8) });
    Tuple *args[] = { &res };

    SECTION("Compare two unequal strings/char-arrays")
    {
        SM.add_and_emit_load("sql");
        SM.add_and_emit_load("abc");
        SM.emit_Cmp_s();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() > 0);
    }

    SECTION("Compare two equal strings/char-arrays")
    {
        SM.add_and_emit_load("sql");
        SM.add_and_emit_load("sql");
        SM.emit_Cmp_s();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 0);
    }

    SECTION("Compare two empty strings")
    {
        SM.add_and_emit_load("");
        SM.add_and_emit_load("");
        SM.emit_Cmp_s();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 0);
    }
}

TEST_CASE("StackMachine/Comparison/Cmp_b", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Boolean(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    SECTION("Compare two unequal bools")
    {
        SM.add_and_emit_load(true);
        SM.add_and_emit_load(false);
        SM.emit_Cmp_b();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() > 0);
    }

     SECTION("Compare two equal bools")
    {
        SM.add_and_emit_load(true);
        SM.add_and_emit_load(true);
        SM.emit_Cmp_b();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_i() == 0);
    }
}

TEST_CASE("StackMachine/Comparison/Like_expr", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Boolean(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    SECTION("Pattern is null")
    {
        SM.add_and_emit_load("sql");
        SM.emit_Push_Null();
        SM.emit_Like_expr();
        SM.emit_Is_Null();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_b() == true);
    }

    SECTION("String is null")
    {
        SM.emit_Push_Null();
        SM.add_and_emit_load("sql");
        SM.emit_Like_expr();
        SM.emit_Is_Null();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_b() == true);
    }

    SECTION("A match exists")
    {
        SM.add_and_emit_load("abc|sql|");
        SM.add_and_emit_load("%sql_");
        SM.emit_Like_expr();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_b() == true);
    }

    SECTION("A match does not exist")
    {
        SM.add_and_emit_load("abc|sql");
        SM.add_and_emit_load("%sql_");
        SM.emit_Like_expr();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_b() == false);
    }

    SECTION("Escaped wildcard match")
    {
        SM.add_and_emit_load("abc|sql_");
        SM.add_and_emit_load("%sql\\_");
        SM.emit_Like_expr();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_b() == true);
    }

    SECTION("Escaped wildcard no match")
    {
        SM.add_and_emit_load("abc|sql|");
        SM.add_and_emit_load("%sql\\_");
        SM.emit_Like_expr();
        SM.emit_St_Tup_b(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0].as_b() == false);
    }
}

/*======================================================================================================================
 * Selection operation
 *====================================================================================================================*/

TEST_CASE("StackMachine/Sel", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };

    SECTION("Select first")
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

    SECTION("Select second")
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

    SECTION("Select with NULL condition")
    {
        SM.emit_Push_Null();
        SM.add_and_emit_load(42);
        SM.add_and_emit_load(13);
        SM.emit_Sel();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(res.is_null(0));
    }

    SECTION("Select first that is NULL")
    {
        SM.add_and_emit_load(true);
        SM.emit_Push_Null();
        SM.add_and_emit_load(13);
        SM.emit_Sel();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(res.is_null(0));
    }

    SECTION("Select second that is NULL")
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

/*======================================================================================================================
 * Intrinsic functions
 *====================================================================================================================*/

TEST_CASE("StackMachine/Intrinsic/Is_Null", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 4) });
    Tuple *args[] = { &res };

    SECTION("Top is null")
    {
        SM.emit_Push_Null();
        SM.emit_Is_Null();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == true);
    }

    SECTION("Top is not null")
    {
        SM.add_and_emit_load(42);
        SM.emit_Is_Null();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == false);
    }
}

TEST_CASE("StackMachine/Intrinsic/Cast_i_f", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 8) });
    Tuple *args[] = { &res };

    float f = 42.42;
    int i = (int) f;
    SM.add_and_emit_load(f);
    SM.emit_Cast(Type::Get_Integer(Type::TY_Scalar, 4), Type::Get_Float(Type::TY_Scalar));
    SM.emit_St_Tup_i(0, 0);
    SM(args);
    REQUIRE(not res.is_null(0));
    REQUIRE(res[0] == i);
}

TEST_CASE("StackMachine/Intrinsic/Cast_i_d", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 8) });
    Tuple *args[] = { &res };

    double d = 42.42;
    int i = (int) d;
    SM.add_and_emit_load(d);
    SM.emit_Cast(Type::Get_Integer(Type::TY_Scalar, 4), Type::Get_Double(Type::TY_Scalar));
    SM.emit_St_Tup_i(0, 0);
    SM(args);
    REQUIRE(not res.is_null(0));
    REQUIRE(res[0] == i);
}

TEST_CASE("StackMachine/Intrinsic/Cast_i_b", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Integer(Type::TY_Scalar, 8) });
    Tuple *args[] = { &res };

    SECTION("Cast true to integer")
    {
        SM.add_and_emit_load(true);
        SM.emit_Cast_i_b();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] != 0);
    }

    SECTION("Cast false to integer")
    {
        SM.add_and_emit_load(false);
        SM.emit_Cast_i_b();
        SM.emit_St_Tup_i(0, 0);
        SM(args);
        REQUIRE(not res.is_null(0));
        REQUIRE(res[0] == 0);
    }
}

TEST_CASE("StackMachine/Intrinsic/Cast_f_i", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Float(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    int i = 42;
    float f = (float) i;
    SM.add_and_emit_load(i);
    SM.emit_Cast_f_i();
    SM.emit_St_Tup_f(0, 0);
    SM(args);
    REQUIRE(not res.is_null(0));
    REQUIRE(res[0] == f);
}

TEST_CASE("StackMachine/Intrinsic/Cast_f_d", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Float(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    double d = 42.42;
    float f = (float) d;
    SM.add_and_emit_load(d);
    SM.emit_Cast_f_d();
    SM.emit_St_Tup_f(0, 0);
    SM(args);
    REQUIRE(not res.is_null(0));
    REQUIRE(res[0] == f);
}

TEST_CASE("StackMachine/Intrinsic/Cast_d_i", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Double(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    int i = 42;
    double d = (double) i;
    SM.add_and_emit_load(i);
    SM.emit_Cast_d_i();
    SM.emit_St_Tup_d(0, 0);
    SM(args);
    REQUIRE(not res.is_null(0));
    REQUIRE(res[0] == d);
}

TEST_CASE("StackMachine/Intrinsic/Cast_d_f", "[core][backend]")
{
    StackMachine SM;
    Tuple res({ Type::Get_Double(Type::TY_Scalar) });
    Tuple *args[] = { &res };

    float f = 42.42;
    double d = (double) f;
    SM.add_and_emit_load(f);
    SM.emit_Cast_d_f();
    SM.emit_St_Tup_d(0, 0);
    SM(args);
    REQUIRE(not res.is_null(0));
    REQUIRE(res[0] == d);
}
