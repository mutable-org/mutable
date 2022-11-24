#include "catch2/catch.hpp"

#include <mutable/IR/CNF.hpp>
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "testutil.hpp"
#include <mutable/util/fn.hpp>
#include <sstream>


using namespace m;
using namespace m::cnf;


/*======================================================================================================================
 * Test CNF operations.
 *====================================================================================================================*/

TEST_CASE("CNF/Clause operators", "[core][ir][cnf]")
{
    Position pos("test");
    ast::Designator A(ast::Token(pos, "A", TK_IDENTIFIER));
    ast::Designator B(ast::Token(pos, "B", TK_IDENTIFIER));
    ast::Designator C(ast::Token(pos, "C", TK_IDENTIFIER));
    ast::Designator D(ast::Token(pos, "D", TK_IDENTIFIER));

    Predicate PA = cnf::Predicate::Positive(&A);
    Predicate PB = cnf::Predicate::Positive(&B);
    Predicate PC = cnf::Predicate::Positive(&C);
    Predicate PD = cnf::Predicate::Positive(&D);

    cnf::Clause AB({PA, PB});
    cnf::Clause CD({PC, PD});

    SECTION("comparison")
    {
        cnf::Clause A({PA});
        cnf::Clause C({PC});

        REQUIRE(A <= AB);
        REQUIRE_FALSE(A >= AB);
        REQUIRE(AB >= A);
        REQUIRE_FALSE(AB <= A);

        REQUIRE_FALSE(C <= AB);
        REQUIRE_FALSE(AB <= C);

        REQUIRE(AB == AB);
        REQUIRE_FALSE(AB == CD);
    }

    /* (A v B) v (C v D) ⇔ (A v B v C v D) */
    SECTION("Logical or")
    {
        auto result = AB or CD;
        REQUIRE(result.size() == 4);
        REQUIRE(contains(result, PA));
        REQUIRE(contains(result, PB));
        REQUIRE(contains(result, PC));
        REQUIRE(contains(result, PD));
    }

    /* (A v B) ^ (C v D) is already in CNF. */
    SECTION("Logical and")
    {
        auto result = AB and CD;
        REQUIRE(result.size() == 2);
        REQUIRE(contains(result, AB));
        REQUIRE(contains(result, CD));
    }

    /* ¬(A v B) ⇔ (¬A) ^ (¬B) */
    SECTION("Logical not")
    {
        auto result = not AB;
        REQUIRE(result.size() == 2);
        REQUIRE(contains(result, cnf::Clause({not PA})));
        REQUIRE(contains(result, cnf::Clause({not PB})));
    }
}

TEST_CASE("CNF/CNF operators", "[core][ir][cnf]")
{
    Position pos("test");
    ast::Designator A(ast::Token(pos, "A", TK_IDENTIFIER));
    ast::Designator B(ast::Token(pos, "B", TK_IDENTIFIER));
    ast::Designator C(ast::Token(pos, "C", TK_IDENTIFIER));
    ast::Designator D(ast::Token(pos, "D", TK_IDENTIFIER));

    Predicate PA = cnf::Predicate::Positive(&A);
    Predicate PB = cnf::Predicate::Positive(&B);
    Predicate PC = cnf::Predicate::Positive(&C);
    Predicate PD = cnf::Predicate::Positive(&D);

    cnf::Clause AB({PA, PB});
    cnf::Clause CD({PC, PD});
    cnf::Clause AC({PA, PC});
    cnf::Clause BD({PB, PD});

    CNF AB_CD({AB, CD}); // (A v B) ^ (C v D)
    CNF AC_BD({AC, BD}); // (A v C) ^ (B v D)

    SECTION("comparison")
    {
        CNF cnf_AB({AB});
        CNF cnf_AC({AC});

        REQUIRE(cnf_AB <= AB_CD);
        REQUIRE_FALSE(AB_CD <= cnf_AB);
        REQUIRE(AB_CD >= cnf_AB);
        REQUIRE_FALSE(cnf_AB >= AB_CD);

        REQUIRE_FALSE(cnf_AC <= AB_CD);
        REQUIRE_FALSE(AB_CD <= cnf_AC);

        REQUIRE(AB_CD == AB_CD);
        REQUIRE_FALSE(AB_CD == AC_BD);
    }

    /* [(A v B) ^ (C v D)] v [(A v C) ^ (B v D)]
     * ⇔ (A v B v A v C) ^ (A v B v B v D) ^ (C v D v A v C) ^ (C v D v B v D) */
    SECTION("Logical or")
    {
        auto result = AB_CD or AC_BD;
        REQUIRE(result.size() == 4);

        cnf::Clause ABAC({PA, PB, PA, PC});
        cnf::Clause ABBD({PA, PB, PB, PD});
        cnf::Clause CDAC({PC, PD, PA, PC});
        cnf::Clause CDBD({PC, PD, PB, PD});

        REQUIRE(contains(result, ABAC));
        REQUIRE(contains(result, ABBD));
        REQUIRE(contains(result, CDAC));
        REQUIRE(contains(result, CDBD));
    }

    /* [(A v B) ^ (C v D)] ^ [(A v C) ^ (B v D)] ⇔ (A v B) ^ (C v D) ^ (A v C) ^ (B v D) */
    SECTION("Logical and")
    {
        auto result = AB_CD and AC_BD;
        REQUIRE(result.size() == 4);

        REQUIRE(contains(result, AB));
        REQUIRE(contains(result, CD));
        REQUIRE(contains(result, AC));
        REQUIRE(contains(result, BD));
    }

    /* ¬[(A v B) ^ (C v D)]
     * ⇔ ¬(A v B) v ¬(C v D)
     * ⇔ (¬A ^ ¬B) v (¬C ^ ¬D)
     * ⇔ (¬A v ¬C) ^ (¬A v ¬D) ^ (¬B v ¬C) ^ (¬B v ¬D) */
    SECTION("Logical not")
    {
        auto result = not AB_CD;
        REQUIRE(result.size() == 4);
        REQUIRE(contains(result, cnf::Clause({not PA, not PC})));
        REQUIRE(contains(result, cnf::Clause({not PA, not PD})));
        REQUIRE(contains(result, cnf::Clause({not PB, not PC})));
        REQUIRE(contains(result, cnf::Clause({not PB, not PD})));
    }
}

TEST_CASE("CNF/CNFGenerator", "[core][ir][cnf]")
{
    SECTION("literal single value")
    {
        LEXER("TRUE");
        ast::Parser parser(lexer);
        ast::Sema sema(diag);

        auto expr = parser.parse_Expr();
        sema(*expr);
        auto cnf = to_CNF(*expr);

        REQUIRE(cnf.size() == 1);
        auto &clause = cnf[0];
        REQUIRE(clause.size() == 1);
        auto &pred = clause[0];
        REQUIRE(not pred.negative());
        auto c = cast<const ast::Constant>(&pred.expr());
        REQUIRE(c);
        REQUIRE(c->tok.type == TK_True);

    }

    SECTION("literal binary expression")
    {
        LEXER("13 < 42");
        ast::Parser parser(lexer);
        ast::Sema sema(diag);

        auto expr = parser.parse_Expr();
        sema(*expr);
        auto cnf = to_CNF(*expr);

        REQUIRE(cnf.size() == 1);
        auto &clause = cnf[0];
        REQUIRE(clause.size() == 1);
        auto &pred = clause[0];
        REQUIRE(not pred.negative());
        auto b = cast<const ast::BinaryExpr>(&pred.expr());
        REQUIRE(b);
        REQUIRE(b->op().type == TK_LESS);

    }

    SECTION("logical or")
    {
        LEXER("TRUE OR FALSE");
        ast::Parser parser(lexer);
        ast::Sema sema(diag);

        auto expr = parser.parse_Expr();
        sema(*expr);
        auto cnf = to_CNF(*expr);

        REQUIRE(cnf.size() == 1);
        auto &clause = cnf[0];
        REQUIRE(clause.size() == 2);
        auto &True  = clause[0];
        auto &False = clause[1];

        REQUIRE(not True.negative());
        auto t = cast<const ast::Constant>(&True.expr());
        REQUIRE(t);
        REQUIRE(t->tok.type == TK_True);

        auto f = cast<const ast::Constant>(&False.expr());
        REQUIRE(not False.negative());
        REQUIRE(f);
        REQUIRE(f->tok.type == TK_False);

    }

    SECTION("logical and")
    {
        LEXER("TRUE AND FALSE");
        ast::Parser parser(lexer);
        ast::Sema sema(diag);

        auto expr = parser.parse_Expr();
        sema(*expr);
        auto cnf = to_CNF(*expr);

        REQUIRE(cnf.size() == 2);
        auto &clause0 = cnf[0];
        auto &clause1 = cnf[1];
        REQUIRE(clause0.size() == 1);
        REQUIRE(clause1.size() == 1);
        auto &True  = clause0[0];
        auto &False = clause1[0];

        REQUIRE(not True.negative());
        auto t = cast<const ast::Constant>(&True.expr());
        REQUIRE(t);
        REQUIRE(t->tok.type == TK_True);

        auto f = cast<const ast::Constant>(&False.expr());
        REQUIRE(not False.negative());
        REQUIRE(f);
        REQUIRE(f->tok.type == TK_False);

    }
}
