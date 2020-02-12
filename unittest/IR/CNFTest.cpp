#include "catch.hpp"

#include "IR/CNF.hpp"
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "testutil.hpp"
#include "util/fn.hpp"
#include <algorithm>
#include <sstream>

using namespace db;
using namespace db::cnf;


/*======================================================================================================================
 * Test CNF operations.
 *====================================================================================================================*/

TEST_CASE("CNF/Clause operators", "[core][ir][cnf]")
{
    Position pos("test");
    Designator A(Token(pos, "A", TK_IDENTIFIER));
    Designator B(Token(pos, "B", TK_IDENTIFIER));
    Designator C(Token(pos, "C", TK_IDENTIFIER));
    Designator D(Token(pos, "D", TK_IDENTIFIER));

    Predicate PA = cnf::Predicate::Positive(&A);
    Predicate PB = cnf::Predicate::Positive(&B);
    Predicate PC = cnf::Predicate::Positive(&C);
    Predicate PD = cnf::Predicate::Positive(&D);

    cnf::Clause AB({PA, PB});
    cnf::Clause CD({PC, PD});

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
    Designator A(Token(pos, "A", TK_IDENTIFIER));
    Designator B(Token(pos, "B", TK_IDENTIFIER));
    Designator C(Token(pos, "C", TK_IDENTIFIER));
    Designator D(Token(pos, "D", TK_IDENTIFIER));

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
        Parser parser(lexer);
        Sema sema(diag);

        auto expr = parser.parse_Expr();
        sema(*expr);

        CNFGenerator gen;
        gen(*expr);
        auto cnf = gen.get();

        REQUIRE(cnf.size() == 1);
        auto &clause = cnf[0];
        REQUIRE(clause.size() == 1);
        auto &pred = clause[0];
        REQUIRE(not pred.negative());
        auto c = cast<const Constant>(pred.expr());
        REQUIRE(c);
        REQUIRE(c->tok.type == TK_True);

        delete expr;
    }

    SECTION("literal binary expression")
    {
        LEXER("13 < 42");
        Parser parser(lexer);
        Sema sema(diag);

        auto expr = parser.parse_Expr();
        sema(*expr);

        CNFGenerator gen;
        gen(*expr);
        auto cnf = gen.get();

        REQUIRE(cnf.size() == 1);
        auto &clause = cnf[0];
        REQUIRE(clause.size() == 1);
        auto &pred = clause[0];
        REQUIRE(not pred.negative());
        auto b = cast<const BinaryExpr>(pred.expr());
        REQUIRE(b);
        REQUIRE(b->op().type == TK_LESS);

        delete expr;
    }

    SECTION("logical or")
    {
        LEXER("TRUE OR FALSE");
        Parser parser(lexer);
        Sema sema(diag);

        auto expr = parser.parse_Expr();
        sema(*expr);

        CNFGenerator gen;
        gen(*expr);
        auto cnf = gen.get();

        REQUIRE(cnf.size() == 1);
        auto &clause = cnf[0];
        REQUIRE(clause.size() == 2);
        auto &True  = clause[0];
        auto &False = clause[1];

        REQUIRE(not True.negative());
        auto t = cast<const Constant>(True.expr());
        REQUIRE(t);
        REQUIRE(t->tok.type == TK_True);

        auto f = cast<const Constant>(False.expr());
        REQUIRE(not False.negative());
        REQUIRE(f);
        REQUIRE(f->tok.type == TK_False);

        delete expr;
    }

    SECTION("logical and")
    {
        LEXER("TRUE AND FALSE");
        Parser parser(lexer);
        Sema sema(diag);

        auto expr = parser.parse_Expr();
        sema(*expr);

        CNFGenerator gen;
        gen(*expr);
        auto cnf = gen.get();

        REQUIRE(cnf.size() == 2);
        auto &clause0 = cnf[0];
        auto &clause1 = cnf[1];
        REQUIRE(clause0.size() == 1);
        REQUIRE(clause1.size() == 1);
        auto &True  = clause0[0];
        auto &False = clause1[0];

        REQUIRE(not True.negative());
        auto t = cast<const Constant>(True.expr());
        REQUIRE(t);
        REQUIRE(t->tok.type == TK_True);

        auto f = cast<const Constant>(False.expr());
        REQUIRE(not False.negative());
        REQUIRE(f);
        REQUIRE(f->tok.type == TK_False);

        delete expr;
    }
}
