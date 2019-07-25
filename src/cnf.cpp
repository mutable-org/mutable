#include "IR/CNF.hpp"
#include "lex/Lexer.hpp"
#include "lex/Token.hpp"
#include "lex/TokenType.hpp"
#include "parse/ASTDumper.hpp"
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "util/ArgParser.hpp"
#include "util/Diagnostic.hpp"
#include "util/fn.hpp"
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <initializer_list>
#include <iostream>


using namespace db;


void usage(std::ostream &out, const char *name)
{
    out << "Performs semantic analysis of the input.\n"
        << "USAGE:\n\t" << name << " <FILE>"
        << "\n\t" << name << " -"
        << std::endl;
}

int main(int argc, const char **argv)
{
    ArgParser AP;
#define ADD(TYPE, VAR, INIT, SHORT, LONG, DESCR, CALLBACK)\
    TYPE VAR = INIT;\
    {\
        std::function<void(TYPE)> callback = CALLBACK;\
        AP.add(SHORT, LONG, VAR, DESCR, callback);\
    }
    ADD(bool, show_help, false,             /* Type, Var, Init  */
        "-h", "--help",                     /* Short, Long      */
        "prints this help message",         /* Description      */
        [&](bool) { show_help = true; });   /* Callback         */
    ADD(bool, color, false,                 /* Type, Var, Init  */
        nullptr, "--color",                 /* Short, Long      */
        "use colors",                       /* Description      */
        [&](bool) { color = true; });       /* Callback         */
    ADD(bool, ast, false,                   /* Type, Var, Init  */
        nullptr, "--ast",                   /* Short, Long      */
        "print AST",                        /* Description      */
        [&](bool) { ast = true; });         /* Callback         */
#undef ADD
    AP.parse_args(argc, argv);

    if (show_help) {
        usage(std::cout, argv[0]);
        std::cout << "WHERE\n";
        AP.print_args(stdout);
        std::exit(EXIT_SUCCESS);
    }

    if (AP.args().size() != 1) {
        usage(std::cerr, argv[0]);
        std::cerr << "WHERE\n";
        AP.print_args(stderr);
        std::exit(EXIT_FAILURE);
    }

    const char *filename = AP.args()[0];
    std::istream *in;
    if (streq(filename, "-")) {
        /* read from stdin */
        in = &std::cin;
    } else {
        /* read from file */
        in = new std::ifstream(filename, std::ios_base::in);
    }

    if (in->fail()) {
        if (in == &std::cin)
            std::cerr << "Failed to open stdin: ";
        else
            std::cerr << "Failed to open the file '" << filename << "': ";
        std::cerr << strerror(errno) << std::endl;
    }

    Catalog &C = Catalog::Get();
    Diagnostic diag(color, std::cout, std::cerr);
    Lexer lexer(diag, C.get_pool(), filename, *in);
    Parser parser(lexer);
    Sema sema(diag);

    Position pos("test");
    Expr *c0 = new Constant(Token(pos, "0", TK_DEC_INT));
    Expr *c1 = new Constant(Token(pos, "1", TK_DEC_INT));
    Expr *c2 = new Constant(Token(pos, "2", TK_DEC_INT));
    Expr *d0 = new Designator(Token(pos, "a", TK_IDENTIFIER));
    Expr *d1 = new Designator(Token(pos, "b", TK_IDENTIFIER));
    Expr *d2 = new Designator(Token(pos, "c", TK_IDENTIFIER));
    Expr *b0 = new BinaryExpr(Token(pos, "<", TK_LESS), d0, c0);
    Expr *b1 = new BinaryExpr(Token(pos, "<", TK_LESS), d1, c1);
    Expr *b2 = new BinaryExpr(Token(pos, "<", TK_LESS), d2, c2);

    cnf::Predicate P0 = cnf::Predicate::Positive(b0);
    cnf::Predicate P1 = cnf::Predicate::Positive(b1);
    cnf::Predicate P2 = cnf::Predicate::Positive(b2);
    cnf::Predicate N0 = cnf::Predicate::Negative(b0);
    cnf::Predicate N1 = cnf::Predicate::Negative(b1);
    cnf::Predicate N2 = cnf::Predicate::Negative(b2);

    cnf::Clause C0({P0, P2});
    C0.dump();

    cnf::Clause C1({P1, N0});
    C1.dump();

    cnf::Clause C0_or_C1 = C0 or C1;
    C0_or_C1.dump();

    cnf::CNF cnf0({C0, C1});
    cnf0.dump();

    cnf::CNF cnf1 = C0 and C1;
    cnf1.dump();

    cnf::CNF cnf2({
        cnf::Clause({P0}),
        cnf::Clause({N1})
    });
    cnf2.dump();

    cnf::CNF cnf3({
        cnf::Clause({N0}),
        cnf::Clause({P2})
    });
    cnf3.dump();

    cnf::CNF cnf2_or_cnf3 = cnf2 or cnf3;
    cnf2_or_cnf3.dump();

#if 0
    while (parser.token()) {
        auto stmt = parser.parse();
        if (diag.num_errors()) {
            diag.clear();
            continue;
        }
        sema(*stmt);
        if (diag.num_errors()) {
            diag.clear();
            continue;
        }
        if (ast) stmt->dump(std::cout);
        // TODO: compute CNF
    }
#endif

    if (in != &std::cin)
        delete in;

    std::exit(EXIT_SUCCESS);
}
