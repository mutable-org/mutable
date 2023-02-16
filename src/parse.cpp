#include "lex/Lexer.hpp"
#include "parse/ASTPrinter.hpp"
#include "parse/Parser.hpp"
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutable/util/ArgParser.hpp>
#include <mutable/util/Diagnostic.hpp>
#include <mutable/util/fn.hpp>


using namespace m;
using namespace m::ast;


void usage(std::ostream &out, const char *name)
{
    out << "Performs grammatical analysis of the input.\n"
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
        AP.add<TYPE>(SHORT, LONG, DESCR, CALLBACK);\
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
        "dump the abstract syntax tree",    /* Description      */
        [&](bool) { ast = true; });         /* Callback         */
#undef ADD
    AP.parse_args(argc, argv);

    if (show_help) {
        usage(std::cout, argv[0]);
        std::cout << "WHERE\n" << AP;
        std::exit(EXIT_SUCCESS);
    }

    if (AP.args().size() != 1) {
        usage(std::cerr, argv[0]);
        std::cerr << "WHERE\n" << AP;
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

    Diagnostic diag(color, std::cout, std::cerr);
    StringPool pool;
    Lexer lexer(diag, pool, filename, *in);
    Parser parser(lexer);
    ASTPrinter printer(std::cout);

    while (parser.token()) {
        auto cmd = parser.parse();
        if (ast) {
            cmd->dump(std::cout);
        } else {
            printer(*cmd);
            std::cout << std::endl;
        }
    }

    if (in != &std::cin)
        delete in;

    std::exit(diag.num_errors() ? EXIT_FAILURE : EXIT_SUCCESS);
}
