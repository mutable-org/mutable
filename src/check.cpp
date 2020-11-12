#include "globals.hpp"
#include "lex/Lexer.hpp"
#include "mutable/util/Diagnostic.hpp"
#include "mutable/util/fn.hpp"
#include "parse/ASTDumper.hpp"
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "util/ArgParser.hpp"
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>


using namespace m;


void usage(std::ostream &out, const char *name)
{
    out << "Performs semantic analysis of the input.\n"
        << "USAGE:\n\t" << name << " [<FILE>...]"
        << std::endl;
}

int main(int argc, const char **argv)
{
    /*----- Parse command line arguments. ----------------------------------------------------------------------------*/
    ArgParser AP;
#define ADD(TYPE, VAR, INIT, SHORT, LONG, DESCR, CALLBACK)\
    VAR = INIT;\
    {\
        std::function<void(TYPE)> callback = CALLBACK;\
        AP.add(SHORT, LONG, DESCR, callback);\
    }
    ADD(bool, Options::Get().show_help, false,              /* Type, Var, Init  */
        "-h", "--help",                                     /* Short, Long      */
        "prints this help message",                         /* Description      */
        [&](bool) { Options::Get().show_help = true; });    /* Callback         */
    ADD(bool, Options::Get().has_color, false,              /* Type, Var, Init  */
        nullptr, "--color",                                 /* Short, Long      */
        "use colors",                                       /* Description      */
        [&](bool) { Options::Get().has_color = true; });    /* Callback         */
    ADD(bool, Options::Get().ast, false,                    /* Type, Var, Init  */
        nullptr, "--ast",                                   /* Short, Long      */
        "print AST",                                        /* Description      */
        [&](bool) { Options::Get().ast = true; });          /* Callback         */
    ADD(bool, Options::Get().quiet, false,                  /* Type, Var, Init  */
        "-q", "--quiet",                                    /* Short, Long      */
        "work in quiet mode",                               /* Description      */
        [&](bool) { Options::Get().quiet = true; });        /* Callback         */
#undef ADD
    AP.parse_args(argc, argv);

    if (Options::Get().show_help) {
        usage(std::cout, argv[0]);
        std::cout << "WHERE\n";
        AP.print_args(stdout);
        std::exit(EXIT_SUCCESS);
    }

    auto args = AP.args();
    if (args.empty())
        args.push_back("-"); // start in interactive mode

    std::istream *in;

    /* Create the diagnostics object. */
    Diagnostic diag(Options::Get().has_color, std::cout, std::cerr);

    bool sema_error = false;
    Catalog &C = Catalog::Get();

    /* Process all the inputs. */
    for (auto filename : args) {
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

        Lexer lexer(diag, C.get_pool(), filename, *in);
        Parser parser(lexer);
        Sema sema(diag);

        while (parser.token()) {
            auto stmt = parser.parse();
            if (diag.num_errors()) {
                diag.clear();
                delete stmt;
                continue;
            }
            sema(*stmt);
            if (Options::Get().ast) stmt->dump(std::cout);
            sema_error = sema_error or diag.num_errors();
            diag.clear(); // clear sema errors
            delete stmt;
        }

        if (in != &std::cin)
            delete in;
    }

    std::exit(sema_error ? EXIT_FAILURE : EXIT_SUCCESS);
}
